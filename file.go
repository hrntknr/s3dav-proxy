package main

import (
	"context"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

type File struct {
	ctx  context.Context
	mc   *minio.Client
	path []string
}

func (f *File) Readdir(count int) ([]fs.FileInfo, error) {
	if len(f.path) == 0 {
		list, err := f.mc.ListBuckets(context.Background())
		if err != nil {
			return nil, handleMinioError(err)
		}
		var files []fs.FileInfo
		for _, bucket := range list {
			files = append(files, &FileInfo{
				name:    bucket.Name,
				size:    0,
				modTime: time.Now(),
				isDir:   true,
			})
		}
		return files, nil
	} else {
		prefix := ""
		if len(f.path) > 1 {
			prefix = strings.Join(f.path[1:], "/") + "/"
		}
		list := f.mc.ListObjects(f.ctx, f.path[0], minio.ListObjectsOptions{
			Prefix:  prefix,
			MaxKeys: count,
		})
		filesMap := map[string]fs.FileInfo{}
		for obj := range list {
			if obj.Err != nil {
				return nil, handleMinioError(obj.Err)
			}
			objNames := splitPath(obj.Key)
			isDir := strings.HasSuffix(obj.Key, "/")
			size := int64(0)
			if !isDir {
				size = obj.Size
			}
			_, ok := filesMap[objNames[len(objNames)-1]]
			if !ok || (isDir && preferDirectory) || (!isDir && !preferDirectory) {
				filesMap[objNames[len(objNames)-1]] = &FileInfo{
					name:    objNames[len(objNames)-1],
					size:    size,
					modTime: obj.LastModified,
					isDir:   isDir,
				}
			}
		}
		files := []fs.FileInfo{}
		for _, file := range filesMap {
			files = append(files, file)
		}
		return files, nil
	}
}
func (f *File) Stat() (fs.FileInfo, error) {
	if len(f.path) == 0 {
		if _, err := f.mc.ListBuckets(context.Background()); err != nil {
			return nil, handleMinioError(err)
		}
		return &FileInfo{
			name:    strings.Join(f.path, "/"),
			size:    0,
			modTime: time.Now(),
			isDir:   true,
		}, nil
	} else {
		exsits, err := f.mc.BucketExists(f.ctx, f.path[0])
		if err != nil {
			return nil, handleMinioError(err)
		}
		if !exsits {
			return nil, os.ErrNotExist
		}
		if len(f.path) == 1 {
			return &FileInfo{
				name:    strings.Join(f.path, "/"),
				size:    0,
				modTime: time.Now(),
				isDir:   true,
			}, nil
		}
		list := f.mc.ListObjects(f.ctx, f.path[0], minio.ListObjectsOptions{
			Prefix: strings.Join(f.path[1:], "/"),
		})
		var findObj *minio.ObjectInfo
		isDir := false
		for obj := range list {
			if obj.Err != nil {
				return nil, handleMinioError(obj.Err)
			}
			if obj.Key == strings.Join(f.path[1:], "/") {
				findObj = &obj
				isDir = false
				if !preferDirectory {
					break
				}
			}
			if strings.HasPrefix(obj.Key, strings.Join(f.path[1:], "/")+"/") {
				findObj = &obj
				isDir = true
				if preferDirectory {
					break
				}
			}
		}
		if findObj != nil {
			if isDir {
				return &FileInfo{
					name:    "",
					size:    0,
					modTime: time.Now(),
					isDir:   true,
				}, nil
			} else {
				return &FileInfo{
					name:    "",
					size:    findObj.Size,
					modTime: findObj.LastModified,
					isDir:   false,
				}, nil
			}
		}
		return nil, os.ErrNotExist
	}
}
func (f *File) Read(p []byte) (n int, err error) {
	return 0, nil
}
func (f *File) Write(p []byte) (n int, err error) {
	return 0, os.ErrNotExist
}
func (f *File) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}
func (f *File) Close() error {
	return nil
}
