package main

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

type File struct {
	ctx         context.Context
	mc          *minio.Client
	path        []string
	dummyDirs   []string
	offset      int64
	lock        *sync.Mutex
	cacheObject *minio.Object
	writeBuffer *bytes.Buffer
	flag        int
}

func newFile(ctx context.Context, mc *minio.Client, path []string, dummyDirs []string, flag int) *File {
	return &File{
		ctx:         context.Background(),
		mc:          mc,
		path:        path,
		dummyDirs:   dummyDirs,
		offset:      0,
		lock:        &sync.Mutex{},
		cacheObject: nil,
		writeBuffer: bytes.NewBuffer([]byte{}),
		flag:        flag,
	}
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
		for _, dir := range listChildDummyDirs(f.dummyDirs, f.path[0], strings.Join(f.path[1:], "/")) {
			_, ok := filesMap[dir]
			if !ok {
				filesMap[dir] = &FileInfo{
					name:    dir,
					size:    0,
					modTime: time.Now(),
					isDir:   true,
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
				if f.writeBuffer.Len() > 0 {
					return &FileInfo{
						name:    "",
						size:    int64(f.writeBuffer.Len()),
						modTime: time.Now(),
						isDir:   false,
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
		}
		if isDummyDir(f.dummyDirs, f.path[0], strings.Join(f.path[1:], "/")) {
			return &FileInfo{
				name:    "",
				size:    0,
				modTime: time.Now(),
				isDir:   true,
			}, nil
		}
		if f.flag&os.O_CREATE != 0 {
			if readOnly {
				return nil, os.ErrPermission
			}
			if _, err := f.mc.PutObject(f.ctx, f.path[0], strings.Join(f.path[1:], "/"), bytes.NewBuffer([]byte{}), 0, minio.PutObjectOptions{}); err != nil {
				return nil, handleMinioError(err)
			}
			return f.Stat()
		}
		return nil, os.ErrNotExist
	}
}

func (f *File) Read(p []byte) (n int, err error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if err := f.cache(); err != nil {
		return 0, err
	}

	return f.cacheObject.Read(p)
}

func (f *File) Write(p []byte) (n int, err error) {
	if readOnly {
		return 0, os.ErrPermission
	}
	f.lock.Lock()
	defer f.lock.Unlock()

	if len(f.path) <= 1 {
		return 0, os.ErrInvalid
	}
	if f.offset != 0 {
		f.writeBuffer.Reset()
		return 0, os.ErrInvalid
	}
	return f.writeBuffer.Write(p)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if err := f.cache(); err != nil {
		return 0, err
	}

	cur, err := f.cacheObject.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	f.offset = cur
	return cur, nil
}

func (f *File) Close() error {
	if f.writeBuffer.Len() > 0 {
		if _, err := f.mc.PutObject(f.ctx, f.path[0], strings.Join(f.path[1:], "/"), f.writeBuffer, int64(f.writeBuffer.Len()), minio.PutObjectOptions{}); err != nil {
			return handleMinioError(err)
		}
	}
	if f.cacheObject != nil {
		if err := f.cacheObject.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (f *File) cache() error {
	if f.cacheObject != nil {
		return nil
	}
	obj, err := f.mc.GetObject(f.ctx, f.path[0], strings.Join(f.path[1:], "/"), minio.GetObjectOptions{})
	if err != nil {
		return handleMinioError(err)
	}
	f.cacheObject = obj
	return nil
}
