package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"golang.org/x/net/webdav"
	"golang.org/x/sync/errgroup"
)

type Handler struct {
	dummyDirs []string
}

func newHandler() *Handler {
	return &Handler{
		dummyDirs: []string{},
	}
}

func (d *Handler) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	if readOnly {
		return os.ErrPermission
	}
	mc := ctx.Value(minioClientCtxKey).(*minio.Client)
	names := splitPath(name)
	if len(names) == 0 {
		return os.ErrInvalid
	}
	if len(names) == 1 {
		if allowBucketsOps {
			return mc.MakeBucket(ctx, names[0], minio.MakeBucketOptions{})
		} else {
			return os.ErrPermission
		}
	}
	// d.dummyDirs = append(d.dummyDirs, strings.Join(names, "/"))
	for i := len(names); i > 1; i-- {
		list := mc.ListObjects(ctx, names[0], minio.ListObjectsOptions{
			Prefix: strings.Join(names[1:i], "/") + "/",
		})
		isExist := false
		for obj := range list {
			if obj.Err != nil {
				return handleMinioError(obj.Err)
			}
			isExist = true
		}
		if !isExist {
			d.dummyDirs = append(d.dummyDirs, strings.Join(names[:i], "/"))
		}
	}
	return nil
}

func (d *Handler) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	fmt.Println("OpenFile", name, flag, perm)
	mc := ctx.Value(minioClientCtxKey).(*minio.Client)
	names := splitPath(name)
	return newFile(ctx, mc, names, d.dummyDirs, flag), nil
}

func (d *Handler) RemoveAll(ctx context.Context, name string) error {
	if readOnly {
		return os.ErrPermission
	}
	mc := ctx.Value(minioClientCtxKey).(*minio.Client)
	names := splitPath(name)
	if len(names) == 0 {
		return os.ErrInvalid
	}
	if len(names) == 1 {
		if allowBucketsOps {
			return mc.RemoveBucket(ctx, names[0])
		} else {
			return os.ErrPermission
		}
	}

	list := mc.ListObjects(ctx, names[0], minio.ListObjectsOptions{
		Prefix: strings.Join(names[1:], "/"),
	})
	isExist := false
	isDir := false
	for obj := range list {
		if obj.Err != nil {
			return handleMinioError(obj.Err)
		}
		if obj.Key == strings.Join(names[1:], "/") {
			isExist = true
			isDir = false
			if !preferDirectory {
				break
			}
		}
		if obj.Key != strings.Join(names[1:], "/") {
			isExist = true
			isDir = true
			if preferDirectory {
				break
			}
		}
	}
	if !isExist {
		if isDummyDir(d.dummyDirs, names[0], strings.Join(names[1:], "/")) {
			d.dummyDirs = deleteDummyDir(d.dummyDirs, names[0], strings.Join(names[1:], "/"))
			return nil
		}
		return os.ErrNotExist
	}

	// Trace back to the parent folder where only the deletion target exists and save it.
	for i := len(names) - 1; i > 1; i-- {
		list := mc.ListObjects(ctx, names[0], minio.ListObjectsOptions{
			Prefix: strings.Join(names[1:i], "/") + "/",
		})
		isExist := false
		for obj := range list {
			if obj.Err != nil {
				return handleMinioError(obj.Err)
			}
			if !strings.HasPrefix(strings.Join(names[1:], "/")+"/", obj.Key) {
				isExist = true
				break
			}
		}
		if isExist {
			continue
		}
		d.dummyDirs = append(d.dummyDirs, strings.Join(names[:i], "/"))
	}

	if isDir {
		eg := &errgroup.Group{}
		list := mc.ListObjects(ctx, names[0], minio.ListObjectsOptions{
			Prefix:    strings.Join(names[1:], "/"),
			Recursive: true,
		})
		for obj := range list {
			obj := obj
			if obj.Err != nil {
				return handleMinioError(obj.Err)
			}
			if obj.Key != strings.Join(names[1:], "/") {
				eg.Go(func() error {
					return handleMinioError(mc.RemoveObject(ctx, names[0], obj.Key, minio.RemoveObjectOptions{}))
				})
			}
		}
		if err := eg.Wait(); err != nil {
			return err
		}
		d.dummyDirs = deleteDummyDir(d.dummyDirs, names[0], strings.Join(names[1:], "/"))
	} else {
		if err := mc.RemoveObject(ctx, names[0], strings.Join(names[1:], "/"), minio.RemoveObjectOptions{}); err != nil {
			return handleMinioError(err)
		}
	}

	return nil
}

func (d *Handler) Rename(ctx context.Context, oldName, newName string) error {
	if readOnly {
		return os.ErrPermission
	}
	mc := ctx.Value(minioClientCtxKey).(*minio.Client)
	oldNames := splitPath(oldName)
	newNames := splitPath(newName)

	dummyDirs, renameDummy := renameDummyDir(d.dummyDirs, oldNames[0], strings.Join(oldNames[1:], "/"), newNames[0], strings.Join(newNames[1:], "/"))
	d.dummyDirs = dummyDirs
	if renameDummy {
		return nil
	}

	dst := minio.CopyDestOptions{
		Bucket: newNames[0],
		Object: strings.Join(newNames[1:], "/"),
	}
	src := minio.CopySrcOptions{
		Bucket: oldNames[0],
		Object: strings.Join(oldNames[1:], "/"),
	}
	if _, err := mc.CopyObject(ctx, dst, src); err != nil {
		return handleMinioError(err)
	}
	if err := mc.RemoveObject(ctx, oldNames[0], strings.Join(oldNames[1:], "/"), minio.RemoveObjectOptions{}); err != nil {
		return handleMinioError(err)
	}

	return nil
}

func (d *Handler) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	mc := ctx.Value(minioClientCtxKey).(*minio.Client)
	names := splitPath(name)

	return newFile(ctx, mc, names, d.dummyDirs, os.O_RDONLY).Stat()
}
