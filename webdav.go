package main

import (
	"context"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"golang.org/x/net/webdav"
)

type Handler struct {
}

func (d *Handler) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	return os.ErrNotExist
}

func (d *Handler) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	mc := ctx.Value(minioClientCtxKey).(*minio.Client)
	names := splitPath(name)
	return &File{ctx: ctx, mc: mc, path: names}, nil
}

func (d *Handler) RemoveAll(ctx context.Context, name string) error {
	return os.ErrNotExist
}

func (d *Handler) Rename(ctx context.Context, oldName, newName string) error {
	return os.ErrNotExist
}

func (d *Handler) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	mc := ctx.Value(minioClientCtxKey).(*minio.Client)
	names := splitPath(name)

	f := File{ctx: ctx, mc: mc, path: names}
	return f.Stat()
}

func (d *Handler) Confirm(now time.Time, name0, name1 string, conditions ...webdav.Condition) (release func(), err error) {
	return nil, nil
}

func (d *Handler) Create(now time.Time, details webdav.LockDetails) (token string, err error) {
	return "", nil
}

func (d *Handler) Refresh(now time.Time, token string, duration time.Duration) (webdav.LockDetails, error) {
	return webdav.LockDetails{}, nil
}

func (d *Handler) Unlock(now time.Time, token string) error {
	return nil
}
