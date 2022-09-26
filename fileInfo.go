package main

import (
	"os"
	"time"
)

type FileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (f *FileInfo) Name() string {
	return f.name
}
func (f *FileInfo) Size() int64 {
	return f.size
}
func (f *FileInfo) Mode() os.FileMode {
	if f.isDir {
		return 700
	} else {
		return 600
	}
}
func (f *FileInfo) ModTime() time.Time {
	return f.modTime
}
func (f *FileInfo) IsDir() bool {
	return f.isDir
}
func (f *FileInfo) Sys() interface{} {
	return nil
}
