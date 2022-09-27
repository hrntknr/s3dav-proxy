package main

import (
	"strings"
)

func splitPath(name string) []string {
	_names := strings.Split(name, "/")
	names := []string{}
	for _, _name := range _names {
		if _name != "" {
			names = append(names, _name)
		}
	}
	return names
}

func handleMinioError(err error) error {
	return err
}

func listChildDummyDirs(dummyDirs []string, bucket string, path string) []string {
	key := bucket
	if path != "" {
		key += "/" + path
	}
	dirs := map[string]struct{}{}
	for _, dummyDir := range dummyDirs {
		if strings.HasPrefix(dummyDir, key+"/") {
			tmp := strings.Split(strings.TrimPrefix(dummyDir, key+"/"), "/")
			if len(tmp) == 1 {
				dirs[tmp[0]] = struct{}{}
			}
		}
	}
	_dirs := []string{}
	for dir := range dirs {
		_dirs = append(_dirs, dir)
	}
	return _dirs
}

func isDummyDir(dummyDirs []string, bucket string, path string) bool {
	key := bucket
	if path != "" {
		key += "/" + path
	}
	for _, dummyDir := range dummyDirs {
		if dummyDir == key {
			return true
		}
	}
	return false
}

func deleteDummyDir(dummyDirs []string, bucket string, path string) []string {
	key := bucket
	if path != "" {
		key += "/" + path
	}
	_dummyDirs := []string{}
	for _, dummyDir := range dummyDirs {
		if !strings.HasPrefix(dummyDir, key) {
			_dummyDirs = append(_dummyDirs, dummyDir)
		}
	}
	return _dummyDirs
}

func renameDummyDir(dummyDirs []string, bucket string, path string, newBucket string, newPath string) ([]string, bool) {
	key := bucket
	if path != "" {
		key += "/" + path
	}
	newKey := newBucket
	if newPath != "" {
		newKey += "/" + newPath
	}
	_dummyDirs := []string{}
	renameDummy := false
	for _, dummyDir := range dummyDirs {
		if strings.HasPrefix(dummyDir, key) {
			if dummyDir == key {
				renameDummy = true
			}
			_dummyDirs = append(_dummyDirs, strings.Replace(dummyDir, key, newKey, 1))
		} else {
			_dummyDirs = append(_dummyDirs, dummyDir)
		}
	}
	return _dummyDirs, renameDummy
}
