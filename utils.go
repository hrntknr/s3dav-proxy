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
			dirs[strings.Split(dummyDir[len(key)+1:], "/")[0]] = struct{}{}
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
