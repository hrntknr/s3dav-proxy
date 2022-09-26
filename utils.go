package main

import (
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
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
	if err == nil {
		return nil
	}
	if minioErr, ok := err.(minio.ErrorResponse); ok {
		switch minioErr.Code {
		case "NoSuchBucket":
			return os.ErrNotExist
		case "NoSuchKey":
			return os.ErrNotExist
		}
	}
	return os.ErrInvalid
}
