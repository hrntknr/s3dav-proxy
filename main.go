package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/cobra"
	"golang.org/x/net/webdav"
)

type ctxKey int

var minioClientCtxKey ctxKey

var (
	port            uint
	endpoint        string
	secure          bool
	preferDirectory bool // Minio does not allow duplicate names for directory and file names, but s3 does.
)

var RootCmd = &cobra.Command{
	Use: "miniodav",
	Run: func(cmd *cobra.Command, args []string) {
		srv := &webdav.Handler{
			FileSystem: &Handler{},
			LockSystem: &Handler{},
			Logger: func(r *http.Request, err error) {
				log.Printf("%s %s %v", r.Method, r.URL, err)
			},
		}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			username, password, _ := r.BasicAuth()
			mc, err := minio.New(endpoint, &minio.Options{
				Creds:  credentials.NewStaticV4(username, password, ""),
				Secure: true,
			})
			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if r.Method == "PROPFIND" {
				if _, err := mc.ListBuckets(context.Background()); err != nil {
					if minioErr, ok := err.(minio.ErrorResponse); ok {
						log.Println(minioErr.Code)
						if minioErr.Code == "SignatureDoesNotMatch" || minioErr.Code == "InvalidAccessKeyId" || minioErr.Code == "AccessDenied" {
							w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
							w.WriteHeader(http.StatusUnauthorized)
							return
						}
					}
					log.Println(err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}

			srv.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), minioClientCtxKey, mc)))
		})
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Fatalf("Error with WebDAV server: %v", err)
		}
	},
}

func init() {
	RootCmd.Flags().StringVarP(&endpoint, "endpoint", "e", "play.min.io", "Minio endpoint")
	RootCmd.Flags().BoolVarP(&secure, "secure", "s", true, "Use secure connection")
	RootCmd.Flags().UintVarP(&port, "port", "p", 8080, "Port to listen on")
	RootCmd.Flags().BoolVarP(&preferDirectory, "prefer-directory", "d", true, "Prefer directory over file")
}

func main() {
	RootCmd.Execute()
}
