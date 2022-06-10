package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"syscall"
	"time"
)

var totalOpenTime time.Duration = time.Duration(0)
var totalFiles int = 0

// Pass a file name as first argument
func main() {
	start := time.Now()
	name := os.Args[1]
	// filepath.WalkDir(name, visit)

	split := strings.Split(name, SlashSeparator)
	basePath := strings.Join(split[:len(split)-1], SlashSeparator)
	bucket := split[len(split)-1]
	storage := &xlStorage{
		diskPath: basePath,
	}
	opts := WalkDirOptions{
		Bucket:         bucket,
		BaseDir:        "",
		Recursive:      true,
		ReportNotFound: false,
		FilterPrefix:   "",
		ForwardTo:      "",
	}

	// Use MinIO code!!!
	totalFiles = storage.WalkDir(context.TODO(), opts)
	totalTime := time.Since(start)
	fmt.Printf("%d;%f;%f\n", totalFiles, totalOpenTime.Seconds(), totalTime.Seconds())
}

func visit(path string, d fs.DirEntry, err error) error {
	isDir, _ := isDirectory(path)
	if !isDir {
		duration := openFile(path)
		totalOpenTime += duration
		totalFiles++
	}
	return nil
}

// isDirectory determines if a file represented
// by `path` is a directory or not
func isDirectory(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false, err
	}

	return fileInfo.IsDir(), err
}

func openFile(name string) time.Duration {
	duration, r, _ := openFileNologSimplified(name, os.O_RDONLY, 0)
	// Close is done here, to ensure that we measure ONLY the
	// syscall.Open() duration in openFileNologSimplified().
	// We should ensure that filehandles are closed to
	// 1. prevent memory leaks and
	// 2. because GlusterFS has a fixed upper limit of how many
	//    fds can be open.
	defer syscall.Close(r)
	return duration
}

// Simplified version of the Unix implementation of os.openFileNolog.
// It mainly lacks support for sticky bit and platforms where "close on exit" is not supported.
func openFileNologSimplified(name string, flag int, perm uint32) (time.Duration, int, error) {
	start := time.Now()
	var r int
	c := 0
	for {
		var e error
		r, e = syscall.Open(name, flag|syscall.O_CLOEXEC, perm)
		if e == nil {
			break
		}
		// We have to check EINTR here, per issues 11180 and 39237.
		if e == syscall.EINTR {
			c++
			continue
		}
		return time.Since(start), r, e

	}

	return time.Since(start), r, nil
}
