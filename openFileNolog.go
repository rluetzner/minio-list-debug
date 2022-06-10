package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

var totalFiles int = 0

// Pass a file name as first argument
func main() {
	start := time.Now()
	name := os.Args[1]
	// filepath.WalkDir(name, visit)

	name = strings.TrimSuffix(name, SlashSeparator)
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
	fmt.Printf("%d;%f\n", totalFiles, totalTime.Seconds())
}
