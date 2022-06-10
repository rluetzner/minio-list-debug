package main

import (
	"context"
	"errors"
	"os"
	"path"
	"runtime"
	"strings"
)

const (
	globalWindowsOSName      = "windows"
	SlashSeparator           = "/"
	globalDirSuffix          = "__XLDIR__"
	globalDirSuffixWithSlash = globalDirSuffix + SlashSeparator
	// XL metadata file carries per object metadata.
	xlStorageFormatFileV1 = "xl.json"
)

var (
	// Disallow updating access times
	readMode = os.O_RDONLY | 0x40000 // O_NOATIME

	// Write with data sync only used only for `xl.meta` writes
	writeMode = 0x1000 // O_DSYNC
)

// HasSuffix - Suffix matcher string matches suffix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func HasSuffix(s string, suffix string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return strings.HasSuffix(strings.ToLower(s), strings.ToLower(suffix))
	}
	return strings.HasSuffix(s, suffix)
}

// pathJoin - like path.Join() but retains trailing SlashSeparator of the last element
func pathJoin(elem ...string) string {
	trailingSlash := ""
	if len(elem) > 0 {
		if HasSuffix(elem[len(elem)-1], SlashSeparator) {
			trailingSlash = SlashSeparator
		}
	}
	return path.Join(elem...) + trailingSlash
}

// contextCanceled returns whether a context is canceled.
func contextCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// checkPathLength - returns error if given path name length more than 255
func checkPathLength(pathName string) error {
	// Apple OS X path length is limited to 1016
	if runtime.GOOS == "darwin" && len(pathName) > 1016 {
		return errors.New("File name too long.")
	}

	// Disallow more than 1024 characters on windows, there
	// are no known name_max limits on Windows.
	if runtime.GOOS == "windows" && len(pathName) > 1024 {
		return errors.New("File name too long.")
	}

	// On Unix we reject paths if they are just '.', '..' or '/'
	if pathName == "." || pathName == ".." || pathName == SlashSeparator {
		return errors.New("Access denied.")
	}

	if len(pathName) < 256 {
		return nil
	}

	// Check each path segment length is > 255 on all Unix
	// platforms, look for this value as NAME_MAX in
	// /usr/include/linux/limits.h
	var count int64
	for _, p := range pathName {
		switch p {
		case '/':
			count = 0 // Reset
		case '\\':
			if runtime.GOOS == globalWindowsOSName {
				count = 0
			}
		default:
			count++
			if count > 255 {
				return errors.New("File name too long.")
			}
		}
	} // Success.
	return nil
}
