package main

import (
	"errors"
	"os"
	"runtime"
	"syscall"
)

type StorageErr string

func (h StorageErr) Error() string {
	return string(h)
}

// errFileNotFound - cannot find the file.
var errFileNotFound = StorageErr("file not found")

// errDiskFull - cannot create volume or files when disk is full.
var errDiskFull = StorageErr("disk path full")

// errVolumeNotFound - cannot find the volume.
var errVolumeNotFound = StorageErr("volume not found")

// errFaultyDisk - disk is faulty.
var errFaultyDisk = StorageErr("disk is faulty")

// errFileAccessDenied - cannot access file, insufficient permissions.
var errFileAccessDenied = StorageErr("file access denied")

// errTooManyOpenFiles - too many open files.
var errTooManyOpenFiles = StorageErr("too many open files, please increase 'ulimit -n'")

func osIsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

// No space left on device error
func isSysErrNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}

// Invalid argument, unsupported flags such as O_DIRECT
func isSysErrInvalidArg(err error) bool {
	return errors.Is(err, syscall.EINVAL)
}

// Input/output error
func isSysErrIO(err error) bool {
	return errors.Is(err, syscall.EIO)
}

// Check if the given error corresponds to EISDIR (is a directory).
func isSysErrIsDir(err error) bool {
	return errors.Is(err, syscall.EISDIR)
}

// Check if the given error corresponds to ENOTDIR (is not a directory).
func isSysErrNotDir(err error) bool {
	return errors.Is(err, syscall.ENOTDIR)
}

func osIsPermission(err error) bool {
	return errors.Is(err, os.ErrPermission)
}

// Check if the given error corresponds to the specific ERROR_PATH_NOT_FOUND for windows
func isSysErrPathNotFound(err error) bool {
	if runtime.GOOS != globalWindowsOSName {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return pathErr.Err == syscall.ENOENT
		}
		return false
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		var errno syscall.Errno
		if errors.As(pathErr.Err, &errno) {
			// ERROR_PATH_NOT_FOUND
			return errno == 0x03
		}
	}
	return false
}

// Check if the given error corresponds to the specific ERROR_INVALID_HANDLE for windows
func isSysErrHandleInvalid(err error) bool {
	if runtime.GOOS != globalWindowsOSName {
		return false
	}
	// Check if err contains ERROR_INVALID_HANDLE errno
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		var errno syscall.Errno
		if errors.As(pathErr.Err, &errno) {
			// ERROR_PATH_NOT_FOUND
			return errno == 0x6
		}
	}
	return false
}

// Check if given error corresponds to too many open files
func isSysErrTooManyFiles(err error) bool {
	return errors.Is(err, syscall.ENFILE) || errors.Is(err, syscall.EMFILE)
}

// Check if the given error corresponds to the ELOOP (too many symlinks).
func isSysErrTooManySymlinks(err error) bool {
	return errors.Is(err, syscall.ELOOP)
}

// Is a one place function which converts all os.PathError
// into a more FS object layer friendly form, converts
// known errors into their typed form for top level
// interpretation.
func osErrToFileErr(err error) error {
	if err == nil {
		return nil
	}
	if osIsNotExist(err) {
		return errFileNotFound
	}
	if osIsPermission(err) {
		return errFileAccessDenied
	}
	if isSysErrNotDir(err) || isSysErrIsDir(err) {
		return errFileNotFound
	}
	if isSysErrPathNotFound(err) {
		return errFileNotFound
	}
	if isSysErrTooManyFiles(err) {
		return errTooManyOpenFiles
	}
	if isSysErrHandleInvalid(err) {
		return errFileNotFound
	}
	if isSysErrIO(err) {
		return errFaultyDisk
	}
	if isSysErrInvalidArg(err) {
		// logger.LogIf(context.Background(), err)
		// For some odd calls with O_DIRECT reads
		// filesystems can return EINVAL, handle
		// these as FileNotFound instead.
		return errFileNotFound
	}
	if isSysErrNoSpace(err) {
		return errDiskFull
	}
	return err
}
