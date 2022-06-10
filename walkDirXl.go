package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type xlStorage struct {
	diskPath string
}

// WalkDirOptions provides options for WalkDir operations.
type WalkDirOptions struct {
	// Bucket to scanner
	Bucket string

	// Directory inside the bucket.
	BaseDir string

	// Do a full recursive scan.
	Recursive bool

	// ReportNotFound will return errFileNotFound if all disks reports the BaseDir cannot be found.
	ReportNotFound bool

	// FilterPrefix will only return results with given prefix within folder.
	// Should never contain a slash.
	FilterPrefix string

	// ForwardTo will forward to the given object path.
	ForwardTo string
}

// getVolDir - will convert incoming volume names to
// corresponding valid volume names on the backend in a platform
// compatible way for all operating systems. If volume is not found
// an error is generated.
func (s *xlStorage) getVolDir(volume string) (string, error) {
	volumeDir := pathJoin(s.diskPath, volume)
	return volumeDir, nil
}

func Access(name string) error {
	if err := unix.Access(name, unix.R_OK|unix.W_OK); err != nil {
		return &os.PathError{Op: "lstat", Path: name, Err: err}
	}
	return nil
}

// WalkDir will traverse a directory and return all entries found.
// On success a sorted meta cache stream will be returned.
// Metadata has data stripped, if any.
func (s *xlStorage) WalkDir(ctx context.Context, opts WalkDirOptions) int {

	totalFiles := 0
	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(opts.Bucket)
	if err != nil {
		log.Fatal(err)
	}

	// Stat a volume entry.
	if err = Access(volumeDir); err != nil {
		log.Fatal(err)
	}

	/*
		// Use a small block size to start sending quickly
		w := newMetacacheWriter(wr, 16<<10)
		w.reuseBlocks = true // We are not sharing results, so reuse buffers.
		defer w.Close()
		out, err := w.stream()
		if err != nil {
			return err
		}
		defer close(out)
	*/

	// Fast exit track to check if we are listing an object with
	// a trailing slash, this will avoid to list the object content.
	if HasSuffix(opts.BaseDir, SlashSeparator) {
		_, err := s.readMetadata(ctx, pathJoin(volumeDir,
			opts.BaseDir[:len(opts.BaseDir)-1]+globalDirSuffix,
			xlStorageFormatFile))
		if err == nil {
			// if baseDir is already a directory object, consider it
			// as part of the list call, this is a AWS S3 specific
			// behavior.
			totalFiles += 1
			fmt.Println(opts.BaseDir)
			/*
				out <- metaCacheEntry{
					name:     opts.BaseDir,
					metadata: metadata,
				}
			*/
		} else {
			st, sterr := os.Lstat(pathJoin(volumeDir, opts.BaseDir, xlStorageFormatFile))
			if sterr == nil && st.Mode().IsRegular() {
				log.Fatal("File not found.")
			}
		}
	}

	prefix := opts.FilterPrefix
	var scanDir func(path string) error

	scanDir = func(current string) error {
		// Skip forward, if requested...
		forward := ""
		if len(opts.ForwardTo) > 0 && strings.HasPrefix(opts.ForwardTo, current) {
			forward = strings.TrimPrefix(opts.ForwardTo, current)
			if idx := strings.IndexByte(forward, '/'); idx > 0 {
				forward = forward[:idx]
			}
		}
		if contextCanceled(ctx) {
			return ctx.Err()
		}

		// s.walkMu.Lock()
		entries, err := s.ListDir(ctx, opts.Bucket, current, -1)
		// s.walkMu.Unlock()
		if err != nil {
			// Folder could have gone away in-between
			// REMOVED

			// ignore this!
			/*
				if opts.ReportNotFound && err == errFileNotFound && current == opts.BaseDir {
					return errFileNotFound
				}
			*/
			// Forward some errors?
			return nil
		}
		if len(entries) == 0 {
			return nil
		}
		dirObjects := make(map[string]struct{})
		for i, entry := range entries {
			if len(prefix) > 0 && !strings.HasPrefix(entry, prefix) {
				// Do do not retain the file, since it doesn't
				// match the prefix.
				entries[i] = ""
				continue
			}
			if len(forward) > 0 && entry < forward {
				// Do do not retain the file, since its
				// lexially smaller than 'forward'
				entries[i] = ""
				continue
			}
			if strings.HasSuffix(entry, SlashSeparator) {
				if strings.HasSuffix(entry, globalDirSuffixWithSlash) {
					// Add without extension so it is sorted correctly.
					entry = strings.TrimSuffix(entry, globalDirSuffixWithSlash) + SlashSeparator
					dirObjects[entry] = struct{}{}
					entries[i] = entry
					continue
				}
				// Trim slash, maybe compiler is clever?
				entries[i] = entries[i][:len(entry)-1]
				continue
			}
			// Do do not retain the file.
			entries[i] = ""

			if contextCanceled(ctx) {
				return ctx.Err()
			}
			// If root was an object return it as such.
			if HasSuffix(entry, xlStorageFormatFile) {
				/*
					var meta metaCacheEntry
					s.walkReadMu.Lock()
					meta.metadata, err = s.readMetadata(ctx, pathJoin(volumeDir, current, entry))
					s.walkReadMu.Unlock()
				*/
				if err != nil {
					// logger.LogIf(ctx, err)
					continue
				}
				/*
					meta.name = strings.TrimSuffix(entry, xlStorageFormatFile)
					meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
					meta.name = pathJoin(current, meta.name)
					meta.name = decodeDirObject(meta.name)
				*/
				totalFiles += 1
				fmt.Println(strings.TrimSuffix(entry, xlStorageFormatFile))
				// out <- meta
				return nil
			}
			// Check legacy.
			if HasSuffix(entry, xlStorageFormatFileV1) {
				// var meta metaCacheEntry
				// s.walkReadMu.Lock()
				_, err = ReadFile(pathJoin(volumeDir, current, entry))
				// s.walkReadMu.Unlock()
				if err != nil {
					// logger.LogIf(ctx, err)
					continue
				}
				/*
					meta.name = strings.TrimSuffix(entry, xlStorageFormatFileV1)
					meta.name = strings.TrimSuffix(meta.name, SlashSeparator)
					meta.name = pathJoin(current, meta.name)
				*/
				totalFiles += 1
				fmt.Println(strings.TrimSuffix(entry, xlStorageFormatFile))
				//out <- meta
				return nil
			}
			// Skip all other files.
		}

		// Process in sort order.
		sort.Strings(entries)
		dirStack := make([]string, 0, 5)
		prefix = "" // Remove prefix after first level as we have already filtered the list.
		if len(forward) > 0 {
			idx := sort.SearchStrings(entries, forward)
			if idx > 0 {
				entries = entries[idx:]
			}
		}

		for _, entry := range entries {
			if entry == "" {
				continue
			}
			if contextCanceled(ctx) {
				return ctx.Err()
			}
			metaname := pathJoin(current, entry)

			// If directory entry on stack before this, pop it now.
			for len(dirStack) > 0 && dirStack[len(dirStack)-1] < metaname {
				pop := dirStack[len(dirStack)-1]
				totalFiles += 1
				fmt.Println(pop)
				//out <- metaCacheEntry{name: pop}
				if opts.Recursive {
					// Scan folder we found. Should be in correct sort order where we are.
					forward = ""
					if len(opts.ForwardTo) > 0 && strings.HasPrefix(opts.ForwardTo, pop) {
						forward = strings.TrimPrefix(opts.ForwardTo, pop)
					}
					scanDir(pop)
				}
				dirStack = dirStack[:len(dirStack)-1]
			}

			// All objects will be returned as directories, there has been no object check yet.
			// Check it by attempting to read metadata.
			_, isDirObj := dirObjects[entry]
			if isDirObj {
				metaname = metaname[:len(metaname)-1] + globalDirSuffixWithSlash
			}

			// s.walkReadMu.Lock()
			_, err = s.readMetadata(ctx, pathJoin(volumeDir, metaname, xlStorageFormatFile))
			// s.walkReadMu.Unlock()
			switch {
			case err == nil:
				// It was an object
				if isDirObj {
					metaname = strings.TrimSuffix(metaname, globalDirSuffixWithSlash) + SlashSeparator
				}
				//out <- meta
				totalFiles += 1
				fmt.Println(metaname)
			case osIsNotExist(err), isSysErrIsDir(err):
				_, err = ReadFile(pathJoin(volumeDir, metaname, xlStorageFormatFileV1))
				if err == nil {
					// It was an object
					// out <- meta
					totalFiles += 1
					fmt.Println(metaname)
					continue
				}

				// NOT an object, append to stack (with slash)
				// If dirObject, but no metadata (which is unexpected) we skip it.
				if !isDirObj {
					if !isDirEmpty(pathJoin(volumeDir, metaname+SlashSeparator)) {
						dirStack = append(dirStack, metaname+SlashSeparator)
					}
				}
			case isSysErrNotDir(err):
				// skip
			default:
				// skip
			}
		}

		// If directory entry left on stack, pop it now.
		for len(dirStack) > 0 {
			pop := dirStack[len(dirStack)-1]
			totalFiles += 1
			fmt.Println(pop)
			// out <- metaCacheEntry{name: pop}
			if opts.Recursive {
				// Scan folder we found. Should be in correct sort order where we are.
				scanDir(pop)
			}
			dirStack = dirStack[:len(dirStack)-1]
		}
		return nil
	}

	// Stream output.
	scanDir(opts.BaseDir)
	return totalFiles
}

// ListDir - return all the entries at the given directory path.
// If an entry is a directory it will be returned with a trailing SlashSeparator.
func (s *xlStorage) ListDir(ctx context.Context, volume, dirPath string, count int) (entries []string, err error) {
	if contextCanceled(ctx) {
		return nil, ctx.Err()
	}

	// Verify if volume is valid and it exists.
	volumeDir, err := s.getVolDir(volume)
	if err != nil {
		return nil, err
	}

	dirPathAbs := pathJoin(volumeDir, dirPath)
	if count > 0 {
		entries, err = readDirN(dirPathAbs, count)
	} else {
		entries, err = readDir(dirPathAbs)
	}
	if err != nil {
		if err == errFileNotFound {
			if ierr := Access(volumeDir); ierr != nil {
				if osIsNotExist(ierr) {
					return nil, errVolumeNotFound
				} else if isSysErrIO(ierr) {
					return nil, errFaultyDisk
				}
			}
		}
		return nil, err
	}

	return entries, nil
}

func (s *xlStorage) readMetadata(ctx context.Context, itemPath string) ([]byte, error) {
	if contextCanceled(ctx) {
		return nil, ctx.Err()
	}

	if err := checkPathLength(itemPath); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(itemPath, readMode, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.IsDir() {
		return nil, &os.PathError{
			Op:   "open",
			Path: itemPath,
			Err:  syscall.EISDIR,
		}
	}
	return readXLMetaNoData(f, stat.Size())
}

// Read at most this much on initial read.
const metaDataReadDefault = 4 << 10

// readXLMetaNoData will load the metadata, but skip data segments.
// This should only be used when data is never interesting.
// If data is not xlv2, it is returned in full.
func readXLMetaNoData(r io.Reader, size int64) ([]byte, error) {
	initial := size
	hasFull := true
	if initial > metaDataReadDefault {
		initial = metaDataReadDefault
		hasFull = false
	}

	buf := metaDataPoolGet()[:initial]
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("readXLMetaNoData.ReadFull: %w", err)
	}
	readMore := func(n int64) error {
		has := int64(len(buf))
		if has >= n {
			return nil
		}
		if hasFull || n > size {
			return io.ErrUnexpectedEOF
		}
		extra := n - has
		buf = append(buf, make([]byte, extra)...)
		_, err := io.ReadFull(r, buf[has:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Returned if we read nothing.
				return fmt.Errorf("readXLMetaNoData.readMore: %w", io.ErrUnexpectedEOF)
			}
			return fmt.Errorf("readXLMetaNoData.readMore: %w", err)
		}
		return nil
	}
	return buf, nil

	_, _, _, err = checkXL2V1(buf)
	if err != nil {
		err = readMore(size)
		return buf, err
	}
	return buf, nil
	/*
		switch major {
		case 1:
			switch minor {
			case 0:
				err = readMore(size)
				return buf, err
			case 1, 2:
				sz, tmp, err := msgp.ReadBytesHeader(tmp)
				if err != nil {
					return nil, err
				}
				want := int64(sz) + int64(len(buf)-len(tmp))

				// v1.1 does not have CRC.
				if minor < 2 {
					if err := readMore(want); err != nil {
						return nil, err
					}
					return buf[:want], nil
				}

				// CRC is variable length, so we need to truncate exactly that.
				wantMax := want + msgp.Uint32Size
				if wantMax > size {
					wantMax = size
				}
				if err := readMore(wantMax); err != nil {
					return nil, err
				}

				tmp = buf[want:]
				_, after, err := msgp.ReadUint32Bytes(tmp)
				if err != nil {
					return nil, err
				}
				want += int64(len(tmp) - len(after))

				return buf[:want], err

			default:
				return nil, errors.New("unknown minor metadata version")
			}
		default:
			return nil, errors.New("unknown major metadata version")
		}
	*/
}

// Return used metadata byte slices here.
var metaDataPool = sync.Pool{New: func() interface{} { return make([]byte, 0, metaDataReadDefault) }}

// metaDataPoolGet will return a byte slice with capacity at least metaDataReadDefault.
// It will be length 0.
func metaDataPoolGet() []byte {
	return metaDataPool.Get().([]byte)[:0]
}

// isDirEmpty - returns whether given directory is empty or not.
func isDirEmpty(dirname string) bool {
	entries, err := readDirN(dirname, 1)
	if err != nil {
		if err != errFileNotFound {
			// logger.LogIf(GlobalContext, err)
		}
		return false
	}
	return len(entries) == 0
}

// Options for readDir function call
type readDirOpts struct {
	// The maximum number of entries to return
	count int
	// Follow directory symlink
	followDirSymlink bool
}

// Return all the entries at the directory dirPath.
func readDir(dirPath string) (entries []string, err error) {
	return readDirWithOpts(dirPath, readDirOpts{count: -1})
}

// Return up to count entries at the directory dirPath.
func readDirN(dirPath string, count int) (entries []string, err error) {
	return readDirWithOpts(dirPath, readDirOpts{count: count})
}

// The buffer must be at least a block long.
// refer https://github.com/golang/go/issues/24015
const blockSize = 8 << 10 // 8192

// By default atleast 128 entries in single getdents call (1MiB buffer)
var (
	direntPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, blockSize*128)
			return &buf
		},
	}

	direntNamePool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, blockSize)
			return &buf
		},
	}
)

// Return count entries at the directory dirPath and all entries
// if count is set to -1
func readDirWithOpts(dirPath string, opts readDirOpts) (entries []string, err error) {
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, osErrToFileErr(err)
	}
	defer f.Close()

	bufp := direntPool.Get().(*[]byte)
	defer direntPool.Put(bufp)
	buf := *bufp

	nameTmp := direntNamePool.Get().(*[]byte)
	defer direntNamePool.Put(nameTmp)
	tmp := *nameTmp

	boff := 0 // starting read position in buf
	nbuf := 0 // end valid data in buf

	count := opts.count

	for count != 0 {
		if boff >= nbuf {
			boff = 0
			nbuf, err = syscall.ReadDirent(int(f.Fd()), buf)
			if err != nil {
				if isSysErrNotDir(err) {
					return nil, errFileNotFound
				}
				return nil, osErrToFileErr(err)
			}
			if nbuf <= 0 {
				break
			}
		}
		consumed, name, typ, err := parseDirEnt(buf[boff:nbuf])
		if err != nil {
			return nil, err
		}
		boff += consumed
		if len(name) == 0 || bytes.Equal(name, []byte{'.'}) || bytes.Equal(name, []byte{'.', '.'}) {
			continue
		}

		// Fallback for filesystems (like old XFS) that don't
		// support Dirent.Type and have DT_UNKNOWN (0) there
		// instead.
		if typ == unexpectedFileMode || typ&os.ModeSymlink == os.ModeSymlink {
			fi, err := os.Stat(pathJoin(dirPath, string(name)))
			if err != nil {
				// It got deleted in the meantime, not found
				// or returns too many symlinks ignore this
				// file/directory.
				if osIsNotExist(err) || isSysErrPathNotFound(err) ||
					isSysErrTooManySymlinks(err) {
					continue
				}
				return nil, err
			}

			// Ignore symlinked directories.
			if !opts.followDirSymlink && typ&os.ModeSymlink == os.ModeSymlink && fi.IsDir() {
				continue
			}

			typ = fi.Mode() & os.ModeType
		}

		var nameStr string
		if typ.IsRegular() {
			nameStr = string(name)
		} else if typ.IsDir() {
			// Use temp buffer to append a slash to avoid string concat.
			tmp = tmp[:len(name)+1]
			copy(tmp, name)
			tmp[len(tmp)-1] = '/' // SlashSeparator
			nameStr = string(tmp)
		}

		count--
		entries = append(entries, nameStr)
	}

	return
}

func parseDirEnt(buf []byte) (consumed int, name []byte, typ os.FileMode, err error) {
	// golang.org/issue/15653
	dirent := (*syscall.Dirent)(unsafe.Pointer(&buf[0]))
	if v := unsafe.Offsetof(dirent.Reclen) + unsafe.Sizeof(dirent.Reclen); uintptr(len(buf)) < v {
		return consumed, nil, typ, fmt.Errorf("buf size of %d smaller than dirent header size %d", len(buf), v)
	}
	if len(buf) < int(dirent.Reclen) {
		return consumed, nil, typ, fmt.Errorf("buf size %d < record length %d", len(buf), dirent.Reclen)
	}
	consumed = int(dirent.Reclen)
	if direntInode(dirent) == 0 { // File absent in directory.
		return
	}
	switch dirent.Type {
	case syscall.DT_REG:
		typ = 0
	case syscall.DT_DIR:
		typ = os.ModeDir
	case syscall.DT_LNK:
		typ = os.ModeSymlink
	default:
		// Skip all other file types. Revisit if/when this code needs
		// to handle such files, MinIO is only interested in
		// files and directories.
		typ = unexpectedFileMode
	}

	nameBuf := (*[unsafe.Sizeof(dirent.Name)]byte)(unsafe.Pointer(&dirent.Name[0]))
	nameLen, err := direntNamlen(dirent)
	if err != nil {
		return consumed, nil, typ, err
	}

	return consumed, nameBuf[:nameLen], typ, nil
}

// unexpectedFileMode is a sentinel (and bogus) os.FileMode
// value used to represent a syscall.DT_UNKNOWN Dirent.Type.
const unexpectedFileMode os.FileMode = os.ModeNamedPipe | os.ModeSocket | os.ModeDevice

func direntInode(dirent *syscall.Dirent) uint64 {
	return dirent.Ino
}

func direntNamlen(dirent *syscall.Dirent) (uint64, error) {
	const fixedHdr = uint16(unsafe.Offsetof(syscall.Dirent{}.Name))
	nameBuf := (*[unsafe.Sizeof(dirent.Name)]byte)(unsafe.Pointer(&dirent.Name[0]))
	const nameBufLen = uint16(len(nameBuf))
	limit := dirent.Reclen - fixedHdr
	if limit > nameBufLen {
		limit = nameBufLen
	}
	// Avoid bugs in long file names
	// https://github.com/golang/tools/commit/5f9a5413737ba4b4f692214aebee582b47c8be74
	nameLen := bytes.IndexByte(nameBuf[:limit], 0)
	if nameLen < 0 {
		return 0, fmt.Errorf("failed to find terminating 0 byte in dirent")
	}
	return uint64(nameLen), nil
}
