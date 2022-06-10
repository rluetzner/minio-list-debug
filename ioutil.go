package main

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/dustin/go-humanize"
	"github.com/ncw/directio"
	"golang.org/x/sys/unix"
)

type ODirectReader struct {
	File      io.Reader
	SmallFile bool
	err       error
	buf       []byte
	bufp      *[]byte
	seenRead  bool
	Fd        uintptr
}

// Block sizes constant.
const (
	BlockSizeSmall       = 128 * humanize.KiByte // Default r/w block size for smaller objects.
	BlockSizeLarge       = 2 * humanize.MiByte   // Default r/w block size for larger objects.
	BlockSizeReallyLarge = 4 * humanize.MiByte   // Default write block size for objects per shard >= 64MiB
)

// O_DIRECT aligned sync.Pool's
var (
	ODirectPoolXLarge = sync.Pool{
		New: func() interface{} {
			b := AlignedBlock(BlockSizeReallyLarge)
			return &b
		},
	}
	ODirectPoolLarge = sync.Pool{
		New: func() interface{} {
			b := AlignedBlock(BlockSizeLarge)
			return &b
		},
	}
	ODirectPoolSmall = sync.Pool{
		New: func() interface{} {
			b := AlignedBlock(BlockSizeSmall)
			return &b
		},
	}
)

// AlignedBlock - pass through to directio implementation.
func AlignedBlock(BlockSize int) []byte {
	return directio.AlignedBlock(BlockSize)
}

func ReadFile(name string) ([]byte, error) {
	f, err := OpenFileDirectIO(name, readMode, 0666)
	if err != nil {
		return nil, err
	}
	r := &ODirectReader{
		File:      f,
		SmallFile: true,
	}
	defer f.Close()
	defer r.Close()

	st, err := f.Stat()
	if err != nil {
		return io.ReadAll(r)
	}
	dst := make([]byte, st.Size())
	_, err = io.ReadFull(r, dst)
	return dst, err
}

// OpenFileDirectIO - bypass kernel cache.
func OpenFileDirectIO(filePath string, flag int, perm os.FileMode) (*os.File, error) {
	return directio.OpenFile(filePath, flag, perm)
}

// DisableDirectIO - disables directio mode.
func DisableDirectIO(fd uintptr) error {
	flag, err := unix.FcntlInt(fd, unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	flag = flag & ^(syscall.O_DIRECT)
	_, err = unix.FcntlInt(fd, unix.F_SETFL, flag)
	return err
}

// Read - Implements Reader interface.
func (o *ODirectReader) Read(buf []byte) (n int, err error) {
	if o.err != nil && (len(o.buf) == 0 || !o.seenRead) {
		return 0, o.err
	}
	if o.buf == nil {
		if o.SmallFile {
			o.bufp = ODirectPoolSmall.Get().(*[]byte)
		} else {
			o.bufp = ODirectPoolLarge.Get().(*[]byte)
		}
	}
	if !o.seenRead {
		o.buf = *o.bufp
		n, err = o.File.Read(o.buf)
		if err != nil && err != io.EOF {
			if isSysErrInvalidArg(err) {
				if err = DisableDirectIO(o.Fd); err != nil {
					o.err = err
					return n, err
				}
				n, err = o.File.Read(o.buf)
			}
			if err != nil && err != io.EOF {
				o.err = err
				return n, err
			}
		}
		if n == 0 {
			// err is likely io.EOF
			o.err = err
			return n, err
		}
		o.err = err
		o.buf = o.buf[:n]
		o.seenRead = true
	}
	if len(buf) >= len(o.buf) {
		n = copy(buf, o.buf)
		o.seenRead = false
		return n, o.err
	}
	n = copy(buf, o.buf)
	o.buf = o.buf[n:]
	// There is more left in buffer, do not return any EOF yet.
	return n, nil
}

// Close - Release the buffer and close the file.
func (o *ODirectReader) Close() {
	if o.bufp != nil {
		if o.SmallFile {
			ODirectPoolSmall.Put(o.bufp)
		} else {
			ODirectPoolLarge.Put(o.bufp)
		}
		o.bufp = nil
		o.buf = nil
	}
	o.err = errors.New("internal error: ODirectReader Read after Close")
}
