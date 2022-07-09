package utils

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	gopath = path.Join(os.Getenv("GOPATH"), "src") + "/"
)

// NotFoundKey 找不到key
var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")
	// ErrReWriteFailure reWrite failure
	ErrReWriteFailure = errors.New("reWrite failure")
	// ErrBadMagic bad magic
	ErrBadMagic = errors.New("bad magic")
	// ErrBadChecksum bad check sum
	ErrBadChecksum = errors.New("bad check sum")
	// ErrChecksumMismatch is returned at checksum mismatch.
	ErrChecksumMismatch = errors.New("checksum mismatch")
	// ErrNotSupportManifestVersion 文件的版本号不匹配
	ErrNotSupportManifestVersion = errors.New("not support this manifest version")
	ErrTruncate                  = errors.New("Do truncate")
	ErrStop                      = errors.New("Stop")

	// ErrManifestHasWrongOp manifest文件中记录了错误的操作（manifest文件只支持create和delete操作）
	ErrManifestHasWrongOp = errors.New("manifest contain wrong operation in change")

	// compact
	ErrFillTables = errors.New("Unable to fill tables")
)

// Panic 如果err 不为nil 则panicc
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// Panic2 _
func Panic2(_ interface{}, err error) {
	Panic(err)
}

// PrintErr err
func PrintErr(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}

// WarpErr err
func WarpErr(format string, err error) error {
	if err != nil {
		fmt.Printf("%s %s %s", format, location(2, true), err)
	}
	return err
}
func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}

	if fullPath {
		if strings.HasPrefix(file, gopath) {
			file = file[len(gopath):]
		}
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}

// CondPanic 如果满足`condition`, 则在 `error`上panic
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
