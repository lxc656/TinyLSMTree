package utils

import (
	"hash/crc32"
	"os"
)

const (
	// MaxLevelNum _
	MaxLevelNum = 7
	// DefaultValueThreshold _
	DefaultValueThreshold = 1024
)

// osFile
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
)

// codec
var (
	MagicText    = [4]byte{'W', 'I', 'N', '!'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable is a CRC32 polynomial table
	CastagnoliCrcTable     = crc32.MakeTable(crc32.Castagnoli)
	MaxHeaderSize      int = 21
)
