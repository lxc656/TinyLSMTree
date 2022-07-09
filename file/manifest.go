package file

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"hash/crc32"
	"io"
	"lsm/file/osFile"
	"lsm/pb"
	"lsm/utils"
	"os"
	"path/filepath"
	"sync"
)

// 磁盘中的manifest构成如下
// ｜ magic(8 B) | changes | changes| ...
// magic构成如下
// | magic text(4 B) | magic version(4 B) |
// changes的构成如下
// ｜ len(4 B) | crc(4 B) | change |

// ManifestFile 维护sst文件元信息的文件
// manifest 比较特殊，不能使用mmap，需要保证实时的写入
type ManifestFile struct {
	opt                       *osFile.FileOption
	file                      *os.File
	lock                      sync.Mutex
	deletionsRewriteThreshold int
	manifest                  *Manifest
}

type Manifest struct {
	Levels    []levelManifest          // 每一层有哪些table
	Tables    map[uint64]TableManifest // 用于快速查询每个table在哪一层
	Creations int
	Deletions int
}

// TableManifest 包含sst的基本信息
type TableManifest struct {
	Level    uint8
	Checksum []byte // 方便今后扩展
}
type levelManifest struct {
	Tables map[uint64]struct{} // table id -> table
}

//TableMeta sst 的一些元信息
type TableMeta struct {
	ID       uint64
	Checksum []byte
}

// OpenManifestFile 打开/创建 manifest文件
func OpenManifestFile(fileOpt *osFile.FileOption) (*ManifestFile, error) {
	path := filepath.Join(fileOpt.WorkDir, utils.ManifestFilename)
	manifestFile := &ManifestFile{lock: sync.Mutex{}, opt: fileOpt}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		// 打开失败 尝试创建一个新的 manifest newFile
		if !os.IsNotExist(err) {
			// 不是因为文件文件不存在而导致的错误，则直接返回错误信息
			return nil, err
		}
		newManifest := createNewManifest()
		newFile, netCreations, err := createFileAndRewrite(fileOpt.WorkDir, newManifest)
		if err != nil {
			return nil, err
		}
		utils.CondPanic(netCreations == 0, errors.Wrap(err, utils.ErrReWriteFailure.Error()))

		manifestFile.file = newFile
		manifestFile.manifest = newManifest
		return manifestFile, nil
	}

	// 打开成功，则对manifest文件进行重放
	manifest, truncOffset, err := ReplayManifestFile(file)
	if err != nil {
		_ = file.Close()
		return manifestFile, err
	}
	// 将manifest的磁盘文件进行截断，使文件大小等于`truncOffset`
	if err := file.Truncate(truncOffset); err != nil {
		_ = file.Close()
		return manifestFile, err
	}

	// 设置对文件下一个读或写的偏移量，这里设置为文件末尾
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return manifestFile, err
	}
	manifestFile.file = file
	manifestFile.manifest = manifest
	return manifestFile, nil
}

// 通过覆写方式创建一个manifest 文件, 即先创建一个rewrite文件并进行相应的数据写入
// 当数据写入成功时，再将rewrite文件改名为manifest文件
// 返回值的第二个表示覆写过程中创建的change对象个数, 即当前manifest结构体已经在追踪的sst文件个数。
func createFileAndRewrite(dir string, manifest *Manifest) (*os.File, int, error) {
	// 创建一个remanifest文件
	path := filepath.Join(dir, utils.ManifestRewriteFilename)
	manifestfile, err := os.OpenFile(path, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	//序列化magic
	buf := make([]byte, 8)
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], utils.MagicVersion)

	// 将当前内存中的manifest结构抽象为一堆的change对象
	netCreations := len(manifest.Tables)
	changes := manifest.asChanges()
	set := pb.ManifestChangeSet{Changes: changes}
	changeBuf, err := set.Marshal()
	if err != nil {
		manifestfile.Close()
		return nil, 0, err
	}

	// 序列化changes中的前缀(len和crc)和一堆change对象
	var lenAndCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenAndCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenAndCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	buf = append(buf, lenAndCrcBuf[:]...)
	buf = append(buf, changeBuf...)

	if _, err := manifestfile.Write(buf); err != nil {
		manifestfile.Close()
		return nil, 0, err
	}
	if err := manifestfile.Sync(); err != nil {
		manifestfile.Close()
		return nil, 0, err
	}
	// 某些系统要求一个文件在改名前必须关闭
	if err = manifestfile.Close(); err != nil {
		return nil, 0, err
	}

	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(path, manifestPath); err != nil {
		return nil, 0, err
	}

	// 设置对文件下一个读或写的偏移量，这里设置为文件末尾
	manifestfile, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	if _, err := manifestfile.Seek(0, io.SeekEnd); err != nil {
		manifestfile.Close()
		return nil, 0, err
	}
	if err := utils.SyncDir(dir); err != nil {
		manifestfile.Close()
		return nil, 0, err
	}

	return manifestfile, netCreations, nil
}

// 将当前manifest结构体的状态序列化成一个changes，其中包含许多change，这些change可用于重建当前manifest结构体状态
func (m *Manifest) asChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for sstId, tableManifest := range m.Tables {
		changes = append(changes, newCreateChange(sstId, int(tableManifest.Level), tableManifest.Checksum))
	}
	return changes
}

func newCreateChange(sstId uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       sstId,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

type bufReader struct {
	reader *bufio.Reader
	count  int64
}

func (r *bufReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.count += int64(n)
	return
}

// ReplayManifestFile 读取磁盘中的manifest文件，并恢复其记录的状态
func ReplayManifestFile(file *os.File) (mf *Manifest, truncOffset int64, err error) {
	reader := &bufReader{reader: bufio.NewReader(file)}

	// 读取magic
	var magicBuf [8]byte
	if _, err := io.ReadFull(reader, magicBuf[:]); err != nil {
		return nil, 0, utils.ErrBadMagic
	}
	if !bytes.Equal(magicBuf[0:4], utils.MagicText[:]) {
		return nil, 0, utils.ErrBadMagic
	}
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != uint32(utils.MagicVersion) {
		return nil, 0, utils.ErrNotSupportManifestVersion
	}

	newManifest := createNewManifest()
	var offset int64
	// 循环读取所有changes并恢复
	for {
		offset = reader.count

		// 读取len和crc
		var lenAndCrcBuf [8]byte
		_, err := io.ReadFull(reader, lenAndCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// 读取到文件末尾，则跳出循环
				break
			}
			return nil, 0, err
		}

		// 读取一个change对象
		length := binary.BigEndian.Uint32(lenAndCrcBuf[0:4])
		var changeBuf = make([]byte, length)
		if _, err := io.ReadFull(reader, changeBuf); err != nil {
			return nil, 0, err
		}

		// 数据校验
		if crc32.Checksum(changeBuf, utils.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenAndCrcBuf[4:8]) {
			return nil, 0, utils.ErrBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(changeBuf); err != nil {
			return nil, 0, err
		}

		if err := applyChangeSet(newManifest, &changeSet); err != nil {
			return nil, 0, err
		}
	}

	return newManifest, offset, err
}

func createNewManifest() *Manifest {
	return &Manifest{
		Levels: make([]levelManifest, 0),
		Tables: make(map[uint64]TableManifest),
	}
}

// 回放一堆changes
func applyChangeSet(mf *Manifest, changeSet *pb.ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(mf, change); err != nil {
			return err
		}
	}
	return nil
}

// 回放一个change
func applyManifestChange(mf *Manifest, change *pb.ManifestChange) error {
	switch change.Op {
	case pb.ManifestChange_CREATE:
		if _, exist := mf.Tables[change.Id]; exist {
			return fmt.Errorf("MANIFEST invalid, table %d exists", change.Id)
		}
		mf.Tables[change.Id] = TableManifest{
			Level:    uint8(change.Level),
			Checksum: append([]byte{}, change.Checksum...),
		}
		for len(mf.Levels) <= int(change.Level) {
			mf.Levels = append(mf.Levels, levelManifest{make(map[uint64]struct{})})
		}
		mf.Levels[change.Level].Tables[change.Id] = struct{}{}
		mf.Creations++
	case pb.ManifestChange_DELETE:
		tm, exist := mf.Tables[change.Id]
		if !exist {
			return fmt.Errorf("MANIFEST removes non-existing table %d", change.Id)
		}
		delete(mf.Levels[tm.Level].Tables, change.Id)
		delete(mf.Tables, change.Id)
		mf.Deletions++
	default:
		return utils.ErrManifestHasWrongOp
	}
	return nil
}

// Must be called while appendLock is held.
func (mf *ManifestFile) rewrite() error {
	// In Windows the files should be closed before doing a Rename.
	if err := mf.file.Close(); err != nil {
		return err
	}
	fp, nextCreations, err := createFileAndRewrite(mf.opt.WorkDir, mf.manifest)
	if err != nil {
		return err
	}
	mf.manifest.Creations = nextCreations
	mf.manifest.Deletions = 0
	mf.file = fp
	return nil
}

// Close 关闭文件
func (mf *ManifestFile) Close() error {
	if err := mf.file.Close(); err != nil {
		return err
	}
	return nil
}

// AddChanges 对外暴露的写比那更丰富
func (mf *ManifestFile) AddChanges(changesParam []*pb.ManifestChange) error {
	return mf.addChanges(changesParam)
}
func (mf *ManifestFile) addChanges(changesParam []*pb.ManifestChange) error {
	changes := pb.ManifestChangeSet{Changes: changesParam}
	buf, err := changes.Marshal()
	if err != nil {
		return err
	}

	// TODO 锁粒度可以优化
	mf.lock.Lock()
	defer mf.lock.Unlock()
	if err := applyChangeSet(mf.manifest, &changes); err != nil {
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if mf.manifest.Deletions > utils.ManifestDeletionsRewriteThreshold &&
		mf.manifest.Deletions > utils.ManifestDeletionsRatio*(mf.manifest.Creations-mf.manifest.Deletions) {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		var lenCrcBuf [8]byte
		binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(buf, utils.CastagnoliCrcTable))
		buf = append(lenCrcBuf[:], buf...)
		if _, err := mf.file.Write(buf); err != nil {
			return err
		}
	}
	err = mf.file.Sync()
	return err
}

// AddTableMeta 存储level表到manifest的level中
func (mf *ManifestFile) AddTableMeta(levelNum int, t *TableMeta) (err error) {
	mf.addChanges([]*pb.ManifestChange{
		newCreateChange(t.ID, levelNum, t.Checksum),
	})
	return err
}

// RevertToManifest 检查所有必要的表文件是否存在，并删除manifest中未引用的所有表文件。
// idMap 记录了从工作目录中读取的所有sst 的id。
func (mf *ManifestFile) RevertToManifest(idMap map[uint64]struct{}) error {
	for id := range mf.manifest.Tables {
		if _, exist := idMap[id]; !exist {
			return fmt.Errorf("table %d does not exis but recorded in manifest", id)
		}
	}

	// 删除manifest中没有引用但却存在于工作目录但sst文件
	for id := range idMap {
		if _, exist := mf.manifest.Tables[id]; !exist {
			utils.PrintErr(fmt.Errorf("Table %d  not referenced in MANIFEST", id))
			filePath := utils.SSTableFullPath(mf.opt.WorkDir, id)
			if err := os.Remove(filePath); err != nil {
				return errors.Wrapf(err, "removing table %d error", id)
			}
		}
	}
	return nil
}

func (mf *ManifestFile) GetManifest() *Manifest {
	return mf.manifest
}
