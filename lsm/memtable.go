package lsm

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io/ioutil"
	"lsm/file"
	"lsm/file/osFile"
	"lsm/utils"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

const walFileExt string = ".wal"

// MemTable
type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *utils.SkipList
	buf        *bytes.Buffer
	maxVersion uint64
}

func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	fileOpt := &osFile.FileOption{
		WorkDir:  lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      newFid,
		FileName: filePath(lsm.option.WorkDir, newFid),
	}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList(int64(1 << 20)), lsm: lsm}
}

// Close
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}
	if err := m.sl.Close(); err != nil {
		return err
	}
	return nil
}

func (m *memTable) set(entry *utils.Entry) error {
	// 写到wal 日志中，防止崩溃
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到memtable中
	if err := m.sl.Add(entry); err != nil {
		return err
	}
	return nil
}

func (m *memTable) Get(key []byte) (*utils.Entry, error) {
	// 索引检查当前的key是否在表中 O(1) 的时间复杂度
	// 从内存表中获取数据
	return m.sl.Search(key), nil
}

func (m *memTable) Size() int64 {
	return m.sl.Size()
}

//recovery
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	// 从工作目录中获取所有文件
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
	}
	var walFileId []uint64
	maxFid := lsm.levels.maxFID
	// 识别后缀为.wal的文件
	for _, file := range files {
		if strings.HasSuffix(file.Name(), walFileExt) {
			fileNameLen := len(file.Name())
			fid, err := strconv.ParseUint(file.Name()[:fileNameLen-len(walFileExt)], 10, 64)
			if err != nil {
				utils.Panic(err)
			}
			if maxFid < fid {
				// 当前wal文件的fid比maxFid大，因此进行更新
				maxFid = fid
			}
			walFileId = append(walFileId, fid)
		}
	}

	// 由于wal文件的存在，所以对fids进行一下排序
	sort.Slice(walFileId, func(i, j int) bool {
		return walFileId[i] < walFileId[j]
	})

	// 对memTable进行恢复
	var imms []*memTable
	for _, fid := range walFileId {
		memTable, err := lsm.RecoveryMemTable(fid)
		utils.Panic(err)
		if memTable.sl.Size() != 0 {
			imms = append(imms, memTable)
		}
	}
	// 更新最终的maxfid，
	// 由于初始化时一定是串行执行的，因此这里不需要原子操作
	lsm.levels.maxFID = maxFid
	return lsm.NewMemtable(), imms
}

func (lsm *LSM) RecoveryMemTable(fid uint64) (*memTable, error) {
	fileOpt := &osFile.FileOption{
		WorkDir:  lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: filePath(lsm.option.WorkDir, fid),
	}
	s := utils.NewSkipList(int64(1 << 20))
	mt := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = file.OpenWalFile(fileOpt)
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}
func filePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}

// UpdateSkipList 恢复wal文件中存储的跳表结构
func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	endOff, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}
	return m.wal.Truncate(int64(endOff))
}

func (m *memTable) replayFunction(opt *lsmOptions) func(*utils.Entry, *utils.ValuePtr) error {
	return func(e *utils.Entry, _ *utils.ValuePtr) error { // Function for replaying.
		if ts := utils.ParseTs(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		return m.sl.Add(e)
	}
}
