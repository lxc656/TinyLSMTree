package lsm

import "lsm/utils"

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *lsmOptions
	closer     *utils.Closer
	maxMemFID  uint32
}

//lsmOptions _
type lsmOptions struct {
	WorkDir      string
	MemTableSize int64
	SSTableMaxSz int64
	// BlockSize is the size of each block inside SSTable in bytes.
	BlockSize int
	// BloomFalsePositive is the false positive probabiltiy of bloom filter.
	BloomFalsePositive float64

	// compact
	NumCompactors       int
	BaseLevelSize       int64
	LevelSizeMultiplier int // 决定level之间期望的size比例
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int
}

func initLSM(opt *lsmOptions) *LSM {
	lsm := &LSM{option: opt}
	lsm.levels = lsm.initLevelManager(opt)
	lsm.memTable, lsm.immutables = lsm.recovery()
	lsm.closer = utils.NewCloser(1)
	return lsm
}
func (lsm *LSM) Close() error {
	if lsm.memTable != nil {
		if err := lsm.memTable.close(); err != nil {
			return err
		}
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	// 等待合并过程的结束
	lsm.closer.Close()
	return nil
}

func (lsm *LSM) StartCompacter() {
	n := lsm.option.NumCompactors //用于配置有几个compact协程
	lsm.closer.Add(n)
	for i := 0; i < n; i++ {
		go lsm.levels.runCompacter(i)
	}
}

// Set _
func (lsm *LSM) Set(entry *utils.Entry) (err error) {
	// 检查当前memtable是否写满，是的话创建新的memtable,并将当前内存表写到immutables中
	// 否则写入当前memtable中
	if int64(lsm.memTable.wal.Size())+
		int64(utils.EstimateWalCodecSize(entry)) > lsm.option.MemTableSize {
		lsm.immutables = append(lsm.immutables, lsm.memTable)
		lsm.memTable = lsm.NewMemtable()
	}

	if err = lsm.memTable.set(entry); err != nil {
		return err
	}
	// 检查是否存在immutable需要刷盘，
	for _, immutable := range lsm.immutables {
		if err = lsm.levels.flush(immutable); err != nil {
			return err
		}
		err = immutable.close()
		utils.Panic(err)
	}
	if len(lsm.immutables) != 0 {
		// TODO 将lsm的immutables队列置空，这里可以优化一下节省内存空间
		lsm.immutables = make([]*memTable, 0)
	}
	return err
}

// Get _
func (lsm *LSM) Get(key []byte) (*utils.Entry, error) {
	var (
		entry *utils.Entry
		err   error
	)
	// 从内存表中查询,先查活跃表，在查不变表
	if entry, err = lsm.memTable.Get(key); entry != nil {
		return entry, err
	}

	for i := len(lsm.immutables) - 1; i >= 0; i-- {
		if entry, err = lsm.immutables[i].Get(key); entry != nil {
			return entry, err
		}
	}
	// 从level manger查询
	return lsm.levels.Get(key)
}
