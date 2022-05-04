package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type commandManager struct {
	cmd    *txnbase.ComposedCmd
	lsn    uint64
	csn    uint32
	driver wal.Driver
}

func newCommandManager(driver wal.Driver) *commandManager {
	return &commandManager{
		cmd:    txnbase.NewComposedCmd(),
		driver: driver,
	}
}

func (mgr *commandManager) GetCSN() uint32 {
	return mgr.csn
}

func (mgr *commandManager) AddInternalCmd(cmd txnif.TxnCmd) {
	mgr.cmd.AddCmd(cmd)
}

func (mgr *commandManager) AddCmd(cmd txnif.TxnCmd) {
	mgr.cmd.AddCmd(cmd)
	mgr.csn++
}

func (mgr *commandManager) MakeLogIndex(csn uint32) *wal.Index {
	return &wal.Index{LSN: mgr.lsn, CSN: csn, Size: mgr.csn}
}

func (mgr *commandManager) ApplyTxnRecord() (logEntry entry.Entry, err error) {
	if mgr.driver == nil {
		return
	}
	var buf []byte
	if buf, err = mgr.cmd.Marshal(); err != nil {
		panic(err)
	}
	logEntry = entry.GetBase()
	logEntry.SetType(ETTxnRecord)
	logEntry.Unmarshal(buf)

	mgr.lsn, err = mgr.driver.AppendEntry(wal.GroupC, logEntry)
	logutil.Debugf("ApplyTxnRecord LSN=%d, Size=%d", mgr.lsn, len(buf))
	return
}
