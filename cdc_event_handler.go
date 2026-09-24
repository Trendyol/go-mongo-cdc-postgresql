package cdcpostgresql

import (
	"github.com/Trendyol/go-mongo-cdc-postgresql/postgresql/bulk"
)

type CdcEventHandler struct {
	bulk *bulk.Bulk
}

func (h *CdcEventHandler) BeforePartitionStop(partitionID int) {
	h.bulk.PreparePartitionRebalancing(partitionID)
}

func (h *CdcEventHandler) AfterPartitionStop(partitionID int) {
	h.bulk.FlushMessages()
	h.bulk.EndPartitionRebalancing(partitionID)
}

func (h *CdcEventHandler) BeforePartitionStart(partitionID int) {
}

func (h *CdcEventHandler) AfterPartitionStart(partitionID int) {
}
