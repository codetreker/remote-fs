package localdisk

import (
	"context"
	"fmt"
)

// Status is a point-in-time operational snapshot. Failure is non-empty after a durability
// or cleanup error has made further object answers unsafe; RecoveryRecords names durable
// crash-recovery intents that remain on disk.
type Status struct {
	StoreID            ID
	InFlightOperations int
	WaitingOperations  int
	InFlightBytes      int64
	PhysicalAvailable  int64
	RecoveryRecords    int64
	Failure            string
}

// Status reports live bounds without making them part of the replaceable Objects
// interface. It returns the snapshot together with any physical measurement or retained
// durability failure. One serialized control slot remains available when ordinary
// operation and byte admission are saturated; Close drains that slot before releasing
// storage descriptors.
func (o *Objects) Status(ctx context.Context) (Status, error) {
	ticket, err := o.gate.acquireControl(ctx)
	if err != nil {
		return Status{StoreID: o.id}, fmt.Errorf("read local object-store status: %w", err)
	}
	defer ticket.release()
	operations, waiting, bytes, _ := o.gate.snapshot()
	status := Status{
		StoreID:            o.id,
		InFlightOperations: operations,
		WaitingOperations:  waiting,
		InFlightBytes:      bytes,
		RecoveryRecords:    o.recoveryRecords.Load(),
	}
	space, spaceErr := o.spaceWhileAdmitted()
	if spaceErr == nil {
		status.PhysicalAvailable = space.Avail
	}
	resultErr, failure := o.health.status(spaceErr)
	status.Failure = failure
	return status, resultErr
}
