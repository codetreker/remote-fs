package serviceapi

import (
	"context"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type Capabilities struct {
	FileSession      FileSessionLimits `json:"fileSession"`
	MaxTransferBytes Decimal           `json:"maxTransferBytes"`
}

type FileSessionLimits struct {
	LeaseNanoseconds   Decimal `json:"leaseNanoseconds"`
	HistoryNanoseconds Decimal `json:"historyNanoseconds"`
	MaxFileSize        Decimal `json:"maxFileSize"`
	MaxFiles           Decimal `json:"maxFiles"`
	MaxOperations      Decimal `json:"maxOperations"`
	MaxWaiters         Decimal `json:"maxWaiters"`
	MaxLockOwners      Decimal `json:"maxLockOwners"`
	MaxLockRanges      Decimal `json:"maxLockRanges"`
	MaxPendingLocks    Decimal `json:"maxPendingLocks"`
	MaxLockActions     Decimal `json:"maxLockActions"`
	MaxCloseActions    Decimal `json:"maxCloseActions"`
}

func FileSessionLimitsFromOptions(options storage.FileSessionOptions) FileSessionLimits {
	return FileSessionLimits{
		LeaseNanoseconds: DecimalFromInt64(int64(options.Lease)), HistoryNanoseconds: DecimalFromInt64(int64(options.History)),
		MaxFileSize: DecimalFromInt64(options.MaxFileSize), MaxFiles: DecimalFromInt64(int64(options.MaxFiles)),
		MaxOperations: DecimalFromInt64(int64(options.MaxOperations)), MaxWaiters: DecimalFromInt64(int64(options.MaxWaiters)),
		MaxLockOwners: DecimalFromInt64(int64(options.MaxLockOwners)), MaxLockRanges: DecimalFromInt64(int64(options.MaxLockRanges)),
		MaxPendingLocks: DecimalFromInt64(int64(options.MaxPendingLocks)), MaxLockActions: DecimalFromInt64(int64(options.MaxLockActions)),
		MaxCloseActions: DecimalFromInt64(int64(options.MaxCloseActions)),
	}
}

func (limits FileSessionLimits) NativeOptions() (storage.FileSessionOptions, error) {
	var options storage.FileSessionOptions
	lease, err := limits.LeaseNanoseconds.Int64()
	if err != nil {
		return options, err
	}
	history, err := limits.HistoryNanoseconds.Int64()
	if err != nil {
		return options, err
	}
	size, err := limits.MaxFileSize.Int64()
	if err != nil {
		return options, err
	}
	options.Lease, options.History, options.MaxFileSize = time.Duration(lease), time.Duration(history), size
	for _, field := range []struct {
		value  Decimal
		target *int
	}{
		{limits.MaxFiles, &options.MaxFiles}, {limits.MaxOperations, &options.MaxOperations}, {limits.MaxWaiters, &options.MaxWaiters},
		{limits.MaxLockOwners, &options.MaxLockOwners}, {limits.MaxLockRanges, &options.MaxLockRanges}, {limits.MaxPendingLocks, &options.MaxPendingLocks},
		{limits.MaxLockActions, &options.MaxLockActions}, {limits.MaxCloseActions, &options.MaxCloseActions},
	} {
		n, err := field.value.Int64()
		if err != nil {
			return storage.FileSessionOptions{}, err
		}
		if n > int64(math.MaxInt) {
			return storage.FileSessionOptions{}, errors.New("file session count exceeds local integer capacity")
		}
		*field.target = int(n)
	}
	if err := options.Check(); err != nil {
		return storage.FileSessionOptions{}, err
	}
	return options, nil
}

func (c *Client) Capabilities(ctx context.Context) (Capabilities, error) {
	var result Capabilities
	err := c.exchange(ctx, http.MethodGet, "/capabilities", nil, &result)
	return result, err
}
