package sqlvalue

import (
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func StoredInteger(value any, storageClass string) (int64, bool) {
	if storageClass != "integer" {
		return 0, false
	}
	integer, ok := value.(int64)
	return integer, ok
}

func WouldExceed(limit int64, values ...int64) bool {
	remaining := limit
	for _, value := range values {
		if value > remaining {
			return true
		}
		remaining -= value
	}
	return false
}

// StoredTime splits an instant into seconds and nanoseconds within that second,
// preserving the range of time.Time without a nanosecond counter overflow.
func StoredTime(t time.Time) (sec int64, nsec int32) {
	return t.Unix(), int32(t.Nanosecond())
}

func LoadedTime(sec int64, nsec int32) time.Time {
	return time.Unix(sec, int64(nsec))
}

// Empty content is NULL because the column references objects, and no object has an empty key.
func StoredKey(key metastore.Key) any {
	if key == "" {
		return nil
	}
	return string(key)
}
