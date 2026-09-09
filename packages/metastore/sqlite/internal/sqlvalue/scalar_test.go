package sqlvalue

import (
	"math"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func TestStoredIntegerRequiresBothTheSQLClassAndInt64Value(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
		class string
		want  int64
		valid bool
	}{
		{"zero", int64(0), "integer", 0, true},
		{"negative", int64(-1), "integer", -1, true},
		{"maximum", int64(math.MaxInt64), "integer", math.MaxInt64, true},
		{"minimum", int64(math.MinInt64), "integer", math.MinInt64, true},
		{"text class", int64(1), "text", 0, false},
		{"null", nil, "null", 0, false},
		{"integer label with text", "1", "integer", 0, false},
		{"integer label with float", float64(1), "integer", 0, false},
		{"integer label with native int", int(1), "integer", 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, valid := StoredInteger(test.value, test.class)
			if got != test.want || valid != test.valid {
				t.Fatalf("StoredInteger(%v, %q) = %d, %v; want %d, %v", test.value, test.class, got, valid, test.want, test.valid)
			}
		})
	}
}

func TestWouldExceedPreservesExactLimitsWithoutAddingOverflow(t *testing.T) {
	for _, test := range []struct {
		name   string
		limit  int64
		values []int64
		want   bool
	}{
		{"zero usage", 0, []int64{0, 0}, false},
		{"below limit", 10, []int64{6, 3}, false},
		{"exact limit", 10, []int64{6, 4, 0}, false},
		{"over limit", 10, []int64{6, 4, 1}, true},
		{"single excess", 10, []int64{11}, true},
		{"maximum exact", math.MaxInt64, []int64{math.MaxInt64 - 1, 1}, false},
		{"maximum plus one", math.MaxInt64, []int64{math.MaxInt64, 1}, true},
		{"two maximum values", math.MaxInt64, []int64{math.MaxInt64, math.MaxInt64}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := WouldExceed(test.limit, test.values...); got != test.want {
				t.Fatalf("WouldExceed(%d, %v) = %v, want %v", test.limit, test.values, got, test.want)
			}
		})
	}
}

func TestTimeColumnsPreserveWideEpochsAndFractionalSeconds(t *testing.T) {
	for _, test := range []struct {
		name string
		sec  int64
		nsec int32
	}{
		{"before epoch", -1, 999999999},
		{"epoch", 0, 0},
		{"beyond signed 32 bits", 2147483648, 1},
		{"beyond nanosecond counter", 13582641600, 500000000},
	} {
		t.Run(test.name, func(t *testing.T) {
			instant := time.Unix(test.sec, int64(test.nsec)).In(time.FixedZone("offset", 3600))
			sec, nsec := StoredTime(instant)
			if sec != test.sec || nsec != test.nsec {
				t.Fatalf("StoredTime(%v) = %d, %d; want %d, %d", instant, sec, nsec, test.sec, test.nsec)
			}
			if got := LoadedTime(sec, nsec); !got.Equal(instant) {
				t.Fatalf("LoadedTime(%d, %d) = %v, want instant %v", sec, nsec, got, instant)
			}
		})
	}
}

func TestStoredKeyUsesNullOnlyForAbsentContent(t *testing.T) {
	if got := StoredKey(""); got != nil {
		t.Fatalf("absent content stores %#v, want NULL", got)
	}
	key := metastore.Key("00112233445566778899aabbccddeeff")
	got, ok := StoredKey(key).(string)
	if !ok || got != string(key) {
		t.Fatalf("content key stores %q as string=%v, want %q", got, ok, key)
	}
}
