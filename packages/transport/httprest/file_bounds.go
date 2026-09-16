package httprest

import (
	"encoding/base64"
	"github.com/codetreker/remote-fs/packages/storage"
)

const fileDiagnosticBytes = 1024

// Each ancestor contains five uint64 scalars, their field names, and a base64
// name. The fixed allowance also includes the maximum JSON integer widths.
const fileEntryScalarBytes = 256
const fileObservationScalarBytes = 4096
const fileReceiptScalarBytes = 8192

func fileControlRequestLimit() int64 { return 4096 }
func fileControlResponseLimit() int64 {
	return fileReceiptScalarBytes + fileObservationScalarBytes + storage.MaxLocationDepth*fileEntryScalarBytes +
		int64(base64.StdEncoding.EncodedLen(storage.MaxLocationNameBytes)) + int64(base64.StdEncoding.EncodedLen(storage.MaxMetadataBytes)) +
		int64(base64.StdEncoding.EncodedLen(storage.MaxLinkTargetBytes)) + 6*fileDiagnosticBytes + 6*MaxLockCapabilityBytes + maxMutationBarrierJSONBytes
}
func fileReadLimit(limit int64) int64 {
	fixed := int64(fileObservationScalarBytes + base64.StdEncoding.EncodedLen(storage.MaxMetadataBytes))
	if limit <= fixed {
		return 0
	}
	return (limit - fixed) / 4 * 3
}

func fileRangeSnapshotResponseLimit() int64 {
	const envelope = int64(len(`{"ranges":{"Revision":18446744073709551615,"Own":[],"Other":[],"Available":9223372036854775807,"OwnerAvailable":9223372036854775807}}`))
	// Other entries include every acquisition field plus its bounded session
	// identity. Charging all entries at that width also covers the Own array.
	const entry = int64(len(`{"Owner":{"Session":"","ID":18446744073709551615},"Range":{"Boundary":false,"ID":18446744073709551615,"Start":18446744073709551615,"End":18446744073709551615,"Exclusive":false}}`)) + 6*storage.MaxFileSessionIDBytes + 1
	return envelope + storage.MaxRangeSnapshotRanges*entry
}
func fileDirectoryResponseLimit(r storage.DirectoryPageRequest) int64 {
	// The native page charges names and four times canonical metadata bytes.
	// Twice that budget covers base64 payloads; scalar and cursor space is separate.
	return 4096 + fileObservationScalarBytes*int64(r.MaxEntries) + 2*int64(r.MaxBytes) + 2*storage.MaxEntryNameBytes
}
