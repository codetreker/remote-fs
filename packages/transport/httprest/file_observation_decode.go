package httprest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/storage"
)

type encodedObservedPayload struct {
	version string
	data    string
}

type deferredObservedAttr struct {
	wire          Attr
	metadata      map[string]encodedObservedPayload
	metadataBytes int64
}

func canonicalBase64Length(encoded string, maximum int, nonempty bool) (int64, error) {
	if len(encoded)%4 != 0 {
		return 0, errors.New("bytes are not padded canonical base64")
	}
	padding := 0
	if strings.HasSuffix(encoded, "==") {
		padding = 2
	} else if strings.HasSuffix(encoded, "=") {
		padding = 1
	}
	if strings.Contains(encoded[:len(encoded)-padding], "=") {
		return 0, errors.New("bytes contain interior base64 padding")
	}
	decoded := int64(len(encoded)/4*3 - padding)
	if decoded > int64(maximum) {
		return 0, syscall.EFBIG
	}
	if nonempty && decoded == 0 {
		return 0, syscall.EINVAL
	}
	return decoded, nil
}

func decodeCanonicalBase64(encoded string, maximum int, nonempty bool) ([]byte, error) {
	if _, err := canonicalBase64Length(encoded, maximum, nonempty); err != nil {
		return nil, err
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New("bytes are not canonical base64")
	}
	return decoded, nil
}

func decodeObservedFileResponse(ctx context.Context, request fileRequest, data []byte) (fileResponse, error) {
	if !utf8.Valid(data) {
		return fileResponse{}, errors.New("file response JSON must be UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return fileResponse{}, errors.New("file response is not an object")
	}
	response := fileResponse{Data: []byte{}}
	seen := make(map[string]bool, 3)
	for decoder.More() {
		field, ok, err := decodeObservedField(decoder, seen)
		if err != nil {
			return fileResponse{}, err
		}
		if !ok {
			return fileResponse{}, errors.New("file response member is not text")
		}
		switch field {
		case "epoch":
			response.Epoch, err = decodeListingUint64(decoder)
		case "data":
			var value []byte
			value, err = decodeListingBytes(decoder, "data", 0, false)
			if len(value) != 0 {
				err = errors.New("observation response carries data")
			}
		case "directory":
			if request.Op != storage.OpFileReadDirNode && request.Op != storage.OpFileObserveDirectoryMetadata {
				return fileResponse{}, errors.New("operation does not return a directory")
			}
			response.Directory, err = decodeObservedDirectory(ctx, decoder, request)
		case "nameObservation":
			if request.Op != storage.OpFileObserveName {
				return fileResponse{}, errors.New("operation does not return a name observation")
			}
			response.NameObservation, err = decodeNameObservation(ctx, decoder, nil)
		default:
			return fileResponse{}, fmt.Errorf("file response carries unknown field %q", field)
		}
		if err != nil {
			return fileResponse{}, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return fileResponse{}, errors.New("file response object did not end")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fileResponse{}, errors.New("file response JSON contains trailing content")
	}
	if !seen["epoch"] || !seen["data"] {
		return fileResponse{}, errors.New("file response is incomplete")
	}
	return response, nil
}

func decodeObservedField(decoder *json.Decoder, seen map[string]bool) (string, bool, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", false, err
	}
	field, ok := token.(string)
	if !ok {
		return "", false, nil
	}
	if seen[field] {
		return "", false, fmt.Errorf("response repeats field %q", field)
	}
	seen[field] = true
	return field, true, nil
}

func decodeObservedDirectory(ctx context.Context, decoder *json.Decoder, request fileRequest) (*observedDirectory, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("observed directory is not an object")
	}
	members := make(map[string]json.RawMessage, 3)
	seen := make(map[string]bool, 3)
	for decoder.More() {
		field, ok, err := decodeObservedField(decoder, seen)
		if err != nil || !ok {
			return nil, errors.New("observed directory member is not text")
		}
		if field != "observation" && field != "name" && field != "entries" {
			return nil, fmt.Errorf("observed directory carries unknown field %q", field)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, err
		}
		members[field] = raw
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("observed directory object did not end")
	}
	observationRaw, hasObservation := members["observation"]
	entriesRaw, hasEntries := members["entries"]
	if !hasObservation || !hasEntries {
		return nil, errors.New("observed directory is incomplete")
	}
	result := &observedDirectory{}
	observationDecoder := json.NewDecoder(bytes.NewReader(observationRaw))
	observationDecoder.UseNumber()
	result.Observation, err = decodeDirectoryObservation(observationDecoder)
	if err != nil {
		return nil, err
	}
	if err := requireObservedDecoderEOF(observationDecoder); err != nil {
		return nil, err
	}
	collector := directoryResponseCollectorFrom(ctx)
	if collector == nil {
		internal, createErr := storage.NewListResult(storage.MaxDirectoryBytes, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
			return storage.ObservedEntryBytes(nameBytes, metadataBytes)
		})
		if createErr != nil {
			return nil, createErr
		}
		collector = &directoryResponseCollector{result: internal, retain: true, names: make(map[string]struct{}), ids: make(map[uint64]struct{})}
	}
	nameRaw, hasName := members["name"]
	if hasName {
		if request.Op != storage.OpFileObserveDirectoryMetadata || request.DirectoryMetadata == nil || !request.DirectoryMetadata.IncludeName {
			return nil, errors.New("directory response carries an unrequested name observation")
		}
		nameDecoder := json.NewDecoder(bytes.NewReader(nameRaw))
		nameDecoder.UseNumber()
		result.Name, err = decodeNameObservation(ctx, nameDecoder, collector.result)
		if err != nil {
			return nil, err
		}
		if err := requireObservedDecoderEOF(nameDecoder); err != nil {
			return nil, err
		}
	}
	if request.Op == storage.OpFileObserveDirectoryMetadata && request.DirectoryMetadata != nil && request.DirectoryMetadata.IncludeName && !hasName {
		return nil, errors.New("directory response omitted its requested name observation")
	}
	entriesDecoder := json.NewDecoder(bytes.NewReader(entriesRaw))
	entriesDecoder.UseNumber()
	token, err = entriesDecoder.Token()
	if err != nil || token != json.Delim('[') {
		return nil, errors.New("observed directory entries are not an array")
	}
	for index := 0; entriesDecoder.More(); index++ {
		if index >= storage.MaxDirectoryEntries {
			return nil, syscall.EFBIG
		}
		if err := decodeObservedEntry(entriesDecoder, result.Observation.ParentID, collector); err != nil {
			return nil, err
		}
	}
	if token, err = entriesDecoder.Token(); err != nil || token != json.Delim(']') {
		return nil, errors.New("observed directory entries did not end")
	}
	if err := requireObservedDecoderEOF(entriesDecoder); err != nil {
		return nil, err
	}
	if collector.retain {
		entries, entriesErr := collector.result.Entries()
		if entriesErr != nil {
			return nil, entriesErr
		}
		result.Entries = make([]observedEntry, 0, len(entries))
		for _, entry := range entries {
			result.Entries = append(result.Entries, observedEntry{RawLeaf: canonicalBytes([]byte(entry.Name)), Attr: AttrOf(entry.Attr)})
		}
	} else {
		result.Entries = []observedEntry{}
	}
	return result, nil
}

func requireObservedDecoderEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("observed response member contains trailing content")
	}
	return nil
}

func decodeDirectoryObservation(decoder *json.Decoder) (directoryObservation, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return directoryObservation{}, errors.New("directory observation is not an object")
	}
	var result directoryObservation
	seen := make(map[string]bool, 2)
	for decoder.More() {
		field, ok, err := decodeObservedField(decoder, seen)
		if err != nil || !ok {
			return directoryObservation{}, errors.New("directory observation member is not text")
		}
		switch field {
		case "parentId":
			result.ParentID, err = decodeListingUint64(decoder)
		case "revision":
			var encoded string
			encoded, err = decodeListingString(decoder)
			if err == nil {
				var decoded []byte
				decoded, err = decodeCanonicalBase64(encoded, storage.MaxObservationTokenBytes, true)
				result.Revision = metadataVersion(decoded)
			}
		default:
			return directoryObservation{}, fmt.Errorf("directory observation carries unknown field %q", field)
		}
		if err != nil {
			return directoryObservation{}, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !seen["parentId"] || !seen["revision"] {
		return directoryObservation{}, errors.New("directory observation is incomplete")
	}
	if err := result.storage().Check(); err != nil {
		return directoryObservation{}, err
	}
	return result, nil
}

func decodeNameObservation(ctx context.Context, decoder *json.Decoder, result *storage.ListResult) (*nameObservation, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("name observation is not an object")
	}
	value := &nameObservation{}
	seen := make(map[string]bool, 4)
	var encodedLeaf string
	for decoder.More() {
		field, ok, err := decodeObservedField(decoder, seen)
		if err != nil || !ok {
			return nil, errors.New("name observation member is not text")
		}
		switch field {
		case "nodeId":
			value.NodeID, err = decodeListingUint64(decoder)
		case "state":
			var state uint64
			state, err = decodeListingUint64(decoder)
			if state > 255 {
				err = errors.New("name observation state exceeds its wire range")
			} else {
				value.State = storage.NameBindingState(state)
			}
		case "parentId":
			value.ParentID, err = decodeListingUint64(decoder)
		case "rawLeaf":
			encodedLeaf, err = decodeListingString(decoder)
		default:
			return nil, fmt.Errorf("name observation carries unknown field %q", field)
		}
		if err != nil {
			return nil, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !seen["nodeId"] || !seen["state"] || !seen["parentId"] {
		return nil, errors.New("name observation is incomplete")
	}
	leafBytes := int64(0)
	if seen["rawLeaf"] {
		leafBytes, err = canonicalBase64Length(encodedLeaf, storage.MaxLeafBytes, false)
		if err != nil {
			return nil, err
		}
	}
	scalar := value.storage()
	charge, err := storage.CheckNameObservationBudget(ctx, scalar, leafBytes)
	if err != nil {
		return nil, err
	}
	if result != nil {
		if err := result.ReservePrefix(charge); err != nil {
			return nil, err
		}
	}
	if seen["rawLeaf"] {
		leaf, err := decodeCanonicalBase64(encodedLeaf, storage.MaxLeafBytes, false)
		if err != nil {
			return nil, err
		}
		wireLeaf := canonicalBytes(leaf)
		value.RawLeaf = &wireLeaf
	}
	if err := value.storage().Check(); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeObservedEntry(decoder *json.Decoder, parent uint64, collector *directoryResponseCollector) error {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("observed directory entry is not an object")
	}
	seen := make(map[string]bool, 2)
	var encodedLeaf string
	var attr deferredObservedAttr
	for decoder.More() {
		field, ok, err := decodeObservedField(decoder, seen)
		if err != nil || !ok {
			return errors.New("observed directory entry member is not text")
		}
		switch field {
		case "rawLeaf":
			encodedLeaf, err = decodeListingString(decoder)
		case "attr":
			attr, err = decodeDeferredObservedAttr(decoder)
		default:
			return fmt.Errorf("observed directory entry carries unknown field %q", field)
		}
		if err != nil {
			return err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !seen["rawLeaf"] || !seen["attr"] {
		return errors.New("observed directory entry is incomplete")
	}
	leafBytes, err := canonicalBase64Length(encodedLeaf, storage.MaxLeafBytes, true)
	if err != nil {
		return err
	}
	if attr.wire.ID == parent {
		return errors.New("directory entry substitutes its parent identity")
	}
	if _, exists := collector.ids[attr.wire.ID]; exists {
		return errors.New("directory response repeats a child identity")
	}
	rawBytes, err := storage.ObservedEntryBytes(leafBytes, attr.metadataBytes)
	if err != nil {
		return err
	}
	if rawBytes > storage.MaxDirectoryBytes-collector.rawUsed {
		return syscall.EFBIG
	}
	collector.rawUsed += rawBytes
	reservation, err := collector.result.Reserve(leafBytes, attr.metadataBytes, attr.wire.Storage())
	if err != nil {
		return err
	}
	leaf, err := decodeCanonicalBase64(encodedLeaf, storage.MaxLeafBytes, true)
	if err != nil {
		return err
	}
	name := string(leaf)
	if _, exists := collector.names[name]; exists {
		return errors.New("directory response repeats a raw name")
	}
	metadata, err := attr.decodeMetadata()
	if err != nil {
		return err
	}
	if err := reservation.Commit(name, metadata); err != nil {
		return err
	}
	collector.names[name] = struct{}{}
	collector.ids[attr.wire.ID] = struct{}{}
	return nil
}

func decodeDeferredObservedAttr(decoder *json.Decoder) (deferredObservedAttr, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return deferredObservedAttr{}, errors.New("observed directory attributes are not an object")
	}
	result := deferredObservedAttr{metadataBytes: 6}
	seen := make(map[string]bool, 8)
	for decoder.More() {
		field, ok, err := decodeObservedField(decoder, seen)
		if err != nil || !ok {
			return deferredObservedAttr{}, errors.New("observed directory attributes contain a duplicate member")
		}
		switch field {
		case "id":
			result.wire.ID, err = decodeListingUint64(decoder)
		case "kind":
			var kind uint64
			kind, err = decodeListingUint64(decoder)
			if kind > 255 {
				err = errors.New("observed directory node kind exceeds its wire range")
			} else {
				result.wire.Kind = storage.NodeKind(kind)
			}
		case "size":
			result.wire.Size, err = decodeListingInt64(decoder)
		case "access_time":
			result.wire.AccessTime, err = decodeListingTime(decoder)
		case "mod_time":
			result.wire.ModTime, err = decodeListingTime(decoder)
		case "birth_time":
			value, decodeErr := decodeListingTime(decoder)
			result.wire.BirthTime, err = &value, decodeErr
		case "change_time":
			value, decodeErr := decodeListingTime(decoder)
			result.wire.ChangeTime, err = &value, decodeErr
		case "metadata":
			result.metadata, result.metadataBytes, err = decodeDeferredObservedMetadata(decoder)
		default:
			return deferredObservedAttr{}, fmt.Errorf("observed directory attributes carry unknown field %q", field)
		}
		if err != nil {
			return deferredObservedAttr{}, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return deferredObservedAttr{}, errors.New("observed directory attributes object did not end")
	}
	for _, required := range []string{"id", "kind", "size", "access_time", "mod_time"} {
		if !seen[required] {
			return deferredObservedAttr{}, fmt.Errorf("observed directory attributes carry no %s", required)
		}
	}
	if err := result.wire.check(); err != nil {
		return deferredObservedAttr{}, err
	}
	return result, nil
}

func decodeDeferredObservedMetadata(decoder *json.Decoder) (map[string]encodedObservedPayload, int64, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, 0, errors.New("observed directory metadata is not an object")
	}
	result := make(map[string]encodedObservedPayload)
	encodedBytes := int64(6)
	for decoder.More() {
		name, err := decodeListingString(decoder)
		if err != nil {
			return nil, 0, errors.New("observed directory metadata namespace is not text")
		}
		if _, exists := result[name]; exists {
			return nil, 0, errors.New("observed directory metadata repeats a namespace")
		}
		if len(result) >= storage.MaxMetadataNamespaces {
			return nil, 0, syscall.EFBIG
		}
		if err := storage.CheckMetadataNamespace(name); err != nil {
			return nil, 0, err
		}
		payload, versionBytes, dataBytes, err := decodeDeferredObservedPayload(decoder)
		if err != nil {
			return nil, 0, err
		}
		encodedBytes += 8 + int64(len(name)) + versionBytes + dataBytes
		if encodedBytes > storage.MaxMetadataBytes {
			return nil, 0, syscall.EFBIG
		}
		result[name] = payload
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, 0, errors.New("observed directory metadata object did not end")
	}
	if len(result) == 0 {
		return nil, 6, nil
	}
	return result, encodedBytes, nil
}

func decodeDeferredObservedPayload(decoder *json.Decoder) (encodedObservedPayload, int64, int64, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return encodedObservedPayload{}, 0, 0, errors.New("observed directory metadata payload is not an object")
	}
	var result encodedObservedPayload
	seen := make(map[string]bool, 2)
	for decoder.More() {
		field, ok, err := decodeObservedField(decoder, seen)
		if err != nil || !ok {
			return encodedObservedPayload{}, 0, 0, errors.New("observed directory metadata payload repeats a member")
		}
		switch field {
		case "version":
			result.version, err = decodeListingString(decoder)
		case "data":
			result.data, err = decodeListingString(decoder)
		default:
			return encodedObservedPayload{}, 0, 0, fmt.Errorf("observed directory metadata payload carries unknown field %q", field)
		}
		if err != nil {
			return encodedObservedPayload{}, 0, 0, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !seen["version"] || !seen["data"] {
		return encodedObservedPayload{}, 0, 0, errors.New("observed directory metadata payload is incomplete")
	}
	versionBytes, err := canonicalBase64Length(result.version, storage.MaxObservationTokenBytes, true)
	if err != nil {
		return encodedObservedPayload{}, 0, 0, err
	}
	dataBytes, err := canonicalBase64Length(result.data, storage.MaxMetadataValueBytes, false)
	if err != nil {
		return encodedObservedPayload{}, 0, 0, err
	}
	return result, versionBytes, dataBytes, nil
}

func (attr deferredObservedAttr) decodeMetadata() (map[string]storage.OpaquePayload, error) {
	if len(attr.metadata) == 0 {
		return nil, nil
	}
	result := make(map[string]storage.OpaquePayload, len(attr.metadata))
	for name, encoded := range attr.metadata {
		version, err := decodeCanonicalBase64(encoded.version, storage.MaxObservationTokenBytes, true)
		if err != nil {
			return nil, err
		}
		data, err := decodeCanonicalBase64(encoded.data, storage.MaxMetadataValueBytes, false)
		if err != nil {
			return nil, err
		}
		result[name] = storage.OpaquePayload{Version: version, Data: data}
	}
	if err := storage.CheckMetadata(result); err != nil {
		return nil, err
	}
	return result, nil
}
