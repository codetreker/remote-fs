package httprest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"syscall"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/storage"
)

type encodedObservedPayload struct {
	version []byte
	data    []byte
}

type deferredObservedAttr struct {
	wire          Attr
	metadata      map[string]encodedObservedPayload
	metadataBytes int64
}

type observationBudgetError struct{ cause error }

func (failure *observationBudgetError) Error() string { return failure.cause.Error() }
func (failure *observationBudgetError) Unwrap() error { return failure.cause }

func budgetObservationError(err error) error {
	if err == nil {
		return nil
	}
	return &observationBudgetError{cause: err}
}

func base64AlphabetValue(value byte) (byte, bool) {
	switch {
	case value >= 'A' && value <= 'Z':
		return value - 'A', true
	case value >= 'a' && value <= 'z':
		return value - 'a' + 26, true
	case value >= '0' && value <= '9':
		return value - '0' + 52, true
	case value == '+':
		return 62, true
	case value == '/':
		return 63, true
	default:
		return 0, false
	}
}

func validateCanonicalBase64(encoded []byte, maximum int, nonempty bool) (int64, error) {
	if len(encoded)%4 != 0 || len(encoded) > base64.StdEncoding.EncodedLen(maximum) {
		return 0, errors.New("bytes are outside their canonical base64 bound")
	}
	padding := 0
	if len(encoded) != 0 && encoded[len(encoded)-1] == '=' {
		padding++
		if len(encoded) > 1 && encoded[len(encoded)-2] == '=' {
			padding++
		}
	}
	dataEnd := len(encoded) - padding
	for index, value := range encoded {
		if index >= dataEnd {
			if value != '=' {
				return 0, errors.New("bytes have invalid base64 padding")
			}
			continue
		}
		if _, ok := base64AlphabetValue(value); !ok {
			return 0, errors.New("bytes are not base64 text")
		}
	}
	if padding == 2 {
		value, _ := base64AlphabetValue(encoded[dataEnd-1])
		if value&0x0f != 0 {
			return 0, errors.New("bytes contain non-canonical base64 padding bits")
		}
	} else if padding == 1 {
		value, _ := base64AlphabetValue(encoded[dataEnd-1])
		if value&0x03 != 0 {
			return 0, errors.New("bytes contain non-canonical base64 padding bits")
		}
	}
	decoded := int64(len(encoded)/4*3 - padding)
	if decoded > int64(maximum) {
		return 0, errors.New("bytes are outside their canonical base64 bound")
	}
	if nonempty && decoded == 0 {
		return 0, syscall.EINVAL
	}
	return decoded, nil
}

func decodeCanonicalBase64Bytes(encoded []byte, maximum int, nonempty bool) ([]byte, error) {
	decodedBytes, err := validateCanonicalBase64(encoded, maximum, nonempty)
	if err != nil {
		return nil, err
	}
	decoded := make([]byte, int(decodedBytes))
	if _, err := base64.StdEncoding.Strict().Decode(decoded, encoded); err != nil {
		return nil, errors.New("bytes are not canonical base64")
	}
	return decoded, nil
}

func checkEncodedLeaf(encoded []byte, decodedBytes int) error {
	var leaf [storage.MaxLeafBytes]byte
	written, err := base64.StdEncoding.Strict().Decode(leaf[:decodedBytes], encoded)
	if err != nil || written != decodedBytes {
		return errors.New("raw leaf is not canonical base64")
	}
	return storage.CheckLeaf(leaf[:written])
}

func decodeObservedFileResponse(ctx context.Context, request fileRequest, data []byte) (fileResponse, error) {
	if !utf8.Valid(data) {
		return fileResponse{}, errors.New("file response JSON must be UTF-8")
	}
	allowed := map[string]struct{}{"epoch": {}, "data": {}, "directory": {}, "nameObservation": {}}
	members, err := splitJSONObject(data, 3, 64, allowed)
	if err != nil {
		return fileResponse{}, err
	}
	response := fileResponse{Data: []byte{}}
	epoch, hasEpoch := members["epoch"]
	dataField, hasData := members["data"]
	if !hasEpoch || !hasData {
		return fileResponse{}, errors.New("file response is incomplete")
	}
	response.Epoch, err = parseJSONUint64(epoch)
	if err != nil {
		return fileResponse{}, errors.New("file response epoch is invalid")
	}
	encodedData, err := rawJSONString(dataField, 0, "file response data")
	if err != nil || len(encodedData) != 0 {
		return fileResponse{}, errors.New("observation response carries data")
	}
	if directory, ok := members["directory"]; ok {
		if request.Op != storage.OpFileReadDirNode && request.Op != storage.OpFileObserveDirectoryMetadata {
			return fileResponse{}, errors.New("operation does not return a directory")
		}
		response.Directory, err = decodeObservedDirectoryBytes(ctx, directory, request)
		if err != nil {
			return fileResponse{}, err
		}
	}
	if name, ok := members["nameObservation"]; ok {
		if request.Op != storage.OpFileObserveName {
			return fileResponse{}, errors.New("operation does not return a name observation")
		}
		response.NameObservation, err = decodeNameObservationBytes(ctx, name, nil)
		if err != nil {
			return fileResponse{}, err
		}
	}
	return response, nil
}

func decodeObservedDirectoryBytes(ctx context.Context, data []byte, request fileRequest) (*observedDirectory, error) {
	allowed := map[string]struct{}{"observation": {}, "name": {}, "entries": {}}
	members, err := splitJSONObject(data, 3, 64, allowed)
	if err != nil {
		return nil, err
	}
	observationRaw, hasObservation := members["observation"]
	entriesRaw, hasEntries := members["entries"]
	if !hasObservation || !hasEntries {
		return nil, errors.New("observed directory is incomplete")
	}
	observation, err := decodeDirectoryObservationBytes(observationRaw)
	if err != nil {
		return nil, err
	}
	result := &observedDirectory{Observation: observation}
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
		result.Name, err = decodeNameObservationBytes(ctx, nameRaw, collector.result)
		if err != nil {
			return nil, err
		}
	}
	if request.Op == storage.OpFileObserveDirectoryMetadata && request.DirectoryMetadata != nil && request.DirectoryMetadata.IncludeName && !hasName {
		return nil, errors.New("directory response omitted its requested name observation")
	}
	entryCount := 0
	if err := forEachJSONArrayValue(entriesRaw, func(entry []byte) error {
		if entryCount >= storage.MaxDirectoryEntries {
			return syscall.EFBIG
		}
		entryCount++
		return decodeObservedEntryBytes(entry, result.Observation.ParentID, collector)
	}); err != nil {
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

func decodeDirectoryObservationBytes(data []byte) (directoryObservation, error) {
	allowed := map[string]struct{}{"parentId": {}, "revision": {}}
	members, err := splitJSONObject(data, 2, 64, allowed)
	if err != nil {
		return directoryObservation{}, err
	}
	parent, hasParent := members["parentId"]
	revision, hasRevision := members["revision"]
	if !hasParent || !hasRevision {
		return directoryObservation{}, errors.New("directory observation is incomplete")
	}
	parentID, err := parseJSONUint64(parent)
	if err != nil {
		return directoryObservation{}, errors.New("directory observation parent is invalid")
	}
	encoded, err := rawJSONString(revision, base64.StdEncoding.EncodedLen(storage.MaxObservationTokenBytes), "directory revision")
	if err != nil {
		return directoryObservation{}, err
	}
	decoded, err := decodeCanonicalBase64Bytes(encoded, storage.MaxObservationTokenBytes, true)
	if err != nil {
		return directoryObservation{}, err
	}
	result := directoryObservation{ParentID: parentID, Revision: metadataVersion(decoded)}
	if err := result.storage().Check(); err != nil {
		return directoryObservation{}, err
	}
	return result, nil
}

func decodeNameObservationBytes(ctx context.Context, data []byte, result *storage.ListResult) (*nameObservation, error) {
	allowed := map[string]struct{}{"nodeId": {}, "state": {}, "parentId": {}, "rawLeaf": {}}
	members, err := splitJSONObject(data, 4, 64, allowed)
	if err != nil {
		return nil, err
	}
	node, hasNode := members["nodeId"]
	state, hasState := members["state"]
	parent, hasParent := members["parentId"]
	if !hasNode || !hasState || !hasParent {
		return nil, errors.New("name observation is incomplete")
	}
	nodeID, err := parseJSONUint64(node)
	if err != nil {
		return nil, errors.New("name observation node is invalid")
	}
	stateValue, err := parseJSONUint64(state)
	if err != nil || stateValue > 255 {
		return nil, errors.New("name observation state is invalid")
	}
	parentID, err := parseJSONUint64(parent)
	if err != nil {
		return nil, errors.New("name observation parent is invalid")
	}
	value := &nameObservation{NodeID: nodeID, State: storage.NameBindingState(stateValue), ParentID: parentID}
	leafRaw, hasLeaf := members["rawLeaf"]
	var encodedLeaf []byte
	leafBytes := int64(0)
	if hasLeaf {
		encodedLeaf, err = rawJSONString(leafRaw, base64.StdEncoding.EncodedLen(storage.MaxLeafBytes), "name observation raw leaf")
		if err != nil {
			return nil, err
		}
		leafBytes, err = validateCanonicalBase64(encodedLeaf, storage.MaxLeafBytes, false)
		if err != nil {
			return nil, err
		}
		if err := checkEncodedLeaf(encodedLeaf, int(leafBytes)); err != nil {
			return nil, err
		}
	}
	scalar := value.storage()
	if err := checkNameObservationScalar(scalar, leafBytes, hasLeaf); err != nil {
		return nil, err
	}
	charge, err := storage.CheckNameObservationBudget(ctx, scalar, leafBytes)
	if err != nil {
		return nil, budgetObservationError(err)
	}
	if result != nil {
		if err := result.ReservePrefix(charge); err != nil {
			return nil, budgetObservationError(err)
		}
	}
	if hasLeaf {
		leaf, err := decodeCanonicalBase64Bytes(encodedLeaf, storage.MaxLeafBytes, false)
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

func checkNameObservationScalar(value storage.NameObservation, leafBytes int64, leafPresent bool) error {
	if value.NodeID == 0 {
		return errors.New("name observation carries no node identity")
	}
	switch value.State {
	case storage.NameRoot, storage.NameDetached:
		if value.ParentID != 0 || leafBytes != 0 || leafPresent {
			return errors.New("unbound name observation carries a parent or raw leaf")
		}
	case storage.NameLinked:
		if value.ParentID == 0 || value.ParentID == value.NodeID || leafBytes == 0 || !leafPresent {
			return errors.New("linked name observation carries an invalid parent or raw leaf")
		}
	default:
		return errors.New("name observation carries an unknown binding state")
	}
	return nil
}

func decodeObservedEntryBytes(data []byte, parent uint64, collector *directoryResponseCollector) error {
	allowed := map[string]struct{}{"rawLeaf": {}, "attr": {}}
	members, err := splitJSONObject(data, 2, 64, allowed)
	if err != nil {
		return err
	}
	rawLeaf, hasLeaf := members["rawLeaf"]
	attrRaw, hasAttr := members["attr"]
	if !hasLeaf || !hasAttr {
		return errors.New("observed directory entry is incomplete")
	}
	encodedLeaf, err := rawJSONString(rawLeaf, base64.StdEncoding.EncodedLen(storage.MaxLeafBytes), "observed directory raw leaf")
	if err != nil {
		return err
	}
	leafBytes, err := validateCanonicalBase64(encodedLeaf, storage.MaxLeafBytes, true)
	if err != nil {
		return err
	}
	if err := checkEncodedLeaf(encodedLeaf, int(leafBytes)); err != nil {
		return err
	}
	attr, err := decodeDeferredObservedAttrBytes(attrRaw)
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
		return budgetObservationError(err)
	}
	leaf, err := decodeCanonicalBase64Bytes(encodedLeaf, storage.MaxLeafBytes, true)
	if err != nil {
		return err
	}
	name := string(leaf)
	if _, exists := collector.names[name]; exists {
		return errors.New("directory response repeats a raw name")
	}
	metadata, err := attr.decodeMetadataBytes()
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

func decodeDeferredObservedAttrBytes(data []byte) (deferredObservedAttr, error) {
	allowed := map[string]struct{}{
		"id": {}, "kind": {}, "size": {}, "access_time": {}, "mod_time": {},
		"birth_time": {}, "change_time": {}, "metadata": {},
	}
	members, err := splitJSONObject(data, 8, 64, allowed)
	if err != nil {
		return deferredObservedAttr{}, err
	}
	for _, required := range []string{"id", "kind", "size", "access_time", "mod_time"} {
		if _, ok := members[required]; !ok {
			return deferredObservedAttr{}, fmt.Errorf("observed directory attributes carry no %s", required)
		}
	}
	result := deferredObservedAttr{metadataBytes: 6}
	result.wire.ID, err = parseJSONUint64(members["id"])
	if err != nil {
		return deferredObservedAttr{}, errors.New("observed directory identity is invalid")
	}
	kind, err := parseJSONUint64(members["kind"])
	if err != nil || kind > 255 {
		return deferredObservedAttr{}, errors.New("observed directory node kind is invalid")
	}
	result.wire.Kind = storage.NodeKind(kind)
	result.wire.Size, err = parseJSONInt64(members["size"])
	if err != nil {
		return deferredObservedAttr{}, errors.New("observed directory size is invalid")
	}
	if err := decodeFileJSON(members["access_time"], &result.wire.AccessTime); err != nil {
		return deferredObservedAttr{}, err
	}
	if err := decodeFileJSON(members["mod_time"], &result.wire.ModTime); err != nil {
		return deferredObservedAttr{}, err
	}
	if value, ok := members["birth_time"]; ok {
		var decoded Time
		if err := decodeFileJSON(value, &decoded); err != nil {
			return deferredObservedAttr{}, err
		}
		result.wire.BirthTime = &decoded
	}
	if value, ok := members["change_time"]; ok {
		var decoded Time
		if err := decodeFileJSON(value, &decoded); err != nil {
			return deferredObservedAttr{}, err
		}
		result.wire.ChangeTime = &decoded
	}
	if value, ok := members["metadata"]; ok {
		result.metadata, result.metadataBytes, err = decodeDeferredObservedMetadataBytes(value)
		if err != nil {
			return deferredObservedAttr{}, err
		}
	}
	if err := result.wire.check(); err != nil {
		return deferredObservedAttr{}, err
	}
	return result, nil
}

func decodeDeferredObservedMetadataBytes(data []byte) (map[string]encodedObservedPayload, int64, error) {
	members, err := splitJSONObject(data, storage.MaxMetadataNamespaces, 6*storage.MaxMetadataNamespaceBytes+2, nil)
	if err != nil {
		return nil, 0, err
	}
	encodedBytes := int64(6)
	result := make(map[string]encodedObservedPayload, len(members))
	for name, raw := range members {
		if err := storage.CheckMetadataNamespace(name); err != nil {
			return nil, 0, err
		}
		payload, versionBytes, dataBytes, err := decodeDeferredObservedPayloadBytes(raw)
		if err != nil {
			return nil, 0, err
		}
		encodedBytes += 8 + int64(len(name)) + versionBytes + dataBytes
		if encodedBytes > storage.MaxMetadataBytes {
			return nil, 0, syscall.EFBIG
		}
		result[name] = payload
	}
	if len(result) == 0 {
		return nil, 6, nil
	}
	return result, encodedBytes, nil
}

func decodeDeferredObservedPayloadBytes(data []byte) (encodedObservedPayload, int64, int64, error) {
	allowed := map[string]struct{}{"version": {}, "data": {}}
	members, err := splitJSONObject(data, 2, 64, allowed)
	if err != nil {
		return encodedObservedPayload{}, 0, 0, err
	}
	versionRaw, hasVersion := members["version"]
	dataRaw, hasData := members["data"]
	if !hasVersion || !hasData {
		return encodedObservedPayload{}, 0, 0, errors.New("observed directory metadata payload is incomplete")
	}
	version, err := rawJSONString(versionRaw, base64.StdEncoding.EncodedLen(storage.MaxObservationTokenBytes), "metadata version")
	if err != nil {
		return encodedObservedPayload{}, 0, 0, err
	}
	versionBytes, err := validateCanonicalBase64(version, storage.MaxObservationTokenBytes, true)
	if err != nil {
		return encodedObservedPayload{}, 0, 0, err
	}
	payload, err := rawJSONString(dataRaw, base64.StdEncoding.EncodedLen(storage.MaxMetadataValueBytes), "metadata payload")
	if err != nil {
		return encodedObservedPayload{}, 0, 0, err
	}
	dataBytes, err := validateCanonicalBase64(payload, storage.MaxMetadataValueBytes, false)
	if err != nil {
		return encodedObservedPayload{}, 0, 0, err
	}
	return encodedObservedPayload{version: version, data: payload}, versionBytes, dataBytes, nil
}

func (attr deferredObservedAttr) decodeMetadataBytes() (map[string]storage.OpaquePayload, error) {
	if len(attr.metadata) == 0 {
		return nil, nil
	}
	result := make(map[string]storage.OpaquePayload, len(attr.metadata))
	for name, encoded := range attr.metadata {
		version, err := decodeCanonicalBase64Bytes(encoded.version, storage.MaxObservationTokenBytes, true)
		if err != nil {
			return nil, err
		}
		data, err := decodeCanonicalBase64Bytes(encoded.data, storage.MaxMetadataValueBytes, false)
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

func splitJSONObject(data []byte, maximumMembers, maximumKeyBytes int, allowed map[string]struct{}) (map[string][]byte, error) {
	if !json.Valid(data) {
		return nil, errors.New("JSON object is malformed")
	}
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return nil, errors.New("JSON value is not an object")
	}
	i++
	result := make(map[string][]byte)
	for {
		i = skipJSONSpace(data, i)
		if i < len(data) && data[i] == '}' {
			i++
			break
		}
		keyEnd, err := scanJSONStringEnd(data, i)
		if err != nil {
			return nil, err
		}
		if keyEnd-i > maximumKeyBytes {
			return nil, errors.New("JSON object member name exceeds its bound")
		}
		var key string
		if err := json.Unmarshal(data[i:keyEnd], &key); err != nil {
			return nil, err
		}
		if allowed != nil {
			if _, ok := allowed[key]; !ok {
				return nil, fmt.Errorf("JSON object carries unknown field %q", key)
			}
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("JSON object repeats field %q", key)
		}
		if len(result) >= maximumMembers {
			return nil, errors.New("JSON object carries too many members")
		}
		i = skipJSONSpace(data, keyEnd)
		if i >= len(data) || data[i] != ':' {
			return nil, errors.New("JSON object member has no value")
		}
		i = skipJSONSpace(data, i+1)
		valueEnd, err := scanJSONValueEnd(data, i)
		if err != nil {
			return nil, err
		}
		result[key] = data[i:valueEnd]
		i = skipJSONSpace(data, valueEnd)
		if i >= len(data) {
			return nil, errors.New("JSON object did not end")
		}
		if data[i] == '}' {
			i++
			break
		}
		if data[i] != ',' {
			return nil, errors.New("JSON object members are not separated")
		}
		i++
	}
	if skipJSONSpace(data, i) != len(data) {
		return nil, errors.New("JSON object contains trailing content")
	}
	return result, nil
}

func forEachJSONArrayValue(data []byte, add func([]byte) error) error {
	if !json.Valid(data) {
		return errors.New("JSON array is malformed")
	}
	i := skipJSONSpace(data, 0)
	if i >= len(data) || data[i] != '[' {
		return errors.New("JSON value is not an array")
	}
	i++
	for {
		i = skipJSONSpace(data, i)
		if i < len(data) && data[i] == ']' {
			i++
			break
		}
		end, err := scanJSONValueEnd(data, i)
		if err != nil {
			return err
		}
		if err := add(data[i:end]); err != nil {
			return err
		}
		i = skipJSONSpace(data, end)
		if i >= len(data) {
			return errors.New("JSON array did not end")
		}
		if data[i] == ']' {
			i++
			break
		}
		if data[i] != ',' {
			return errors.New("JSON array values are not separated")
		}
		i++
	}
	if skipJSONSpace(data, i) != len(data) {
		return errors.New("JSON array contains trailing content")
	}
	return nil
}

func skipJSONSpace(data []byte, index int) int {
	for index < len(data) {
		switch data[index] {
		case ' ', '\t', '\r', '\n':
			index++
		default:
			return index
		}
	}
	return index
}

func scanJSONStringEnd(data []byte, start int) (int, error) {
	if start >= len(data) || data[start] != '"' {
		return 0, errors.New("JSON object member name is not a string")
	}
	for index := start + 1; index < len(data); index++ {
		switch data[index] {
		case '\\':
			index++
		case '"':
			return index + 1, nil
		}
	}
	return 0, errors.New("JSON string did not end")
}

func scanJSONValueEnd(data []byte, start int) (int, error) {
	if start >= len(data) {
		return 0, errors.New("JSON value is absent")
	}
	if data[start] == '"' {
		return scanJSONStringEnd(data, start)
	}
	if data[start] != '{' && data[start] != '[' {
		index := start
		for index < len(data) && data[index] != ',' && data[index] != '}' && data[index] != ']' && data[index] != ' ' && data[index] != '\t' && data[index] != '\r' && data[index] != '\n' {
			index++
		}
		return index, nil
	}
	stack := []byte{data[start]}
	inString := false
	for index := start + 1; index < len(data); index++ {
		value := data[index]
		if inString {
			if value == '\\' {
				index++
			} else if value == '"' {
				inString = false
			}
			continue
		}
		switch value {
		case '"':
			inString = true
		case '{', '[':
			stack = append(stack, value)
		case '}', ']':
			open := stack[len(stack)-1]
			if open == '{' && value != '}' || open == '[' && value != ']' {
				return 0, errors.New("JSON value has mismatched delimiters")
			}
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				return index + 1, nil
			}
		}
	}
	return 0, errors.New("JSON value did not end")
}

func parseJSONUint64(data []byte) (uint64, error) {
	if len(data) == 0 || len(data) > 20 {
		return 0, errors.New("unsigned JSON integer exceeds its lexical bound")
	}
	return strconv.ParseUint(string(data), 10, 64)
}

func parseJSONInt64(data []byte) (int64, error) {
	if len(data) == 0 || len(data) > 20 {
		return 0, errors.New("signed JSON integer exceeds its lexical bound")
	}
	return strconv.ParseInt(string(data), 10, 64)
}

func rawJSONString(data []byte, maximumEncodedBytes int, field string) ([]byte, error) {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return nil, fmt.Errorf("%s is not a JSON string", field)
	}
	value := data[1 : len(data)-1]
	if len(value) > maximumEncodedBytes {
		return nil, fmt.Errorf("%s exceeds its encoded bound: %w", field, syscall.EFBIG)
	}
	if bytes.IndexByte(value, '\\') >= 0 {
		return nil, fmt.Errorf("%s must not use JSON escapes", field)
	}
	return value, nil
}
