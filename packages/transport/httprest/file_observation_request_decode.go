package httprest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func checkObservationRequestBounds(data []byte) error {
	members, err := splitJSONObject(data, 64, 64, nil)
	if err != nil {
		return err
	}
	if guards, ok := members["guards"]; ok {
		if err := checkNamespaceGuardsRequestBounds(guards); err != nil {
			return err
		}
	}
	if options, ok := members["directoryMetadata"]; ok {
		if err := checkDirectoryMetadataRequestBounds(options); err != nil {
			return err
		}
	}
	return nil
}

func checkDirectoryMetadataRequestBounds(data []byte) error {
	allowed := map[string]struct{}{"guards": {}, "includeName": {}}
	members, err := splitJSONObject(data, 2, 64, allowed)
	if err != nil {
		return err
	}
	if guards, ok := members["guards"]; ok {
		return checkNamespaceGuardsRequestBounds(guards)
	}
	return nil
}

func checkNamespaceGuardsRequestBounds(data []byte) error {
	allowed := map[string]struct{}{"directories": {}, "edges": {}, "rootId": {}}
	members, err := splitJSONObject(data, 3, 64, allowed)
	if err != nil {
		return err
	}
	var used int64
	if directories, ok := members["directories"]; ok {
		if err := checkDirectoryGuardRequestBounds(directories, &used); err != nil {
			return err
		}
	}
	if edges, ok := members["edges"]; ok {
		if err := checkEdgeGuardRequestBounds(edges, &used); err != nil {
			return err
		}
	}
	return nil
}

func checkDirectoryGuardRequestBounds(data []byte, used *int64) error {
	allowed := map[string]struct{}{"parentId": {}, "revision": {}}
	count := 0
	return forEachJSONArrayValue(data, func(value []byte) error {
		if count >= storage.MaxNamespaceGuards {
			return fmt.Errorf("directory guards exceed their count bound: %w", syscall.EFBIG)
		}
		count++
		if err := addNamespaceGuardRequestBytes(used, 8); err != nil {
			return err
		}
		members, err := splitJSONObject(value, 2, 64, allowed)
		if err != nil {
			return err
		}
		if revision, ok := members["revision"]; ok {
			decoded, err := boundedCanonicalRequestBytes(revision, storage.MaxObservationTokenBytes, "directory guard revision")
			if err != nil {
				return err
			}
			return addNamespaceGuardRequestBytes(used, decoded)
		}
		return nil
	})
}

func checkEdgeGuardRequestBounds(data []byte, used *int64) error {
	allowed := map[string]struct{}{"parentId": {}, "rawLeaf": {}, "childId": {}}
	count := 0
	return forEachJSONArrayValue(data, func(value []byte) error {
		if count >= storage.MaxNamespaceGuards {
			return fmt.Errorf("edge guards exceed their count bound: %w", syscall.EFBIG)
		}
		count++
		if err := addNamespaceGuardRequestBytes(used, 16); err != nil {
			return err
		}
		members, err := splitJSONObject(value, 3, 64, allowed)
		if err != nil {
			return err
		}
		if leaf, ok := members["rawLeaf"]; ok {
			decoded, err := boundedCanonicalRequestBytes(leaf, storage.MaxLeafBytes, "edge guard raw leaf")
			if err != nil {
				return err
			}
			return addNamespaceGuardRequestBytes(used, decoded)
		}
		return nil
	})
}

func boundedCanonicalRequestBytes(data []byte, maximum int, field string) (int64, error) {
	maximumEncoded := base64.StdEncoding.EncodedLen(maximum)
	if len(data) > maximumEncoded*6+2 {
		return 0, fmt.Errorf("%s exceeds its encoded bound: %w", field, syscall.EFBIG)
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return 0, fmt.Errorf("%s is not a JSON string", field)
	}
	decoded, err := validateCanonicalBase64([]byte(encoded), maximum, false)
	if err != nil {
		return 0, fmt.Errorf("%s is invalid: %w", field, err)
	}
	return decoded, nil
}

func addNamespaceGuardRequestBytes(used *int64, amount int64) error {
	if amount < 0 || amount > storage.MaxNamespaceGuardBytes-*used {
		return fmt.Errorf("namespace guards exceed their byte bound: %w", syscall.EFBIG)
	}
	*used += amount
	return nil
}
