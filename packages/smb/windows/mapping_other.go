//go:build !windows

package windows

func newMappingSystem() (mappingSystem, error) { return nil, ErrUnsupported }
