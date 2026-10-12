package service

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const maxConfigurationBytes = 64 << 10

// LoadConfig reads one bounded YAML document. Omitted policy fields select the
// published defaults; explicit invalid values, duplicate keys and aliases fail.
func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open service configuration: %w", err)
	}
	config, parseErr := ParseConfig(file)
	return config, errors.Join(parseErr, file.Close())
}

func ParseConfig(reader io.Reader) (Config, error) {
	encoded, err := io.ReadAll(io.LimitReader(reader, maxConfigurationBytes+1))
	if err != nil {
		return Config{}, fmt.Errorf("read service configuration: %w", err)
	}
	if len(encoded) > maxConfigurationBytes {
		return Config{}, errors.New("service configuration exceeds 64 KiB")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return Config{}, fmt.Errorf("decode service configuration: %w", err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			err = errors.New("only one YAML document is allowed")
		}
		return Config{}, fmt.Errorf("service configuration: %w", err)
	}
	if err := validateConfigurationNode(&document, 0); err != nil {
		return Config{}, err
	}
	fields, err := configurationMapping(document.Content[0])
	if err != nil {
		return Config{}, err
	}
	var stateRoot, origin string
	for name, target := range map[string]*string{"state_root": &stateRoot, "external_origin": &origin} {
		node, ok := fields[name]
		if !ok || node.Kind != yaml.ScalarNode || node.Tag != "!!str" || node.Value == "" {
			return Config{}, fmt.Errorf("configuration %s requires a nonempty string", name)
		}
		*target = node.Value
		delete(fields, name)
	}
	config := DefaultConfig(stateRoot, origin)
	for name, node := range fields {
		switch name {
		case "listen":
			err = configurationDecode(name, node, &config.Listen)
		case "trusted_proxy_cidrs":
			err = configurationDecode(name, node, &config.TrustedProxyCIDRs)
		case "shutdown_timeout":
			err = configurationDuration(name, node, &config.ShutdownTimeout)
		default:
			err = fmt.Errorf("unknown service configuration field %q", name)
		}
		if err != nil {
			return Config{}, err
		}
	}
	if err := config.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid service configuration: %w", err)
	}
	return config, nil
}

func configurationDecode(name string, node *yaml.Node, target any) error {
	if err := node.Decode(target); err != nil {
		return fmt.Errorf("invalid configuration %s: %w", name, err)
	}
	return nil
}

func configurationDuration(name string, node *yaml.Node, target *time.Duration) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("configuration %s requires a duration string", name)
	}
	value, err := time.ParseDuration(node.Value)
	if err != nil || value < time.Millisecond {
		return fmt.Errorf("configuration %s requires a duration of at least one millisecond", name)
	}
	*target = value
	return nil
}

func configurationMapping(node *yaml.Node) (map[string]*yaml.Node, error) {
	if node.Kind != yaml.MappingNode {
		return nil, errors.New("service configuration requires mappings")
	}
	fields := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || key.Value == "<<" {
			return nil, errors.New("service configuration requires literal string keys")
		}
		if _, exists := fields[key.Value]; exists {
			return nil, fmt.Errorf("duplicate service configuration key %q", key.Value)
		}
		fields[key.Value] = node.Content[i+1]
	}
	return fields, nil
}

func validateConfigurationNode(node *yaml.Node, depth int) error {
	if depth > 16 || node.Kind == yaml.AliasNode || node.Tag == "!!null" {
		return errors.New("service configuration rejects aliases, null values and excessive nesting")
	}
	if node.Kind == yaml.DocumentNode && len(node.Content) != 1 {
		return errors.New("service configuration requires one nonempty document")
	}
	if node.Kind == yaml.MappingNode {
		if _, err := configurationMapping(node); err != nil {
			return err
		}
	}
	for _, child := range node.Content {
		if err := validateConfigurationNode(child, depth+1); err != nil {
			return err
		}
	}
	return nil
}
