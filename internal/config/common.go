package config

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const CurrentVersion = 1

var (
	idPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	envPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func decodeStrictYAML(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return err
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple YAML documents are not allowed")
		}
		return err
	}
	return nil
}

func validID(value string) bool {
	return len(value) <= 128 && idPattern.MatchString(value)
}

func validEnvName(value string) bool {
	return envPattern.MatchString(value)
}

func reservedEnvName(value string) bool {
	return strings.HasPrefix(strings.ToUpper(value), "BOARKSHOP_")
}

func commandError(scope string, argv []string, timeout Duration) error {
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return fmt.Errorf("%s argv must contain a non-empty executable", scope)
	}
	for i, arg := range argv {
		if strings.IndexByte(arg, 0) >= 0 {
			return fmt.Errorf("%s argv[%d] contains a NUL byte", scope, i)
		}
	}
	if timeout <= 0 {
		return fmt.Errorf("%s timeout must be positive", scope)
	}
	return nil
}
