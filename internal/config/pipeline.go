package config

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	PipelineFilename      = "pipeline.yaml"
	DefaultCommandTimeout = Duration(30 * time.Second)
)

type Pipeline struct {
	Version   int                  `yaml:"version"`
	ID        string               `yaml:"id"`
	Enabled   bool                 `yaml:"enabled"`
	Env       map[string]string    `yaml:"env"`
	Secrets   map[string]SecretRef `yaml:"secrets"`
	Resources []string             `yaml:"resources"`
	Guard     Command              `yaml:"guard"`
	Steps     []Step               `yaml:"steps"`
	Dir       string               `yaml:"-"`
	File      string               `yaml:"-"`
}

type SecretRef struct {
	Env  string `yaml:"env,omitempty"`
	File string `yaml:"file,omitempty"`
}

type Command struct {
	Argv    []string `yaml:"argv"`
	Timeout Duration `yaml:"timeout"`
}

type Step struct {
	ID      string   `yaml:"id"`
	Argv    []string `yaml:"argv"`
	Timeout Duration `yaml:"timeout"`
}

// LoadPipeline loads a pipeline.yaml file. Passing its containing directory is
// also supported. Resource and secret file references must remain inside that
// directory and are returned as absolute paths.
func LoadPipeline(path string) (*Pipeline, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve pipeline path: %w", err)
	}
	if info, statErr := os.Stat(absolute); statErr == nil && info.IsDir() {
		absolute = filepath.Join(absolute, PipelineFilename)
	}

	pipeline := &Pipeline{
		Enabled: true,
		Env:     make(map[string]string),
		Secrets: make(map[string]SecretRef),
		Guard:   Command{Timeout: DefaultCommandTimeout},
	}
	if err := decodeStrictYAML(absolute, pipeline); err != nil {
		return nil, fmt.Errorf("load pipeline %q: %w", absolute, err)
	}
	pipeline.File = filepath.Clean(absolute)
	pipeline.Dir = filepath.Dir(pipeline.File)
	if err := pipeline.normalizeAndValidate(); err != nil {
		return nil, fmt.Errorf("validate pipeline %q: %w", absolute, err)
	}
	return pipeline, nil
}

// LoadPipelines loads pipeline.yaml from each immediate child directory. Child
// directories without pipeline.yaml and nested pipeline files are ignored.
func LoadPipelines(root string) ([]*Pipeline, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve pipelines directory: %w", err)
	}
	entries, err := os.ReadDir(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("read pipelines directory %q: %w", absoluteRoot, err)
	}

	result := make([]*Pipeline, 0, len(entries))
	ids := make(map[string]string)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(absoluteRoot, entry.Name(), PipelineFilename)
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("inspect pipeline %q: %w", entry.Name(), err)
		}
		pipeline, err := LoadPipeline(path)
		if err != nil {
			return nil, err
		}
		canonicalID := strings.ToLower(pipeline.ID)
		if previous, exists := ids[canonicalID]; exists {
			return nil, fmt.Errorf("duplicate pipeline id %q in %q and %q", pipeline.ID, previous, pipeline.File)
		}
		ids[canonicalID] = pipeline.File
		result = append(result, pipeline)
	}
	return result, nil
}

func (pipeline *Pipeline) normalizeAndValidate() error {
	if pipeline.Version != CurrentVersion {
		return fmt.Errorf("version must be %d", CurrentVersion)
	}
	if !validID(pipeline.ID) {
		return fmt.Errorf("id %q is invalid", pipeline.ID)
	}
	if pipeline.Env == nil {
		pipeline.Env = make(map[string]string)
	}
	if pipeline.Secrets == nil {
		pipeline.Secrets = make(map[string]SecretRef)
	}
	for name := range pipeline.Env {
		if !validEnvName(name) {
			return fmt.Errorf("env name %q is invalid", name)
		}
		if reservedEnvName(name) {
			return fmt.Errorf("env name %q uses the reserved BOARKSHOP_ prefix", name)
		}
	}
	for name, reference := range pipeline.Secrets {
		if !validEnvName(name) {
			return fmt.Errorf("secret destination env name %q is invalid", name)
		}
		if reservedEnvName(name) {
			return fmt.Errorf("secret destination env name %q uses the reserved BOARKSHOP_ prefix", name)
		}
		if _, exists := pipeline.Env[name]; exists {
			return fmt.Errorf("environment name %q is declared in both env and secrets", name)
		}
		if (reference.Env == "") == (reference.File == "") {
			return fmt.Errorf("secret %q must set exactly one of env or file", name)
		}
		if reference.Env != "" && !validEnvName(reference.Env) {
			return fmt.Errorf("secret %q source env %q is invalid", name, reference.Env)
		}
		if reference.File != "" {
			resolved, err := resolveSafeReference(pipeline.Dir, reference.File, true)
			if err != nil {
				return fmt.Errorf("secret %q file: %w", name, err)
			}
			reference.File = resolved
			pipeline.Secrets[name] = reference
		}
	}

	seenResources := make(map[string]struct{}, len(pipeline.Resources))
	for index, resource := range pipeline.Resources {
		resolved, err := resolveSafeReference(pipeline.Dir, resource, false)
		if err != nil {
			return fmt.Errorf("resources[%d]: %w", index, err)
		}
		if _, exists := seenResources[resolved]; exists {
			return fmt.Errorf("duplicate resource %q", resource)
		}
		seenResources[resolved] = struct{}{}
		pipeline.Resources[index] = resolved
	}

	if err := commandError("guard", pipeline.Guard.Argv, pipeline.Guard.Timeout); err != nil {
		return err
	}
	stepIDs := make(map[string]struct{}, len(pipeline.Steps))
	for index := range pipeline.Steps {
		step := &pipeline.Steps[index]
		if !validID(step.ID) {
			return fmt.Errorf("steps[%d].id %q is invalid", index, step.ID)
		}
		if _, exists := stepIDs[step.ID]; exists {
			return fmt.Errorf("duplicate step id %q", step.ID)
		}
		stepIDs[step.ID] = struct{}{}
		if step.Timeout == 0 {
			step.Timeout = DefaultCommandTimeout
		}
		if err := commandError(fmt.Sprintf("step %q", step.ID), step.Argv, step.Timeout); err != nil {
			return err
		}
	}
	return nil
}

func resolveSafeReference(root, reference string, requireFile bool) (string, error) {
	if strings.TrimSpace(reference) == "" {
		return "", fmt.Errorf("reference must not be empty")
	}
	portable := strings.ReplaceAll(reference, `\`, "/")
	if path.IsAbs(portable) || filepath.IsAbs(reference) || filepath.VolumeName(reference) != "" ||
		(len(portable) >= 2 && portable[1] == ':') {
		return "", fmt.Errorf("reference %q must be relative", reference)
	}
	portable = path.Clean(portable)
	if portable == "." || portable == ".." || strings.HasPrefix(portable, "../") {
		return "", fmt.Errorf("reference %q escapes the pipeline directory", reference)
	}
	clean := filepath.FromSlash(portable)

	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	target := filepath.Join(absoluteRoot, clean)
	relative, err := filepath.Rel(absoluteRoot, target)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("reference %q escapes the pipeline directory", reference)
	}
	info, err := lstatWithoutLinks(absoluteRoot, relative)
	if err != nil {
		return "", fmt.Errorf("reference %q: %w", reference, err)
	}
	if requireFile && !info.Mode().IsRegular() {
		return "", fmt.Errorf("reference %q must name a regular file", reference)
	}
	return filepath.Clean(target), nil
}

// lstatWithoutLinks walks every referenced component without following it.
// Rejecting links entirely avoids both directory escapes and time-of-check /
// time-of-use ambiguity when commands later open a configured resource.
func lstatWithoutLinks(root, relative string) (os.FileInfo, error) {
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("pipeline directory is a symbolic link")
	}
	if !rootInfo.IsDir() {
		return nil, fmt.Errorf("pipeline root is not a directory")
	}

	current := root
	components := strings.Split(filepath.Clean(relative), string(filepath.Separator))
	info := rootInfo
	for index, component := range components {
		current = filepath.Join(current, component)
		var err error
		info, err = os.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("path component %q is a symbolic link", component)
		}
		if index < len(components)-1 && !info.IsDir() {
			return nil, fmt.Errorf("path component %q is not a directory", component)
		}
	}
	return info, nil
}
