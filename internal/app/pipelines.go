package app

import (
	"fmt"

	"github.com/boarkshop/boarkshop/internal/engine"
)

// pipelineDirectorySource treats the configured pipelines directory as the
// source of truth. Each Load returns a complete immutable-ready generation;
// callers retain their previous generation when Load fails.
type pipelineDirectorySource struct {
	root         string
	allowMissing bool
}

func (source pipelineDirectorySource) Load() ([]engine.Pipeline, error) {
	manifests, err := loadPipelineManifests(source.root, source.allowMissing)
	if err != nil {
		return nil, err
	}
	pipelines := make([]engine.Pipeline, 0, len(manifests))
	for _, manifest := range manifests {
		if !manifest.Enabled {
			continue
		}
		secretEnv := make(map[string]string, len(manifest.Secrets))
		for destination, reference := range manifest.Secrets {
			value, err := resolveReference(reference.Env, reference.File)
			if err != nil {
				return nil, fmt.Errorf("pipeline %q secret %q: %w", manifest.ID, destination, err)
			}
			secretEnv[destination] = value
		}
		steps := make([]engine.Step, 0, len(manifest.Steps))
		for _, step := range manifest.Steps {
			steps = append(steps, engine.Step{
				ID:      step.ID,
				Argv:    append([]string(nil), step.Argv...),
				Timeout: step.Timeout.Std(),
			})
		}
		pipelines = append(pipelines, engine.Pipeline{
			ID:        manifest.ID,
			Directory: manifest.Dir,
			Env:       cloneStringMap(manifest.Env),
			SecretEnv: secretEnv,
			Guard: engine.Step{
				ID:      "guard",
				Argv:    append([]string(nil), manifest.Guard.Argv...),
				Timeout: manifest.Guard.Timeout.Std(),
			},
			Steps: steps,
		})
	}
	return pipelines, nil
}
