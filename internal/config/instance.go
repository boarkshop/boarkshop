package config

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // Keep configured IANA timezones available in the standalone binary.

	"github.com/robfig/cron/v3"
)

const (
	DefaultQueueSize            = 1024
	DefaultMaxParallelProcesses = 4
	DefaultShutdownTimeout      = Duration(30 * time.Second)
	DefaultHTTPAddress          = "127.0.0.1:8080"
	DefaultHTTPMaxBodyBytes     = int64(1 << 20)
	DefaultHTTPReadHeader       = Duration(5 * time.Second)
	DefaultTelegramAPIBase      = "https://api.telegram.org"
	DefaultTelegramPollTimeout  = Duration(30 * time.Second)
)

type Instance struct {
	Version              int       `yaml:"version"`
	DataDir              string    `yaml:"data_dir"`
	PipelinesDir         string    `yaml:"pipelines_dir"`
	QueueSize            int       `yaml:"queue_size"`
	MaxParallelProcesses int       `yaml:"max_parallel_processes"`
	ShutdownTimeout      Duration  `yaml:"shutdown_timeout"`
	Listeners            Listeners `yaml:"listeners"`
}

type Listeners struct {
	HTTP     HTTPListener     `yaml:"http"`
	Telegram TelegramListener `yaml:"telegram"`
	Cron     CronListener     `yaml:"cron"`
}

type HTTPListener struct {
	Enabled           bool     `yaml:"enabled"`
	Address           string   `yaml:"address"`
	MaxBodyBytes      int64    `yaml:"max_body_bytes"`
	ReadHeaderTimeout Duration `yaml:"read_header_timeout"`
}

type TelegramListener struct {
	Bots []TelegramBot `yaml:"bots"`
}

type TelegramBot struct {
	ID          string   `yaml:"id"`
	Token       TokenRef `yaml:"token"`
	APIBase     string   `yaml:"api_base"`
	PollTimeout Duration `yaml:"poll_timeout"`
}

type TokenRef struct {
	Env  string `yaml:"env,omitempty"`
	File string `yaml:"file,omitempty"`
}

type CronListener struct {
	Timezone  string         `yaml:"timezone"`
	Schedules []CronSchedule `yaml:"schedules"`
}

type CronSchedule struct {
	ID         string `yaml:"id"`
	Expression string `yaml:"expression"`
}

// LoadInstance reads one strict, versioned instance YAML file. Relative paths
// are resolved from the directory containing the configuration file.
func LoadInstance(path string) (*Instance, error) {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve instance config path: %w", err)
	}

	config := defaultInstance(filepath.Dir(absolutePath))
	if err := decodeStrictYAML(absolutePath, config); err != nil {
		return nil, fmt.Errorf("load instance config %q: %w", absolutePath, err)
	}
	if err := config.normalizeAndValidate(filepath.Dir(absolutePath)); err != nil {
		return nil, fmt.Errorf("validate instance config %q: %w", absolutePath, err)
	}
	return config, nil
}

func defaultInstance(base string) *Instance {
	return &Instance{
		DataDir:              filepath.Join(base, "data"),
		PipelinesDir:         filepath.Join(base, "pipelines"),
		QueueSize:            DefaultQueueSize,
		MaxParallelProcesses: DefaultMaxParallelProcesses,
		ShutdownTimeout:      DefaultShutdownTimeout,
		Listeners: Listeners{
			HTTP: HTTPListener{
				Address:           DefaultHTTPAddress,
				MaxBodyBytes:      DefaultHTTPMaxBodyBytes,
				ReadHeaderTimeout: DefaultHTTPReadHeader,
			},
			Cron: CronListener{Timezone: "UTC"},
		},
	}
}

func (config *Instance) normalizeAndValidate(base string) error {
	if config.Version != CurrentVersion {
		return fmt.Errorf("version must be %d", CurrentVersion)
	}

	var err error
	config.DataDir, err = resolvePath(base, config.DataDir)
	if err != nil {
		return fmt.Errorf("data_dir: %w", err)
	}
	config.PipelinesDir, err = resolvePath(base, config.PipelinesDir)
	if err != nil {
		return fmt.Errorf("pipelines_dir: %w", err)
	}
	if pathsOverlap(config.DataDir, config.PipelinesDir) {
		return fmt.Errorf("data_dir and pipelines_dir must be separate, non-nested directories")
	}
	if config.QueueSize <= 0 {
		return fmt.Errorf("queue_size must be positive")
	}
	if config.MaxParallelProcesses <= 0 {
		return fmt.Errorf("max_parallel_processes must be positive")
	}
	if config.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown_timeout must be positive")
	}
	if err := validateHTTP(config.Listeners.HTTP); err != nil {
		return fmt.Errorf("listeners.http: %w", err)
	}
	if err := validateTelegram(base, &config.Listeners.Telegram); err != nil {
		return fmt.Errorf("listeners.telegram: %w", err)
	}
	if err := validateCron(config.Listeners.Cron); err != nil {
		return fmt.Errorf("listeners.cron: %w", err)
	}
	return nil
}

func validateHTTP(http HTTPListener) error {
	if http.Address == "" {
		return fmt.Errorf("address must not be empty")
	}
	_, port, err := net.SplitHostPort(http.Address)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", http.Address, err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("invalid TCP port %q", port)
	}
	if http.MaxBodyBytes <= 0 {
		return fmt.Errorf("max_body_bytes must be positive")
	}
	if http.ReadHeaderTimeout <= 0 {
		return fmt.Errorf("read_header_timeout must be positive")
	}
	return nil
}

func validateTelegram(base string, telegram *TelegramListener) error {
	ids := make(map[string]struct{}, len(telegram.Bots))
	for index := range telegram.Bots {
		bot := &telegram.Bots[index]
		if !validID(bot.ID) {
			return fmt.Errorf("bots[%d].id %q is invalid", index, bot.ID)
		}
		if _, exists := ids[bot.ID]; exists {
			return fmt.Errorf("duplicate bot id %q", bot.ID)
		}
		ids[bot.ID] = struct{}{}

		if (bot.Token.Env == "") == (bot.Token.File == "") {
			return fmt.Errorf("bot %q token must set exactly one of env or file", bot.ID)
		}
		if bot.Token.Env != "" && !validEnvName(bot.Token.Env) {
			return fmt.Errorf("bot %q token env %q is invalid", bot.ID, bot.Token.Env)
		}
		if bot.Token.File != "" {
			resolved, err := resolvePath(base, bot.Token.File)
			if err != nil {
				return fmt.Errorf("bot %q token file: %w", bot.ID, err)
			}
			bot.Token.File = resolved
		}

		if bot.APIBase == "" {
			bot.APIBase = DefaultTelegramAPIBase
		}
		parsed, err := url.Parse(bot.APIBase)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("bot %q api_base must be an absolute HTTP(S) URL", bot.ID)
		}
		bot.APIBase = strings.TrimRight(bot.APIBase, "/")
		if bot.PollTimeout == 0 {
			bot.PollTimeout = DefaultTelegramPollTimeout
		}
		if bot.PollTimeout <= 0 {
			return fmt.Errorf("bot %q poll_timeout must be positive", bot.ID)
		}
	}
	return nil
}

func validateCron(listener CronListener) error {
	if listener.Timezone == "" {
		return fmt.Errorf("timezone must not be empty")
	}
	if _, err := time.LoadLocation(listener.Timezone); err != nil {
		return fmt.Errorf("invalid timezone %q: %w", listener.Timezone, err)
	}

	ids := make(map[string]struct{}, len(listener.Schedules))
	for index, schedule := range listener.Schedules {
		if !validID(schedule.ID) {
			return fmt.Errorf("schedules[%d].id %q is invalid", index, schedule.ID)
		}
		if _, exists := ids[schedule.ID]; exists {
			return fmt.Errorf("duplicate schedule id %q", schedule.ID)
		}
		ids[schedule.ID] = struct{}{}
		if len(strings.Fields(schedule.Expression)) != 5 {
			return fmt.Errorf("schedule %q expression must contain exactly five fields", schedule.ID)
		}
		if _, err := cron.ParseStandard(schedule.Expression); err != nil {
			return fmt.Errorf("schedule %q expression: %w", schedule.ID, err)
		}
	}
	return nil
}

func resolvePath(base, value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("path must not be empty")
	}
	if !filepath.IsAbs(value) {
		value = filepath.Join(base, value)
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absolute), nil
}

func samePath(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func pathsOverlap(left, right string) bool {
	if samePath(left, right) {
		return true
	}
	return containsPath(left, right) || containsPath(right, left)
}

func containsPath(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
