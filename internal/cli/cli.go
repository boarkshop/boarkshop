package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/boarkshop/boarkshop/internal/app"
)

type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer, build BuildInfo) int {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		printUsage(stdout)
		return 0
	}
	command := "serve"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command = args[0]
		args = args[1:]
	}

	switch command {
	case "serve":
		return runServe(ctx, args, stderr)
	case "validate":
		return runValidate(args, stdout, stderr)
	case "version":
		if len(args) != 0 {
			fmt.Fprintln(stderr, "version does not accept arguments")
			return 2
		}
		fmt.Fprintf(stdout, "boarkshop %s (commit %s, built %s)\n", valueOr(build.Version, "dev"), valueOr(build.Commit, "unknown"), valueOr(build.Date, "unknown"))
		return 0
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", command)
		printUsage(stderr)
		return 2
	}
}

func runServe(ctx context.Context, args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultConfigPath(), "path to instance YAML configuration")
	logLevel := flags.String("log-level", "info", "debug, info, warn, or error")
	flags.Usage = func() { printServeUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	level, err := parseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: level}))
	if err := app.Run(ctx, *configPath, logger); err != nil {
		logger.Error("boarkshop stopped with an error", "error", err)
		return 1
	}
	return 0
}

func runValidate(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultConfigPath(), "path to instance YAML configuration")
	flags.Usage = func() { fmt.Fprintln(stderr, "Usage: boarkshop validate [--config PATH]") }
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments: %s\n", strings.Join(flags.Args(), " "))
		return 2
	}
	if err := app.Validate(*configPath); err != nil {
		fmt.Fprintf(stderr, "configuration is invalid: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "configuration is valid")
	return 0
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q", value)
	}
}

func defaultConfigPath() string {
	if configured := os.Getenv("BOARKSHOP_CONFIG"); configured != "" {
		return configured
	}
	return "boarkshop.yaml"
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, `Usage:
  boarkshop [serve] [--config PATH] [--log-level LEVEL]
  boarkshop validate [--config PATH]
  boarkshop version

With no command, boarkshop starts the daemon.`)
}

func printServeUsage(output io.Writer) {
	fmt.Fprintln(output, "Usage: boarkshop [serve] [--config PATH] [--log-level LEVEL]")
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
