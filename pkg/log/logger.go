package log

import (
	"hyper-tracker/pkg/lifecycle"
	"io"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

var out io.Writer = os.Stdout

type Config struct {
	Level      string `json:"level" yaml:"level"`
	Format     string `json:"format" yaml:"format"`
	Stdout     bool   `json:"stdout" yaml:"stdout"`
	Output     string `json:"output" yaml:"output"`
	MaxAge     int    `json:"max_age" yaml:"max_age"`
	MaxSize    int    `json:"max_size" yaml:"max_size"`
	MaxBackups int    `json:"max_backups" yaml:"max_backups"`
	Compress   bool   `json:"compress" yaml:"compress"`
}

func NewOutput(cfg *Config) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   cfg.Output,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAge,
		Compress:   cfg.Compress,
	}
}

func GetOut() io.Writer {
	return out
}

func NewLogger(cfg *Config) (io.Writer, *slog.Logger) {
	var ow io.Writer
	if cfg.Output != "" {
		output := NewOutput(cfg)
		lifecycle.RegisterShutdownNoErrorCloser(output)
		if cfg.Stdout {
			ow = io.MultiWriter(os.Stdout, output)
		} else {
			ow = output
		}
	} else {
		ow = os.Stdout
	}
	var level slog.Level
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	switch strings.ToLower(cfg.Format) {
	case "json":
		handler := slog.NewJSONHandler(ow, &slog.HandlerOptions{
			Level:     level,
			AddSource: true,
		})
		return ow, slog.New(handler)
	default:
		handler := slog.NewTextHandler(ow, &slog.HandlerOptions{
			Level:     level,
			AddSource: true,
		})
		return ow, slog.New(handler)
	}
}

func Setup(cfg *Config) {
	ow, logger := NewLogger(cfg)
	out = ow
	slog.SetDefault(logger)
}
