package log

import "log"

type Logger interface {
	Debug(v ...any)
	Debugf(format string, v ...any)
}

var logger Logger

func SetLogger(l Logger) {
	logger = l
}

func Debug(v ...any) {
	if logger == nil {
		return
	}
	logger.Debug(v...)
}

func Debugf(format string, v ...any) {
	if logger == nil {
		return
	}
	logger.Debugf(format, v...)
}

type defaultLogger struct {
	logger *log.Logger
}

func NewLogger() *defaultLogger {
	return &defaultLogger{
		logger: log.Default(),
	}
}

func (l *defaultLogger) Debug(v ...any) {
	l.logger.Print(v...)
}

func (l *defaultLogger) Debugf(format string, v ...any) {
	l.logger.Printf(format, v...)
}
