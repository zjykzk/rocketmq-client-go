package log

import "fmt"

// MockLogger mock logger
type MockLogger struct{}

func (l MockLogger) Debug(v ...interface{}) {
	fmt.Print(v...)
	println()
}
func (l MockLogger) Debugf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	println()
}

func (l MockLogger) Info(v ...interface{}) {
	fmt.Print(v...)
	println()
}
func (l MockLogger) Infof(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	println()
}

func (l MockLogger) Warnf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	println()
}
func (l MockLogger) Warn(v ...interface{}) {
	fmt.Print(v...)
	println()
}

func (l MockLogger) Errorf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
	println()
}
func (l MockLogger) Error(v ...interface{}) {
	fmt.Print(v...)
	println()
}
