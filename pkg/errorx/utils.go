package errorx

import (
	"fmt"
	"runtime"
	"strings"
)

func GetFrames(skip int) []runtime.Frame {
	pc := make([]uintptr, 32)
	n := runtime.Callers(skip+2, pc)
	if n < 1 {
		return nil
	}
	var frames []runtime.Frame
	var frame runtime.Frame
	var more bool
	cfs := runtime.CallersFrames(pc[:n])
	for {
		frame, more = cfs.Next()
		if frame.PC < 1 {
			return frames
		}
		file := frame.File
		if strings.Contains(file, "/runtime/panic.go") && len(frames) == 0 {
			continue
		}
		frames = append(frames, frame)
		if !more {
			break
		}
	}
	return frames
}

func GetFrame(skip int) *runtime.Frame {
	pc := make([]uintptr, 1)
	n := runtime.Callers(skip+2, pc)
	if n < 1 {
		return nil
	}
	cfs := runtime.CallersFrames(pc[:n])
	frame, _ := cfs.Next()
	return &frame
}

func FormatFrames(frames []runtime.Frame, indent int) string {
	var indentStr string
	if indent > 0 {
		indentStr = strings.Repeat(" ", indent)
	}
	var sb strings.Builder
	for _, frame := range frames {
		sb.WriteString(fmt.Sprintf("%s%s:%d\n", indentStr, frame.File, frame.Line))
	}
	return sb.String()
}
