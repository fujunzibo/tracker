package errorx

import (
	"fmt"
	"runtime"
)

type newError struct {
	msg    string
	loc    string
	frames []runtime.Frame
}

func (e *newError) Error() string {
	if len(e.frames) < 1 {
		if e.loc != "" {
			return e.msg + " at " + e.loc
		}
		return e.msg
	}
	stacks := ""
	for _, frame := range e.frames {
		stacks += fmt.Sprintf("\n        at %s:%d", frame.File, frame.Line)
	}
	return e.msg + stacks
}

func (e *newError) WithLoc(skip ...int) *newError {
	var sk int
	if len(skip) > 0 {
		sk = skip[0] + 1
	} else {
		sk = 1
	}
	fr := GetFrame(sk)
	e.loc = fmt.Sprintf("%s:%d", fr.File, fr.Line)
	return e
}

func (e *newError) WithStack(skip ...int) *newError {
	var sk int
	if len(skip) > 0 {
		sk = skip[0] + 1
	} else {
		sk = 1
	}
	e.frames = GetFrames(sk)
	return e
}

func New(msg string) *newError {
	return &newError{
		msg: msg,
	}
}

func NewWithStack(msg string, skip ...int) *newError {
	var sk int
	if len(skip) > 0 {
		sk = skip[0] + 1
	} else {
		sk = 1
	}
	return &newError{
		msg:    msg,
		frames: GetFrames(sk),
	}
}

func NewWithLoc(msg string, skip ...int) *newError {
	var sk int
	if len(skip) > 0 {
		sk = skip[0]
	}
	we := &newError{msg: msg}
	return we.WithLoc(sk + 1)
}
