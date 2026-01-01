package errorx

import (
	"errors"
	"fmt"
	"runtime"
)

type wrapError struct {
	err    error
	msg    string
	loc    string
	frames []runtime.Frame
}

func (e *wrapError) errorStr() string {
	if len(e.frames) < 1 {
		if e.loc != "" {
			return e.err.Error() + " at " + e.loc
		}
		return e.err.Error()
	}
	stacks := ""
	for _, frame := range e.frames {
		stacks += fmt.Sprintf("\n        at %s:%d", frame.File, frame.Line)
	}
	return e.err.Error() + stacks
}

func (e *wrapError) Error() string {
	if e.msg == "" {
		return e.errorStr()
	}
	return e.msg + ": " + e.errorStr()
}

func (e *wrapError) Is(err error) bool {
	if err == e || err == e.err {
		return true
	}
	return errors.Is(e.err, err)
}

func (e wrapError) As(target interface{}) bool {
	return errors.As(e.err, target)
}

func (e wrapError) Unwrap() error {
	return e.err
}

func (e *wrapError) WithMsg(msg string) *wrapError {
	e.msg = msg
	return e
}

func (e *wrapError) WithLoc(skip ...int) *wrapError {
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

func (e *wrapError) WithStack(skip ...int) *wrapError {
	var sk int
	if len(skip) > 0 {
		sk = skip[0] + 1
	} else {
		sk = 1
	}
	e.frames = GetFrames(sk)
	return e
}

func Wrap(err error) *wrapError {
	return &wrapError{err: err}
}

func WrapWithMsg(err error, msg string) *wrapError {
	return &wrapError{err: err, msg: msg}
}

func WrapWithStack(err error, skip ...int) *wrapError {
	var sk int
	if len(skip) > 0 {
		sk = skip[0] + 1
	} else {
		sk = 1
	}
	return &wrapError{
		err:    err,
		frames: GetFrames(sk),
	}
}

func WrapWithLoc(err error, skip ...int) *wrapError {
	var sk int
	if len(skip) > 0 {
		sk = skip[0]
	}
	we := &wrapError{err: err}
	return we.WithLoc(sk + 1)
}

func Errorf(format string, v ...interface{}) *wrapError {
	err := fmt.Errorf(format, v...)
	return &wrapError{
		err: err,
	}
}
