package configx

import (
	"bytes"
	"fmt"
	"reflect"
)

type preset interface {
	Preset()
}

type validator interface {
	Validate() error
}

func Preset(v reflect.Value) {
	if !v.IsValid() || !v.CanInterface() {
		return
	}
	var p preset
	var ok bool
	kind := v.Kind()
	if kind == reflect.Ptr {
		if v.IsNil() {
			return
		}
		p, ok = v.Interface().(preset)
		for kind == reflect.Ptr {
			v = v.Elem()
			kind = v.Kind()
		}
	} else if v.CanAddr() {
		p, ok = v.Addr().Interface().(preset)
	}

	if ok {
		p.Preset()
	}

	switch kind {
	case reflect.Struct:
		num := v.NumField()
		for i := 0; i < num; i++ {
			Preset(v.Field(i))
		}
	case reflect.Slice, reflect.Array:
		num := v.Len()
		for i := 0; i < num; i++ {
			Preset(v.Index(i))
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			Preset(iter.Value())
		}
	}
}

type Error struct {
	errs []error
}

func (e *Error) Errors() []error {
	return e.errs
}

func (e *Error) Len() int {
	return len(e.errs)
}

func (e *Error) Append(errs ...error) {
	e.errs = append(e.errs, errs...)
}

func (e *Error) Extend(err error) {
	te, ok := err.(*Error)
	if ok {
		e.errs = append(e.errs, te.errs...)
	} else {
		e.errs = append(e.errs, err)
	}
}

func (e *Error) Error() string {
	buf := &bytes.Buffer{}
	for _, err := range e.errs {
		buf.WriteString(err.Error())
		buf.WriteByte(';')
		buf.WriteByte(' ')
	}
	if buf.Len() < 1 {
		return ""
	}
	buf.Truncate(buf.Len() - 2)
	return buf.String()
}

func (e *Error) Unwrap() error {
	if len(e.errs) == 0 {
		return nil
	}
	return e
}

func validate(v validator) (err error) {
	defer func() {
		re := recover()
		if re != nil {
			var ok bool
			err, ok = re.(error)
			if !ok {
				err = fmt.Errorf("%v", re)
			}
		}
	}()
	err = v.Validate()
	return
}

func Validate(v reflect.Value) error {
	if !v.IsValid() || !v.CanInterface() {
		return nil
	}

	var p validator
	var ok bool
	kind := v.Kind()
	if kind == reflect.Ptr {
		p, ok = v.Interface().(validator)
		for kind == reflect.Ptr && !v.IsNil() {
			v = v.Elem()
			kind = v.Kind()
		}
	} else {
		p, ok = v.Interface().(validator)
		if !ok && v.CanAddr() {
			p, ok = v.Addr().Interface().(validator)
		}
	}

	errs := &Error{}

	if ok {
		err := validate(p)
		if err != nil {
			errs.Extend(err)
		}
	}

	switch kind {
	case reflect.Struct:
		num := v.NumField()
		for i := 0; i < num; i++ {
			err := Validate(v.Field(i))
			if err != nil {
				errs.Extend(err)
			}
		}
	case reflect.Slice, reflect.Array:
		num := v.Len()
		for i := 0; i < num; i++ {
			err := Validate(v.Index(i))
			if err != nil {
				errs.Extend(err)
			}
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			err := Validate(iter.Value())
			if err != nil {
				errs.Extend(err)
			}
		}
	}
	return errs.Unwrap()
}
