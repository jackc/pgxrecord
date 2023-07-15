package pgxrecord

import (
	"fmt"
	"strings"
)

type ValidationError struct {
	field string
	err   error
}

func (ve *ValidationError) Field() string {
	return ve.field
}

func (ve *ValidationError) Unwrap() error {
	return ve.err
}

func (ve *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", ve.field, ve.err)
}

type ValidationErrors struct {
	errors []*ValidationError
}

// Add adds a new error to the validation errors for the given field. By convention, an empty string for field indicates
// a record-level error.
func (ve *ValidationErrors) Add(field string, err error) {
	ve.errors = append(ve.errors, &ValidationError{field: field, err: err})
}

// Len returns the number of errors in the ValidationErrors.
func (ve *ValidationErrors) Len() int {
	if ve == nil {
		return 0
	}

	return len(ve.errors)
}

// On returns a []*ValidationError for the given field.
func (ve *ValidationErrors) On(field string) []*ValidationError {
	if ve == nil {
		return nil
	}

	var errs []*ValidationError
	for _, e := range ve.errors {
		if e.field == field {
			errs = append(errs, e)
		}
	}
	return errs
}

// All returns all errors.
func (ve *ValidationErrors) All() []*ValidationError {
	if ve == nil {
		return nil
	}

	return ve.errors
}

// Unwrap unwraps all errors.
func (ve *ValidationErrors) Unwrap() []error {
	var errs []error
	for _, e := range ve.errors {
		errs = append(errs, e)
	}

	return errs
}

// Error satisfies the error interface.
func (ve *ValidationErrors) Error() string {
	if len(ve.errors) == 0 {
		return "BUG: ValidationErrors.Error() called with no errors"
	}

	sb := strings.Builder{}
	for i, e := range ve.errors {
		if i > 0 {
			sb.WriteString(", ")
		}

		if e.field == "" {
			sb.WriteString(e.err.Error())
		} else {
			sb.WriteString(e.field)
			sb.WriteString(": ")
			sb.WriteString(e.err.Error())
		}
	}

	return sb.String()
}

type GetterSetter interface {
	Get(attribute string) any
	Set(attribute string, value any)
}

type RecordValidator struct {
	record GetterSetter
	errors *ValidationErrors
}

func (v *RecordValidator) Validate(field string, validators ...ValueValidator) {
	value := v.record.Get(field)
	for _, validator := range validators {
		var err error
		value, err = validator.Validate(value)
		if err != nil {
			v.errors.Add(field, err)
			return
		}
	}
	v.record.Set(field, value)
}

type ValueValidator interface {
	Validate(any) (any, error)
}

// type RecordValidator interface {
// 	ValidateRecord(ctx context.Context, db DB, table *Table, record *Record) error
// }

// type RecordValidatorBuilder struct {
// 	ctx    context.Context
// 	db     DB
// 	table  *Table
// 	record *Record
// }

// func NewRecordValidatorBuilder(fn func(rvb *RecordValidatorBuilder)) RecordValidator {

// }
