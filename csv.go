// Package csv implements a rel.Relation object that uses csv.Reader
package csv

import (
	"encoding/csv"
	"github.com/jonlawlor/rel"
	"github.com/jonlawlor/rel/att"
	"io"
	"reflect"
	"strconv"
)

// New creates a relation that reads from csv, with one tuple per csv record.

func New(r *csv.Reader, z interface{}, ckeystr [][]string) rel.Relation {
	if len(ckeystr) == 0 {
		return &csvTable{r, att.DefaultKeys(z), z, false, nil}
	}
	ckeys := att.String2CandKeys(ckeystr)
	att.OrderCandidateKeys(ckeys)
	return &csvTable{r, ckeys, z, true, nil}
}

// csvTable is an implementation of Relation using a csv.Reader
type csvTable struct {
	// the *csv.Reader which can produce records
	source1 *csv.Reader

	// set of candidate keys
	cKeys att.CandKeys

	// the type of the tuples contained within the relation
	zero interface{}

	// sourceDistinct indicates if the source csv was already distinct or if a
	// distinct has to be performed when sending tuples
	sourceDistinct bool

	// TODO(jonlawlor): fold the errors between csv reading & type conversion
	// together with line numbers?
	err error
}

// error types

type FieldMismatch struct {
	expected, found int
}

func (e *FieldMismatch) Error() string {
	return "CSV line fields mismatch. Expected " + strconv.Itoa(e.expected) + " found " + strconv.Itoa(e.found)
}

type UnsupportedType struct {
	Type string
}

func (e *UnsupportedType) Error() string {
	return "Unsupported type: " + e.Type
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *csvTable) Tuples(t chan<- interface{}) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}
	e1 := reflect.TypeOf(r.zero)
	// the unmarshaller is based on this stackoverflow answer:
	// http://stackoverflow.com/a/20773337/774834
	// thanks, Valentyn Shybanov!

	if r.sourceDistinct {
		go func(reader *csv.Reader, e1 reflect.Type, res chan<- interface{}) {
			for {
				record, err := reader.Read()

				// if we have an error, it might just be eof, which means
				// we are done reading the csv.
				if err != nil {
					if err != io.EOF {
						r.err = err
					}
					break
				}

				tup := reflect.Indirect(reflect.New(e1))
				err = parseTuple(&tup, record)
				if err != nil {
					r.err = err
					break
				}
				select {
				case res <- tup.Interface():
				case <-cancel:
					// do nothing, this is the end of the line
					return
				}
			}
			close(res)
		}(r.source1, e1, t)
		return cancel
	}
	m := map[interface{}]struct{}{}
	go func(reader *csv.Reader, e1 reflect.Type, res chan<- interface{}) {
		for {
			record, err := reader.Read()

			// if we have an error, it might just be eof, which means
			// we are done reading the csv.
			if err != nil {
				if err != io.EOF {
					r.err = err
				}
				break
			}

			tup := reflect.Indirect(reflect.New(e1))
			err = parseTuple(&tup, record)
			if err != nil {
				r.err = err
				break
			}
			if _, isdup := m[tup.Interface()]; !isdup {
				m[tup.Interface()] = struct{}{}
				select {
				case res <- tup.Interface():
				case <-cancel:
					// do nothing, this is the end of the line
					return
				}
			}
		}
		close(res)
	}(r.source1, e1, t)
	return cancel
}

func parseTuple(tup *reflect.Value, record []string) error {
	if tup.NumField() != len(record) {
		return &FieldMismatch{tup.NumField(), len(record)}
	}
	for i := 0; i < tup.NumField(); i++ {
		f := tup.Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(record[i])
		case reflect.Bool:
			val, err := strconv.ParseBool(record[i])
			if err != nil {
				return err
			}
			f.SetBool(val)
		case reflect.Int:
			val, err := strconv.ParseInt(record[i], 0, 0)
			if err != nil {
				return err
			}
			f.SetInt(val)
		case reflect.Int8:
			val, err := strconv.ParseInt(record[i], 0, 8)
			if err != nil {
				return err
			}
			f.SetInt(val)
		case reflect.Int16:
			val, err := strconv.ParseInt(record[i], 0, 16)
			if err != nil {
				return err
			}
			f.SetInt(val)
		case reflect.Int32:
			val, err := strconv.ParseInt(record[i], 0, 32)
			if err != nil {
				return err
			}
			f.SetInt(val)
		case reflect.Int64:
			val, err := strconv.ParseInt(record[i], 0, 64)
			if err != nil {
				return err
			}
			f.SetInt(val)
		case reflect.Uint:
			val, err := strconv.ParseUint(record[i], 0, 0)
			if err != nil {
				return err
			}
			f.SetUint(val)
		case reflect.Uint8:
			val, err := strconv.ParseUint(record[i], 0, 8)
			if err != nil {
				return err
			}
			f.SetUint(val)
		case reflect.Uint16:
			val, err := strconv.ParseUint(record[i], 0, 16)
			if err != nil {
				return err
			}
			f.SetUint(val)
		case reflect.Uint32:
			val, err := strconv.ParseUint(record[i], 0, 32)
			if err != nil {
				return err
			}
			f.SetUint(val)
		case reflect.Uint64:
			val, err := strconv.ParseUint(record[i], 0, 64)
			if err != nil {
				return err
			}
			f.SetUint(val)
		case reflect.Float32:
			val, err := strconv.ParseFloat(record[i], 32)
			if err != nil {
				return err
			}
			f.SetFloat(val)
		case reflect.Float64:
			val, err := strconv.ParseFloat(record[i], 64)
			if err != nil {
				return err
			}
			f.SetFloat(val)
		default:
			return &UnsupportedType{f.Type().String()}
		}
	}
	return nil
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *csvTable) Zero() interface{} {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *csvTable) CKeys() att.CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *csvTable) GoString() string {
	return "placeholder"
}

// String returns a text representation of the Relation
func (r *csvTable) String() string {
	return "Relation(" + rel.HeadingString(r) + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *csvTable) Project(z2 interface{}) rel.Relation {
	return rel.NewProject(r1, z2)
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *csvTable) Restrict(p att.Predicate) rel.Relation {
	return rel.NewRestrict(r1, p)
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *csvTable) Rename(z2 interface{}) rel.Relation {
	e2 := reflect.TypeOf(z2)

	// figure out the new names
	names2 := att.FieldNames(e2)

	// create a map from the old names to the new names if there is any
	// difference between them
	nameMap := make(map[att.Attribute]att.Attribute)
	for i, att := range rel.Heading(r1) {
		nameMap[att] = names2[i]
	}

	cKeys1 := r1.cKeys
	cKeys2 := make(att.CandKeys, len(cKeys1))
	// for each of the candidate keys, rename any keys from the old names to
	// the new ones
	for i := range cKeys1 {
		cKeys2[i] = make([]att.Attribute, len(cKeys1[i]))
		for j, key := range cKeys1[i] {
			cKeys2[i][j] = nameMap[key]
		}
	}
	// order the keys
	att.OrderCandidateKeys(cKeys2)

	r1.zero = z2
	r1.cKeys = cKeys2
	return r1
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *csvTable) Union(r2 rel.Relation) rel.Relation {
	return rel.NewUnion(r1, r2)
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *csvTable) SetDiff(r2 rel.Relation) rel.Relation {
	return rel.NewSetDiff(r1, r2)
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *csvTable) Join(r2 rel.Relation, zero interface{}) rel.Relation {
	return rel.NewJoin(r1, r2, zero)
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *csvTable) GroupBy(t2, vt interface{}, gfcn func(<-chan interface{}) interface{}) rel.Relation {
	return rel.NewGroupBy(r1, t2, vt, gfcn)
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *csvTable) Map(mfcn func(from interface{}) (to interface{}), z2 interface{}, ckeystr [][]string) rel.Relation {
	return rel.NewMap(r1, mfcn, z2, ckeystr)
}

// Error returns an error encountered during construction or computation
func (r1 *csvTable) Err() error {
	return r1.err
}
