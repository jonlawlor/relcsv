// Package csv implements a rel.Relation object that uses csv.Reader
package csv

import (
	"encoding/csv"
	"github.com/jonlawlor/rel"
)

// New creates a relation that reads from csv, with one tuple per csv record.

func New(r *csv.Reader, z rel.T, ckeystr [][]string) rel.Relation {
	if len(ckeystr) == 0 {
		return &csvTable{r, rel.DefaultKeys(z), z, false}
	}
	ckeys := rel.String2CandKeys(ckeystr)
	rel.OrderCandidateKeys(ckeys)
	return &rel.csvTable{r, ckeys, z, true}

}

// csvTable is an implementation of Relation using a csv.Reader
type csvTable struct {
	// the *csv.Reader which can produce records
	source *csv.Reader

	// set of candidate keys
	cKeys rel.CandKeys

	// the type of the tuples contained within the relation
	zero rel.T

	// sourceDistinct indicates if the source csv was already distinct or if a
	// distinct has to be performed when sending tuples
	sourceDistinct bool

	err error
}

// Tuples sends each tuple in the relation to a channel
// note: this consumes the values of the relation, and when it is finished it
// closes the input channel.
func (r *csvTable) Tuples(t chan<- rel.T) chan<- struct{} {
	cancel := make(chan struct{})

	if r.Err() != nil {
		close(t)
		return cancel
	}

	if r.sourceDistinct {
		go func(rbody reflect.Value, res chan<- rel.T) {
			resSel := reflect.SelectCase{reflect.SelectRecv, rbody, reflect.Value{}}
			canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}
			cases := []reflect.SelectCase{resSel, canSel}
			for {
				chosen, rtup, ok := reflect.Select(cases)
				if !ok || chosen == 1 {
					// cancel has been closed, so close the results
					// TODO(jonlawlor): include a cancel channel in the rel.csvTable
					// struct so that we can continue the cancellation to the data
					// source.
					if chosen == 1 {
						return
					}
					break
				}
				select {
				case res <- T(rtup.Interface()):
				case <-cancel:
					return
				}
			}
			close(res)
		}(r.rbody, t)
		return cancel
	}
	// build up a map where each key is one of the tuples.  This consumes
	// memory.
	mem := map[T]struct{}{}
	go func(rbody reflect.Value, res chan<- rel.T) {
		resSel := reflect.SelectCase{reflect.SelectRecv, rbody, reflect.Value{}}
		canSel := reflect.SelectCase{reflect.SelectRecv, reflect.ValueOf(cancel), reflect.Value{}}
		cases := []reflect.SelectCase{resSel, canSel}
		for {
			chosen, rtup, ok := reflect.Select(cases)
			if !ok || chosen == 1 {
				// cancel has been closed, so close the results
				// TODO(jonlawlor): include a cancel channel in the rel.csvTable
				// struct so that we can continue the cancellation to the data
				// source.
				if chosen == 1 {
					return
				}
				break
			}
			tup := T(rtup.Interface())
			if _, dup := mem[tup]; !dup {
				select {
				case res <- tup:
				case <-cancel:
					return
				}
				mem[tup] = struct{}{}
			}
		}
		close(res)
	}(r.rbody, t)
	return cancel
}

// Zero returns the zero value of the relation (a blank tuple)
func (r *csvTable) Zero() rel.T {
	return r.zero
}

// CKeys is the set of candidate keys in the relation
func (r *csvTable) CKeys() CandKeys {
	return r.cKeys
}

// GoString returns a text representation of the Relation
func (r *csvTable) GoString() string {
	return goStringTabTable(r)
}

// String returns a text representation of the Relation
func (r *csvTable) String() string {
	return "Relation(" + HeadingString(r) + ")"
}

// Project creates a new relation with less than or equal degree
// t2 has to be a new type which is a subdomain of r.
func (r1 *csvTable) Project(z2 T) Relation {
	if r1.Err() != nil {
		return r1
	}
	att2 := rel.FieldNames(reflect.TypeOf(z2))
	if Deg(r1) == len(att2) {
		// either projection is an error or a no op
		return r1
	} else {
		return &ProjectExpr{r1, z2, nil}
	}
}

// Restrict creates a new relation with less than or equal cardinality
// p has to be a func(tup T) bool where tup is a subdomain of the input r.
// This is a general purpose restrict - we might want to have specific ones for
// the typical theta comparisons or <= <, =, >, >=, because it will allow much
// better optimization on the source data side.
func (r1 *csvTable) Restrict(p Predicate) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &RestrictExpr{r1, p, nil}
}

// Rename creates a new relation with new column names
// z2 has to be a struct with the same number of fields as the input relation
// note: we might want to change this into a projectrename operation?  It will
// be tricky to represent this in go's type system, I think.
func (r1 *csvTable) Rename(z2 T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &RenameExpr{r1, z2, nil}
}

// Union creates a new relation by unioning the bodies of both inputs
//
func (r1 *csvTable) Union(r2 Relation) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &UnionExpr{r1, r2, nil}
}

// SetDiff creates a new relation by set minusing the two inputs
//
func (r1 *csvTable) SetDiff(r2 Relation) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &SetDiffExpr{r1, r2, nil}
}

// Join creates a new relation by performing a natural join on the inputs
//
func (r1 *csvTable) Join(r2 Relation, zero T) Relation {
	if r1.Err() != nil {
		return r1
	}
	if r2.Err() != nil {
		return r2
	}
	return &JoinExpr{r1, r2, zero, nil}
}

// GroupBy creates a new relation by grouping and applying a user defined func
//
func (r1 *csvTable) GroupBy(t2, vt T, gfcn func(<-chan T) T) Relation {
	if r1.Err() != nil {
		return r1
	}
	return &GroupByExpr{r1, t2, vt, gfcn, nil}
}

// Map creates a new relation by applying a function to tuples in the source
func (r1 *csvTable) Map(mfcn func(from T) (to T), z2 T, ckeystr [][]string) Relation {
	if r1.Err() != nil {
		return r1
	}
	// determine the type of the returned tuples
	r := new(MapExpr)
	r.source1 = r1
	r.zero = z2
	r.fcn = mfcn
	if len(ckeystr) == 0 {
		// all relations have a candidate key of all of their attributes, or
		// a non zero subset if the relation is not dee or dum
		r.cKeys = rel.DefaultKeys(z2)
	} else {
		r.isDistinct = true
		// convert from [][]string to CandKeys
		r.cKeys = rel.String2CandKeys(ckeystr)
	}
	return r
}

// Error returns an error encountered during construction or computation
func (r1 *csvTable) Err() error {
	return r1.err
}
