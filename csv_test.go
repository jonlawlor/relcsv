//csv_test implements some tests for csv based relations

package csv

import (
	"encoding/csv"
	"github.com/jonlawlor/rel"
	"github.com/jonlawlor/rel/att"
	"strings"
	"testing"
)

type suppliersTup struct {
	SNO    int
	SName  string
	Status int
	City   string
}

func suppliersCSV() rel.Relation {
	// example csv data
	csvStr := `1,Smith,20,London
2,Jones,10,Paris
3,Blake,30,Paris
4,Clark,20,London
5,Adams,30,Athens`
	var reader = csv.NewReader(strings.NewReader(csvStr))

	return New(reader, suppliersTup{}, [][]string{[]string{"SNO"}})
}

type orderTup struct {
	PNO int
	SNO int
	Qty int
}

func orders() rel.Relation {
	return rel.New([]orderTup{
		{1, 1, 300},
		{1, 2, 200},
		{1, 3, 400},
		{1, 4, 200},
		{1, 5, 100},
		{1, 6, 100},
		{2, 1, 300},
		{2, 2, 400},
		{3, 2, 200},
		{4, 2, 200},
		{4, 4, 300},
		{4, 5, 400},
	}, [][]string{
		[]string{"PNO", "SNO"},
	})
}

func TestCSV(t *testing.T) {
	type distinctTup struct {
		SNO   int
		SName string
	}
	type nonDistinctTup struct {
		SName string
		City  string
	}
	type titleCaseTup struct {
		Sno    int
		SName  string
		Status int
		City   string
	}
	type joinTup struct {
		PNO    int
		SNO    int
		Qty    int
		SName  string
		Status int
		City   string
	}
	type groupByTup struct {
		City   string
		Status int
	}
	type valTup struct {
		Status int
	}
	groupFcn := func(val <-chan interface{}) interface{} {
		res := valTup{}
		for vi := range val {
			v := vi.(valTup)
			res.Status += v.Status
		}
		return res
	}
	type mapRes struct {
		SNO     int
		SName   string
		Status2 int
		City    string
	}
	mapFcn := func(tup1 interface{}) interface{} {
		if v, ok := tup1.(suppliersTup); ok {
			return mapRes{v.SNO, v.SName, v.Status * 2, v.City}
		} else {
			return mapRes{}
		}
	}
	mapKeys := [][]string{
		[]string{"SNO"},
	}
	var relTest = []struct {
		rel          rel.Relation
		expectString string
		expectDeg    int
		expectCard   int
	}{
		{suppliersCSV(), "Relation(SNO, SName, Status, City)", 4, 5},
		{suppliersCSV().Restrict(att.Attribute("SNO").EQ(1)), "σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 1},
		{suppliersCSV().Project(distinctTup{}), "π{SNO, SName}(Relation(SNO, SName, Status, City))", 2, 5},
		{suppliersCSV().Project(nonDistinctTup{}), "π{SName, City}(Relation(SNO, SName, Status, City))", 2, 5},
		{suppliersCSV().Rename(titleCaseTup{}), "Relation(Sno, SName, Status, City)", 4, 5},
		{suppliersCSV().SetDiff(suppliersCSV().Restrict(att.Attribute("SNO").EQ(1))), "Relation(SNO, SName, Status, City) − σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 4},
		{suppliersCSV().Union(suppliersCSV().Restrict(att.Attribute("SNO").EQ(1))), "Relation(SNO, SName, Status, City) ∪ σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 5},
		{suppliersCSV().Join(orders(), joinTup{}), "Relation(SNO, SName, Status, City) ⋈ Relation(PNO, SNO, Qty)", 6, 11},
		{suppliersCSV().GroupBy(groupByTup{}, valTup{}, groupFcn), "Relation(SNO, SName, Status, City).GroupBy({City, Status}, {Status})", 2, 3},
		{suppliersCSV().Map(mapFcn, mapRes{}, mapKeys), "Relation(SNO, SName, Status, City).Map({SNO, SName, Status, City}->{SNO, SName, Status2, City})", 4, 5},
		{suppliersCSV().Map(mapFcn, mapRes{}, [][]string{}), "Relation(SNO, SName, Status, City).Map({SNO, SName, Status, City}->{SNO, SName, Status2, City})", 4, 5},
	}

	for i, tt := range relTest {
		if str := tt.rel.String(); str != tt.expectString {
			t.Errorf("%d has String() => %v, want %v", i, str, tt.expectString)
		}
		if deg := rel.Deg(tt.rel); deg != tt.expectDeg {
			t.Errorf("%d %s has Deg() => %v, want %v", i, tt.expectString, deg, tt.expectDeg)
		}
		if card := rel.Card(tt.rel); card != tt.expectCard {
			t.Errorf("%d %s has Card() => %v, want %v", i, tt.expectString, card, tt.expectCard)
		}
	}
}
