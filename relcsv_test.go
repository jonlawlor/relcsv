//relcsv_test implements some tests for csv based relations

package relcsv

import (
	"encoding/csv"
	"github.com/jonlawlor/rel"
	"strings"
	"testing"
)

// example csv data

type supplierTup struct {
	SNO    int
	SName  string
	Status int
	City   string
}

func suppliers() rel.Relation {
	csvStr := `1,Smith,20,London
2,Jones,10,Paris
3,Blake,30,Paris
4,Clark,20,London
5,Adams,30,Athens`
	var reader = csv.NewReader(strings.NewReader(csvStr))

	return New(reader, supplierTup{}, [][]string{[]string{"SNO"}})
}

type orderTup struct {
	PNO int
	SNO int
	Qty int
}

func orders() rel.Relation {

	csvStr := `1,1,300
1,2,200
1,3,400
1,4,200
1,5,100
1,6,100
2,1,300
2,2,400
3,2,200
4,2,200
4,4,300
4,5,400`

	var reader = csv.NewReader(strings.NewReader(csvStr))

	// intentionally non distinct
	return New(reader, orderTup{}, [][]string{})

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
	groupFcn := func(val <-chan valTup) valTup {
		res := valTup{}
		for vi := range val {
			res.Status += vi.Status
		}
		return res
	}
	type mapRes struct {
		SNO     int
		SName   string
		Status2 int
		City    string
	}
	mapFcn := func(tup1 supplierTup) mapRes {
		return mapRes{tup1.SNO, tup1.SName, tup1.Status * 2, tup1.City}
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
		{suppliers(), "Relation(SNO, SName, Status, City)", 4, 5},
		{suppliers().Restrict(rel.Attribute("SNO").EQ(1)), "σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 1},
		{suppliers().Project(distinctTup{}), "π{SNO, SName}(Relation(SNO, SName, Status, City))", 2, 5},
		{suppliers().Project(nonDistinctTup{}), "π{SName, City}(Relation(SNO, SName, Status, City))", 2, 5},
		{suppliers().Rename(titleCaseTup{}), "Relation(Sno, SName, Status, City)", 4, 5},
		{suppliers().Diff(suppliers().Restrict(rel.Attribute("SNO").EQ(1))), "Relation(SNO, SName, Status, City) − σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 4},
		{suppliers().Union(suppliers().Restrict(rel.Attribute("SNO").EQ(1))), "Relation(SNO, SName, Status, City) ∪ σ{SNO == 1}(Relation(SNO, SName, Status, City))", 4, 5},
		{suppliers().Join(orders(), joinTup{}), "Relation(SNO, SName, Status, City) ⋈ Relation(PNO, SNO, Qty)", 6, 11},
		{suppliers().GroupBy(groupByTup{}, groupFcn), "Relation(SNO, SName, Status, City).GroupBy({City, Status}->{Status})", 2, 3},
		{suppliers().Map(mapFcn, mapKeys), "Relation(SNO, SName, Status, City).Map({SNO, SName, Status, City}->{SNO, SName, Status2, City})", 4, 5},
		{suppliers().Map(mapFcn, [][]string{}), "Relation(SNO, SName, Status, City).Map({SNO, SName, Status, City}->{SNO, SName, Status2, City})", 4, 5},
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
