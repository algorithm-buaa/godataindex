package godataindex

import (
	"encoding/csv"
	// "fmt"
	"io"
	"os"
	"strconv"
	"testing"
	// "unsafe"
)

var datafile string = "data.csv"

var di *DataIndex
var df []DataIndex

func init() {
	seq := []float64{
		1.0, 1.0, 1.0, 1.0, 1.0, 1.0, // 0-5
		3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, // 6-12
		5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, // 13-20
		9.0,                                      // 21
		10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, // 22-28
		9.0, //29
	}

	di = NewIndexWithArray(seq)

	// df, _ = NewIndexFromCSVFile(datafile)
}

func TestGetValueForTime(t *testing.T) {

	times := []int{-1, 0, 1, 6, 20, 21, 28, 29, 30}
	expected := []float64{0.0, 1.0, 1.0, 3.0, 5.0, 9.0, 10.0, 9.0, 0.0}

	for i, exp := range expected {
		act, err := di.GetValueForTime(times[i])
		if exp != act {
			t.Errorf("Expected %.4f != Actual %.4f (err: %v)", exp, act, err)
		}
	}
}

type interval struct {
	t1, t2 int
}

func TestReplayInterval(t *testing.T) {
	times := []interval{
		{0, 0},
		{0, 1},
		{0, 7},
		{5, 13},
		{21, 29},
	}
	exp := [][]float64{
		{1.0},
		{1.0, 1.0},
		{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 3.0, 3.0},
		{1.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 5.0},
		{9.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 9.0}, // 21;29}
	}

	for i, time := range times {
		act := di.ReplayInterval(time.t1, time.t2)

		if !equal(exp[i], act) {
			t.Errorf("%d: EXP: %v != ACT %v", i, exp[i], act)
		}
	}
}

func equal(exp, act []float64) bool {
	if len(exp) != len(act) {
		return false
	}

	for i := 0; i < len(exp); i++ {
		if exp[i] != act[i] {
			return false
		}
	}
	return true
}

func TestDataFile(t *testing.T) {
	if df == nil {
		return
	}

	expected := []float64{0.0, 0.0840, 0.078, 0.0}
	times := []int{-1, 89756, 252704, 375842}

	var exp, act float64
	var i int
	var err error
	for i, exp = range expected {
		act, err = df[0].GetValueForTime(times[i])
		if exp != act {
			t.Errorf("Expected %.4f != Actual %.4f (err: %v)",
				exp, act, err)
		}
	}

}

func TestWriteAndRead(t *testing.T) {
	if df == nil {
		return
	}

	// write first dataindex to disk
	err := df[0].WriteToFile("first.out")
	if err != nil {
		t.Errorf("Err: %v", err)
	}

	// read dataindex from disk
	var di *DataIndex
	di, err = NewIndexFromIndexFile("first.out")
	if err != nil {
		t.Errorf("Err: %v", err)
	}
	// insert a value at the end of it 
	// to make sure that times etc. are correctly
	// updated as we load in the values
	var exp, act float64
	exp = 0.005
	di.AddVal(exp)

	act, err = di.GetValueForTime(375842)
	if err != nil {
		t.Errorf("Err: %v", err)
	}
	if act != exp {
		t.Errorf("Expected %.4f != Actual %.4f (err: %v)",
			exp, act)
	}
	// fmt.Println(di)

}

var data []DataIndex

func BenchmarkNewDataIndex(b *testing.B) {
	// var err error
	for i := 0; i < b.N; i++ {
		data, _ = parseCSVFile(datafile)
	}
}

// func BenchmarkNaiveDataParseFile(b *testing.B) {

// 	for i := 0; i < b.N; i++ {
// 		data = naiveParseFile(datafile)
// 	}
// }

func BenchmarkGetValueForTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		exp := 0.078
		if act, err := data[0].GetValueForTime(252704); act != exp {
			b.Errorf("Expected %.4f == Actual %.4f (err: %v)", exp, act, err)
		}
	}
}

func naiveParseFile(path string) [][]float64 {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	r := csv.NewReader(file)

	var arr [][]float64
	var row []string

	// allocate array for each column

	row, err = r.Read()
	r.FieldsPerRecord = len(row)
	arr = make([][]float64, len(row), len(row))

	// allocate array for each row or use and appropriate number
	for i := 0; i < len(arr); i++ {
		arr[i] = make([]float64, 0, 1000)
	}

	if row != nil {
		for i := 0; i < len(arr); i++ {
			val, err := strconv.ParseFloat(row[i], 64)
			if err != nil {
				panic(err)
			}
			arr[i] = append(arr[i], val)
		}
	}

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		var val float64
		for i, col := range row {

			val, err = strconv.ParseFloat(col, 64)
			if err != nil {
				panic(err)
			}
			arr[i] = append(arr[i], val)
		}
	}
	return arr
}
