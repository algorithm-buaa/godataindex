package godataindex

import (
	"bytes"
	"encoding/csv"
	// "flag"
	"fmt"
	"io"
	// "math"
	// "errors"
	"bufio"
	"os"
	"strconv"
	"strings"
)

type DataPoint struct {
	value              float64
	timeStart, timeEnd int
	growth             bool
}

func (dp *DataPoint) GetVal() float64 {
	return dp.value
}

func (dp *DataPoint) GetStats() (int, int, float64, bool) {
	return dp.timeStart, dp.timeEnd, dp.value, dp.growth
}

func (dp *DataPoint) getInterval(t1, t2 int, arr *[]float64) (end, n int) {
	if t2 > dp.timeEnd {
		end = dp.timeEnd + 1
		n = dp.timeEnd - t1 + 1
	} else {
		end = t2 + 1
		n = t2 - t1 + 1
	}
	for i := 0; i < n; i++ {
		*arr = append(*arr, dp.value)
	}

	return
}

type DataIndex struct {
	// keep track of the time of each change
	// - use this to calculate contiguous equal datapoints
	changeTimes []int
	// keep track of the individual values
	datapoints []DataPoint
	// the last inserted value to be compared 
	// against future inserts
	currentVal float64
	// references the latest inserted datapoint
	// so we can update stats on it while 
	// the value is unchaged
	dp *DataPoint
	// current time (increased for every insert)
	time int
	// end time
	timeEnd int
}

func NewDataIndex() *DataIndex {
	// most of this is actually initialized 
	// to these datapoints automatically, 
	// but just to be specific
	return &DataIndex{
		changeTimes: make([]int, 0, 10),
		datapoints:  make([]DataPoint, 0, 10),
		currentVal:  -1,
		time:        0,
		timeEnd:     0,
	}
}

func NewIndexFromIndexFile(path string) (*DataIndex, error) {
	// most of this is actually initialized 
	// to these datapoints automatically, 
	// but just to be specific
	di, err := parseDataIndexFile(path)
	if err != nil {
		return nil, err
	}

	return di, nil
}

func NewIndexFromCSVFile(path string) ([]DataIndex, error) {
	// most of this is actually initialized 
	// to these datapoints automatically, 
	// but just to be specific
	di, err := parseCSVFile(path)
	if err != nil {
		return nil, err
	}

	return di, nil
}

func NewIndexWithArray(data []float64) *DataIndex {
	// most of this is actually initialized 
	// to these datapoints automatically, 
	// but just to be specific
	index := NewDataIndex()
	for _, val := range data {
		index.AddVal(val)
	}

	return index
}

func NewIndexWithVal(datapoint float64) *DataIndex {
	// most of this is actually initialized 
	// to these datapoints automatically, 
	// but just to be specific
	index := NewDataIndex()
	index.AddVal(datapoint)

	return index
}

// Update the end time and the current time
// - time will eventually be 1 more than what we account for
// - timeEnd is merely to prevent overflow in array
//   when searching for datapoints for a given time
func (i *DataIndex) tick() {
	i.timeEnd = i.time
	i.time++
}

func (di *DataIndex) GetTimeEnd() int {
	return di.timeEnd
}

func (di *DataIndex) GetIntervalCount() int {
	return len(di.datapoints)
}

// Returns the bottom and upper limits of the 
// interval represented in the DataIndex
// [0;timeEnd]
func (di *DataIndex) GetTimeInterval() (int, int) {
	return 0, di.timeEnd
}

// AddVal only inserts the val-value if the 
// value is different from the last imported value
// - if the datapoints are equal, we increase the repeat
//   of that particular datapoint
// - if they're not equal, we add the current time to the 
//   list of changeTimes (TODO: better word) and create 
//   a new datapoint for that value at that time
func (i *DataIndex) AddVal(val float64) {

	if i.currentVal != val {
		dp := &DataPoint{
			value:     val,
			timeStart: i.time,
			timeEnd:   i.time,
			growth:    i.currentVal <= val,
		}
		i.currentVal = val
		// insert into list of unique datapoints
		i.datapoints = append(i.datapoints, *dp)
		// register time changed
		i.changeTimes = append(i.changeTimes, i.time)
		i.dp = &i.datapoints[len(i.datapoints)-1]
	} else {
		// update the endTime for this value
		// in the interval
		i.dp.timeEnd++
	}
	// update timeEnd and the internal time
	i.tick()
}

func (i *DataIndex) AddDataPoint(start, end int, value float64, growth bool) error {
	if start > end {
		return fmt.Errorf("Invlaid interval [%d;%d]", start, end)
	}

	dp := &DataPoint{
		value:     value,
		timeStart: start,
		timeEnd:   end,
		growth:    growth,
	}
	i.currentVal = value
	// insert into list of unique datapoints
	i.datapoints = append(i.datapoints, *dp)
	// register time changed
	i.changeTimes = append(i.changeTimes, start)
	i.dp = &i.datapoints[len(i.datapoints)-1]

	// update timeEnd and time to
	// enable future AddVal() calls
	i.timeEnd = end
	i.time = end + 1

	return nil
}

// GetIndexForTime returns the index of the DataPoint where _time_
// resides
// Returns -1 if _time_ is not in [0;timeEnd]
func (di *DataIndex) getIndexForTime(time int) int {
	if time > di.timeEnd || time < 0 {
		return -1
	}

	// (TODO: @paddie could improve with a binary search alg.)
	prev := 0
	for i := 1; i < len(di.datapoints); prev, i = i, i+1 {
		if time < di.changeTimes[i] {
			break
		}
	}
	return prev
}

// Returns the index of the datapoint in which t1 starts and t2 ends
// - DEPRECATED
func (di *DataIndex) getChangeIndexesForInterval(t1, t2 int) (int, int) {

	// (TODO: @paddie could improve with a binary search alg.)
	time := t1

	var i1, i2 int
	for i, j, p := 1, 1, 0; i < len(di.changeTimes); p, i = i, i+1 {
		if time < di.changeTimes[i] {
			if j == 2 {
				i2 = p
				break
			}
			i1 = p
			time = t2
			i, j, p = i-1, j+1, p-1
		}
	}
	return i1, i2
}

// Returns the value in the datapoint covering the given time unit
func (di *DataIndex) GetValueForTime(time int) (float64, error) {

	index := di.getIndexForTime(time)
	if index == -1 {
		return 0.0, fmt.Errorf("time %d: Outside DataIndex scope: [%d;%d]",
			time, 0, di.timeEnd)
	}
	return di.datapoints[index].GetVal(), nil
}

// t1 == t2: []float{_}
// t1 << t2: []float{_,..}
// \forall t \in {t1,t2} | t <= timeEnd ^ t => 0 ^ t1 <= t2
func (di *DataIndex) ReplayInterval(t1, t2 int) []float64 {

	if t1 > t2 ||
		t1 < 0 ||
		t2 < 0 ||
		t1 > di.timeEnd ||
		t2 > di.timeEnd {
		return []float64{}
	}

	if t1 == t2 {
		val, err := di.GetValueForTime(t1)
		if err != nil {
			return nil
		}
		return []float64{val}
	}

	interval := make([]float64, 0, t2-t1+1)
	i1 := di.getIndexForTime(t1)

	// begin from the index and return values until we've
	// produced t2-t1 + 1 values from our datapoints
	id, rem := i1, t2-t1+1
	var time, n int = t1, 0
	for rem > 0 {
		time, n = di.datapoints[id].getInterval(time, t2, &interval)
		rem -= n
		id++
	}
	// fmt.Println("Done")
	return interval

}

func (i *DataIndex) String() string {
	var buffer bytes.Buffer
	for _, val := range i.datapoints {
		buffer.WriteString(fmt.Sprintf("%d-%d:%.3f%s\n",
			val.timeStart, val.timeEnd, val.value,
			func(growth bool) string {
				if growth {
					return "+"
				}
				return "-"
			}(val.growth)))
	}
	return buffer.String()
}

func (i *DataIndex) WriteToFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	for _, dp := range i.datapoints {
		_, err = fmt.Fprintf(file, "%d-%d:%.3f:%s\n",
			dp.timeStart, dp.timeEnd, dp.value,
			func(growth bool) string {
				if growth {
					return "+"
				}
				return "-"
			}(dp.growth))
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

// TODO: @paddie At bit more resiliency wrt.
//       erroneus time intervals and signs
func parseDataIndexFile(path string) (*DataIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(file)
	var line string

	di := NewDataIndex()
	for {
		line, err = readln(r)
		if err != nil {
			break
		}

		// 0-109:0.077:+
		it := strings.Split(line, ":")

		// parse the interval
		interval := strings.Split(it[0], "-")
		var ts, te int
		ts, err = strconv.Atoi(interval[0])
		if err != nil {
			fmt.Println(err)
		}
		te, err = strconv.Atoi(interval[1])
		if err != nil {
			fmt.Println(err)
		}

		// float value
		var val float64
		val, err = strconv.ParseFloat(it[1], 64)
		if err != nil {
			fmt.Println(err)
		}
		// sign
		sign := func(sign string) bool {
			if sign == "+" {
				return true
			}
			return false
		}(it[2])
		di.AddDataPoint(ts, te, val, sign)
	}

	return di, nil
}

func readln(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

// if nRows || nCols = 0, estimate or peak ahead to spy
// the dimension
func parseCSVFile(path string) ([]DataIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	r := csv.NewReader(file)

	var row []string
	// allocate array for each column
	row, err = r.Read()
	nCols := len(row)
	r.FieldsPerRecord = nCols

	arr := make([]DataIndex, nCols, nCols)
	for i := 0; i < nCols; i++ {
		arr[i] = *NewDataIndex()
		if row != nil {
			val, err := strconv.ParseFloat(row[i], 64)
			if err != nil {
				return nil, err
			}
			arr[i].AddVal(val)
		}
	}

	if row != nil {
		for i := 0; i < len(arr); i++ {
			val, err := strconv.ParseFloat(row[i], 64)
			if err != nil {
				return nil, err
			}
			arr[i].AddVal(val)
		}
	}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var val float64
		for i, col := range row {

			val, err = strconv.ParseFloat(col, 64)
			if err != nil {
				return nil, err
			}
			arr[i].AddVal(val)
		}
	}

	return arr, nil
}
