package functor

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"

	hll "github.com/DataDog/hyperloglog"

	"github.com/xsnout/grizzly/capnp/grizzly"
)

// Functor embodies an aggregate function.  It typically has an internal state that is
// 1. intialized before the first row is processed
// 2. updated by using information from a row
// 3. read by calling `Value`
// 4. Reset at the window boundary to be ready to aggregate the next values from the upcoming window.
type Functor interface {
	Init(typ *grizzly.FieldType)
	Reset()
	Update(value interface{})
	Value() interface{}
}

type First struct {
	alreadySet bool
	first      interface{}
}

func (f *First) Init(typ *grizzly.FieldType) {
	f.Reset()
}

func (f *First) Reset() {
	f.alreadySet = false
}

func (f *First) Update(value interface{}) {
	if !f.alreadySet {
		f.alreadySet = true
		f.first = value
	}
}

func (f *First) Value() interface{} {
	return f.first
}

type Last struct {
	Last interface{}
}

func (f *Last) Init(typ *grizzly.FieldType) {
	f.Reset()
}

func (f *Last) Reset() {
	// TODO: Nothing to do?
}

func (f *Last) Update(value interface{}) {
	f.Last = value
}

func (f *Last) Value() interface{} {
	return f.Last
}

type Counter struct {
	Count int64
}

func (f *Counter) Init(typ *grizzly.FieldType) {
	f.Reset()
}

func (f *Counter) Reset() {
	f.Count = 0
}

func (f *Counter) Update(ignoreMe interface{}) {
	f.Count++
}

func (f *Counter) Value() interface{} {
	return float64(f.Count)
}

type Averager struct {
	theType grizzly.FieldType
	Count   int64
	Sum     float64
}

func (f *Averager) Init(typ *grizzly.FieldType) {
	f.theType = *typ
	f.Reset()
}

func (f *Averager) Reset() {
	f.Count = 0
	f.Sum = 0
}

func (f *Averager) Update(value interface{}) {
	f.Count++
	switch f.theType {
	case grizzly.FieldType_float64:
		f.Sum += +value.(float64)
	case grizzly.FieldType_integer64:
		f.Sum += float64(value.(int64))
	default:
		panic(fmt.Errorf("unknown type %T of value %v", value, value))
	}
}

func (f *Averager) Value() interface{} {
	return f.Sum / float64(f.Count)
}

type Minimizer struct {
	TheType grizzly.FieldType
	Minimum interface{}
}

func (f *Minimizer) Init(typ *grizzly.FieldType) {
	f.TheType = *typ
	f.Reset()
}

func (f *Minimizer) Reset() {
	switch f.TheType {
	case grizzly.FieldType_boolean:
		f.Minimum = false
	case grizzly.FieldType_float64:
		f.Minimum = float64(math.MaxFloat64)
	case grizzly.FieldType_integer64:
		f.Minimum = int64(math.MaxInt64)
	default:
		panic(fmt.Errorf("unknown type %v", f.TheType))
	}
}

func (f *Minimizer) Update(value interface{}) {
	switch f.TheType {
	case grizzly.FieldType_float64:
		if value.(float64) < f.Minimum.(float64) {
			f.Minimum = value
		}
	case grizzly.FieldType_integer64:
		v, ok := value.(int64)
		if !ok {
			panic(fmt.Errorf("cannot cast value %v of type %T to int64", value, value))
		}
		m, ok := f.Minimum.(int64)
		if !ok {
			panic(fmt.Errorf("cannot cast value %v of type %T to int64", f.Minimum, f.Minimum))
		}
		if v < m {
			f.Minimum = int64(v)
		}
	default:
		panic(fmt.Errorf("unknown type %T of value %v", value, value))
	}
}

func (f *Minimizer) Value() interface{} {
	switch f.TheType {
	case grizzly.FieldType_float64:
		if v, ok := f.Minimum.(float64); !ok {
			panic(fmt.Errorf("cannot convert value %v to type float64", v))
		} else {
			return float64(v)
		}
	case grizzly.FieldType_integer64:
		if v, ok := f.Minimum.(int64); !ok {
			panic(fmt.Errorf("cannot convert value %v to type int64", v))
		} else {
			return int64(v)
		}
	default:
		panic(fmt.Errorf("unknown type %v", f.TheType))
	}
}

type Maximizer struct {
	TheType grizzly.FieldType
	Maximum interface{}
}

func (f *Maximizer) Init(typ *grizzly.FieldType) {
	f.TheType = *typ
	f.Reset()
}

func (f *Maximizer) Reset() {
	switch f.TheType {
	case grizzly.FieldType_float64:
		f.Maximum = float64(-math.MaxFloat64)
	case grizzly.FieldType_integer64:
		f.Maximum = int64(math.MinInt64)
	default:
		panic(fmt.Errorf("unknown type %v", f.TheType))
	}
}

func (f *Maximizer) Update(value interface{}) {
	switch f.TheType {
	case grizzly.FieldType_float64:
		if value.(float64) > f.Maximum.(float64) {
			f.Maximum = value
		}
	case grizzly.FieldType_integer64:
		v, ok := value.(int64)
		if !ok {
			panic(fmt.Errorf("1cannot convert value %v to type int64", value))
		}
		m, ok := f.Maximum.(int64)
		if !ok {
			panic(fmt.Errorf("2cannot convert value %v to type int64 (%T)", f.Maximum, f.Maximum))
		}
		if int64(v) > int64(m) {
			f.Maximum = int64(v)
		}
	default:
		panic(fmt.Errorf("unknown type %T of value %v", value, value))
	}
}

func (f *Maximizer) Value() interface{} {
	return f.Maximum
}

type NoOp struct {
	TheType  grizzly.FieldType
	TheValue interface{}
}

func (f *NoOp) Init(typ *grizzly.FieldType) {
	f.TheType = *typ
	f.Reset()
}

func (f *NoOp) Reset() {
}

func (f *NoOp) Update(value interface{}) {
	switch f.TheType {
	case grizzly.FieldType_float64:
		if value.(float64) < f.TheValue.(float64) {
			f.TheValue = value
		}
	case grizzly.FieldType_integer64:
		v, ok := value.(int64)
		if !ok {
			panic(fmt.Errorf("cannot cast value %v of type %T to int64", value, value))
		}
		m, ok := f.TheValue.(int64)
		if !ok {
			panic(fmt.Errorf("cannot cast value %v of type %T to int64", f.TheValue, f.TheValue))
		}
		if v < m {
			f.TheValue = int64(v)
		}
	default:
		panic(fmt.Errorf("unknown type %T of value %v", value, value))
	}
}

func (f *NoOp) Value() interface{} {
	switch f.TheType {
	case grizzly.FieldType_float64:
		if v, ok := f.TheValue.(float64); !ok {
			panic(fmt.Errorf("cannot convert value %v to type float64", v))
		} else {
			return float64(v)
		}
	case grizzly.FieldType_integer64:
		if v, ok := f.TheValue.(int64); !ok {
			panic(fmt.Errorf("cannot convert value %v to type int64", v))
		} else {
			return int64(v)
		}
	default:
		panic(fmt.Errorf("unknown type %v", f.TheType))
	}
}

type Summer struct {
	TheType grizzly.FieldType
	Sum     float64
}

func (f *Summer) Init(typ *grizzly.FieldType) {
	f.TheType = *typ
	f.Reset()
}

func (f *Summer) Reset() {
	f.Sum = 0
}

func (f *Summer) Update(value interface{}) {
	switch f.TheType {
	case grizzly.FieldType_float64:
		f.Sum += value.(float64)
	case grizzly.FieldType_integer64:
		f.Sum += float64(value.(int64))
	default:
		panic(fmt.Errorf("unknown type %v", f.TheType.String()))
	}
}

func (f *Summer) Value() interface{} {
	return f.Sum
}

type DistinctCounter struct {
	TheType     grizzly.FieldType
	Counts      map[uint32]int
	NumDistinct int
}

func (f *DistinctCounter) Init(typ *grizzly.FieldType) {
	f.TheType = *typ
	f.Reset()
}

func (f *DistinctCounter) Reset() {
	f.Counts = make(map[uint32]int)
	f.NumDistinct = 0
}

func (f *DistinctCounter) Update(value interface{}) {
	key := getHash(f.TheType, value)
	if _, ok := f.Counts[key]; ok {
		f.Counts[key]++
	} else {
		f.Counts[key] = 1
		f.NumDistinct++
	}
}

func (f *DistinctCounter) Value() interface{} {
	return f.NumDistinct
}

type Uniquer struct {
	TheType grizzly.FieldType
	HLL     *hll.HyperLogLog
}

func (f *Uniquer) Init(typ *grizzly.FieldType) {
	f.TheType = *typ
	f.Reset()
}

func (f *Uniquer) Reset() {
	const i int = 17
	m := uint(math.Pow(2, float64(i)))
	if h, err := hll.New(m); err != nil {
		panic(fmt.Errorf("cannot make New(%d): %v", m, err))
	} else {
		f.HLL = h
	}
}

func (f *Uniquer) Update(value interface{}) {
	f.HLL.Add(getHash(f.TheType, value))
}

func (f *Uniquer) Value() interface{} {
	return f.HLL.Count()
}

func getHash(typ grizzly.FieldType, value interface{}) (result uint32) {
	hash := fnv.New32()

	switch typ {
	case grizzly.FieldType_float64:
		hash.Write([]byte(float64ToBytes(value.(float64))))
	case grizzly.FieldType_integer64:
		hash.Write([]byte(int64ToBytes(int64(value.(int64)))))
	case grizzly.FieldType_text:
		hash.Write([]byte(value.(string)))
	default:
		panic(fmt.Errorf("unknown type %v", typ))
	}

	result = hash.Sum32()
	hash.Reset()
	return
}

func float64ToBytes(f float64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(f))
	return buf[:]
}

func int64ToBytes(i int64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(i))
	return buf[:]
}
