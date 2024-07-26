// Package engine implements the actual query processor.  It runs exacly one query generated using the compiler package.
package engine

import (
	"encoding/csv"
	"fmt"
	"math"

	"io"
	"os"
	"sync"
	"time"

	"github.com/xsnout/grizzly/capnp/data"
	"github.com/xsnout/grizzly/capnp/grizzly"
	"github.com/xsnout/grizzly/pkg/_out/functions"
	"github.com/xsnout/grizzly/pkg/common"
	"github.com/xsnout/grizzly/pkg/compiler"
	"github.com/xsnout/grizzly/pkg/operator"
	"github.com/xsnout/grizzly/pkg/utility"

	"capnproto.org/go/capnp/v3"

	"github.com/rs/zerolog"
)

const (
	CsvComment          = '#'
	ChannelCapacity int = 1000
)

var (
	log zerolog.Logger
)

func init() {
	//zerolog.SetGlobalLevel(zerolog.Disabled)
	//zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log = zerolog.New(os.Stderr).With().Caller().Timestamp().Logger()
	log.Info().Msg("Engine says welcome!")

	operator.Init() // configure logging
}

type Engine struct {
	exitAfterSeconds int
	planRoot         grizzly.Node

	reader io.Reader
	writer io.Writer

	ingress         operator.Ingress
	ingressFilter   operator.Filter
	window          operator.Window
	aggregate       operator.Aggregate
	aggregateFilter operator.Filter
	project         operator.Project
	projectFilter   operator.Filter
	egress          operator.Egress

	ingressToIngressFilterChannel     chan *data.IngressRow
	ingressFilterToWindowChannel      chan *data.IngressRow
	windowToAggregateChannel          chan []*data.IngressRow // the slice is a window of rows
	aggregateToAggregateFilterChannel chan *data.AggregateRow
	aggregateFilterToProjectChannel   chan *data.AggregateRow
	projectToProjectFilterChannel     chan *data.EgressRow
	projectFilterToEgressChannel      chan *data.EgressRow
}

func NewEngine(
	dataReader io.Reader,
	dataWriter io.Writer,
	planReader io.Reader,
	exitAfterSeconds int,
) *Engine {
	root := utility.ReadBinaryPlan(planReader)
	template := "could not find node for %s operator"

	var found bool
	var node *grizzly.Node

	var window operator.Window
	if node, found = utility.FindFirstNodeByType(&root, grizzly.OperatorType_window); !found {
		panic(fmt.Errorf(template, grizzly.OperatorType_window.String()))
	}
	window.Init(node)

	var egress operator.Egress
	if node, found = utility.FindFirstNodeByType(&root, grizzly.OperatorType_egress); !found {
		panic(fmt.Errorf(template, grizzly.OperatorType_egress.String()))
	}
	egress.Init(node)

	var ingress operator.Ingress
	if node, found = utility.FindFirstNodeByType(&root, grizzly.OperatorType_ingress); !found {
		panic(fmt.Errorf(template, grizzly.OperatorType_ingress.String()))
	}
	ingress.Init(node)

	var aggregate operator.Aggregate
	if node, found = utility.FindFirstNodeByType(&root, grizzly.OperatorType_aggregate); !found {
		panic(fmt.Errorf(template, grizzly.OperatorType_aggregate.String()))
	}
	aggregate.Init(node)

	var project operator.Project
	if node, found = utility.FindFirstNodeByType(&root, grizzly.OperatorType_project); !found {
		panic(fmt.Errorf(template, grizzly.OperatorType_project.String()))
	}
	project.Init(node)

	var ingressFilter operator.Filter
	if node, found = utility.FindFirstNodeByType(&root, grizzly.OperatorType_ingressFilter); !found {
		panic(fmt.Errorf(template, grizzly.OperatorType_ingressFilter.String()))
	}
	ingressFilter.Init(node)

	var aggregateFilter operator.Filter
	if node, found = utility.FindFirstNodeByType(&root, grizzly.OperatorType_aggregateFilter); !found {
		panic(fmt.Errorf(template, grizzly.OperatorType_aggregateFilter.String()))
	}
	aggregateFilter.Init(node)

	var projectFilter operator.Filter
	if node, found = utility.FindFirstNodeByType(&root, grizzly.OperatorType_projectFilter); !found {
		panic(fmt.Errorf(template, grizzly.OperatorType_projectFilter.String()))
	}
	projectFilter.Init(node)

	return &Engine{
		exitAfterSeconds: exitAfterSeconds,
		planRoot:         root,

		reader: dataReader,
		writer: dataWriter,

		ingress:         ingress,
		ingressFilter:   ingressFilter,
		window:          window,
		aggregate:       aggregate,
		aggregateFilter: aggregateFilter,
		project:         project,
		projectFilter:   projectFilter,
		egress:          egress,

		ingressToIngressFilterChannel:     make(chan *data.IngressRow, ChannelCapacity),
		ingressFilterToWindowChannel:      make(chan *data.IngressRow, ChannelCapacity),
		windowToAggregateChannel:          make(chan []*data.IngressRow, ChannelCapacity),
		aggregateToAggregateFilterChannel: make(chan *data.AggregateRow, ChannelCapacity),
		aggregateFilterToProjectChannel:   make(chan *data.AggregateRow, ChannelCapacity),
		projectToProjectFilterChannel:     make(chan *data.EgressRow, ChannelCapacity),
		projectFilterToEgressChannel:      make(chan *data.EgressRow, ChannelCapacity),
	}
}

func (e *Engine) Run() {
	go e.IngressWorker()
	go e.IngressFilterWorker()
	go e.WindowWorker()
	go e.AggregateWorker()
	go e.AggregateFilterWorker()
	go e.ProjectWorker()
	go e.ProjectFilterWorker()
	go e.EgressWorker()

	time.Sleep(time.Duration(e.exitAfterSeconds) * time.Second)
}

func (e *Engine) IngressWorker() {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}

	csvReader := csv.NewReader(e.reader)
	csvReader.Comma = common.CsvSeparator
	csvReader.Comment = CsvComment

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		ingressRow, err := data.NewIngressRow(seg)
		if err != nil {
			panic(err)
		}
		e.ingress.Ingress(record, &ingressRow)
		e.ingressToIngressFilterChannel <- &ingressRow
	}
}

func (e *Engine) IngressFilterWorker() {
	var filter functions.Filter
	for {
		ingressRow := <-e.ingressToIngressFilterChannel
		//pass := e.ingressFilterOp.Filter(payload)
		pass := filter.EvalIngressFilter(*ingressRow)
		if pass {
			e.ingressFilterToWindowChannel <- ingressRow
		}
	}
}

func (e *Engine) WindowWorker() {
	log.Info().Msgf("WindowWorker: windowType: %s", e.window.WindowType)

	switch e.window.WindowType {
	case compiler.WindowTypeSession:
		e.SessionWindowWorker()
	case compiler.WindowTypeSlice:
		switch e.window.IntervalType {
		case compiler.IntervalTypeTime:
			if e.window.SequenceField == "" {
				e.LiveTimeWindowWorker()
			} else { // "based on" clause present
				e.ReplayTimeWindowWorker()
			}
		case compiler.IntervalTypeDistance:
			if e.window.SequenceField == "" {
				e.LiveDistanceWindowWorker()
			} else { // "based on" clause present
				e.ReplayDistanceWindowWorker()
			}
		default:
			panic(fmt.Errorf("interval type not implemented %v for window type %v", e.window.IntervalType, e.window.WindowType))
		}
	default:
		panic(fmt.Errorf("window type not implemented: %v", e.window.WindowType))
	}
}

type Window []*data.IngressRow

type WindowGroup struct {
	groupFieldNames []string
	windows         map[string]Window
}

func CreateWindowGroup(groupFieldNames []string) (wg WindowGroup) {
	wg.groupFieldNames = groupFieldNames
	wg.windows = make(map[string]Window)
	return
}

func (wg *WindowGroup) IsOpen(groupKey string) (ok bool) {
	_, ok = wg.windows[groupKey]
	return
}

func (wg *WindowGroup) Append(ingressRow *data.IngressRow) {
	groupKey := wg.GroupKey(ingressRow)

	var window Window
	var ok bool
	if window, ok = wg.windows[groupKey]; !ok {
		window = []*data.IngressRow{ingressRow}
	} else {
		window = append(window, ingressRow)
	}
	wg.windows[groupKey] = window
}

func (wg *WindowGroup) Close(groupKey string) (window Window, ok bool) {
	if window, ok = wg.windows[groupKey]; !ok {
		// That's fine, the window has already been closed before.
		return
	}
	delete(wg.windows, groupKey)
	return
}

func (wg *WindowGroup) GroupKey(ingressRow *data.IngressRow) (key string) {
	var group data.Group
	var err error
	if group, err = ingressRow.Group(); err != nil {
		panic(err)
	}

	key = ""
	for _, name := range wg.groupFieldNames {
		getMethodName := utility.UpcaseFirstLetter(name)
		values := operator.InvokeWithoutParameters(group, getMethodName)
		key += fmt.Sprintf("%v", values[0])
	}
	return
}

func (wg *WindowGroup) AllGroupKeys() (keys []string) {
	keys = make([]string, len(wg.windows))
	i := 0
	for k := range wg.windows {
		keys[i] = k
		i++
	}
	return
}

func (e *Engine) SessionWindowWorker() {
	var filter functions.Filter

	if len(e.window.GroupFieldNames) == 0 {
		window := []*data.IngressRow{}

		for {
			ingressRow := <-e.ingressFilterToWindowChannel
			if len(window) > 0 { // is open
				keepOpen := !filter.EvalSessionCloseFilter(*ingressRow)
				if keepOpen {
					window = append(window, ingressRow)
					continue // fetch next row
				} else { // close it, create new empty window
					window = []*data.IngressRow{}
					if e.window.SessionIncludeClosingRow { // inclusive window
						window = append(window, ingressRow)
					}
					e.windowToAggregateChannel <- window
					// Now, check if the current row opens a new window.
				}
			}
			// closed window
			if filter.EvalSessionOpenFilter(*ingressRow) {
				window = []*data.IngressRow{ingressRow}

			}
		}
	} else {
		wg := CreateWindowGroup(e.window.GroupFieldNames)

		for {
			ingressRow := <-e.ingressFilterToWindowChannel
			key := wg.GroupKey(ingressRow)
			if wg.IsOpen(key) {
				keepOpen := !filter.EvalSessionCloseFilter(*ingressRow)
				if keepOpen {
					wg.Append(ingressRow)
					continue // fetch next row
				} else { // close it, create new empty window
					var window Window
					var ok bool
					if window, ok = wg.Close(key); !ok {
						continue // The window happens to be closed already, that's fine.
					}
					if e.window.SessionIncludeClosingRow { // inclusive window
						window = append(window, ingressRow)
					}
					e.windowToAggregateChannel <- window
					// Now, check if the current row opens a new window.
				}
			}
			// closed window
			if filter.EvalSessionOpenFilter(*ingressRow) {
				wg.Append(ingressRow) // open a new window
			}
		}
	}
}

func (e *Engine) LiveDistanceWindowWorker() {
	maxRows := int(e.window.IntervalRows)
	var window []*data.IngressRow
	for {
		for i := 0; i < maxRows; i++ {
			ingressRow := <-e.ingressFilterToWindowChannel
			window = append(window, ingressRow)
		}
		//log.Info().Msgf("RowedWindowWorker: %d rows interval elapsed", maxRows)
		e.windowToAggregateChannel <- window
		window = []*data.IngressRow{}
	}
}

func (e *Engine) LiveTimeWindowWorker() {
	intervalMillis := int(e.window.TickerSeconds * 1000)
	ticker := time.NewTicker(time.Duration(intervalMillis) * time.Millisecond)
	quit := make(chan struct{})
	rowCount := 0
	totalRowCount := 0

	var windowMutex sync.Mutex

	if len(e.window.GroupFieldNames) == 0 {
		var window Window
		for {
			go func() {
				for {
					ingressRow := <-e.ingressFilterToWindowChannel
					windowMutex.Lock()
					window = append(window, ingressRow)
					windowMutex.Unlock()
					rowCount++
				}
			}()

			select {
			case <-ticker.C:
				e.windowToAggregateChannel <- window
				windowMutex.Lock()
				window = Window{}
				windowMutex.Unlock()
				totalRowCount += rowCount
				rowCount = 0
			case <-quit:
				ticker.Stop()
				return
			}
		}
	} else {
		wg := CreateWindowGroup(e.window.GroupFieldNames)
		for {
			go func() {
				for {
					ingressRow := <-e.ingressFilterToWindowChannel
					windowMutex.Lock()
					wg.Append(ingressRow)
					windowMutex.Unlock()
					rowCount++
				}
			}()

			select {
			case <-ticker.C:
				windowMutex.Lock() // Let's keep the lock time short
				keys := wg.AllGroupKeys()
				windowMutex.Unlock()
				for _, key := range keys {
					//log.Info().Msgf("groupKey: %s, len: %d", groupKey, len(groupKey))
					windowMutex.Lock() // Let's keep the lock time short
					window, ok := wg.Close(key)
					windowMutex.Unlock()
					if ok {
						e.windowToAggregateChannel <- window
					}
				}
				totalRowCount += rowCount
				rowCount = 0
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}
}

// If we have historic data, we process it as fast as possible.
func (e *Engine) ReplayTimeWindowWorker() {
	chunkDuration := time.Second * time.Duration(e.window.TickerSeconds)
	var hi time.Time

	if len(e.window.GroupFieldNames) == 0 { // without grouping
		window := Window{}
		for {
			ingressRow := <-e.ingressFilterToWindowChannel
			t := operator.Timestamp(ingressRow, e.window.SequenceField)

			if hi.Before(t) { // hi < t
				// Close the window and emit it, and add the current row to a new window.
				if len(window) > 0 {
					// Emit
					e.windowToAggregateChannel <- window
				}
				// Populate new window
				window = Window{ingressRow}
				_, hi = surroundingTimeInterval(t, chunkDuration)
			} else { // t < hi
				window = append(window, ingressRow)
			}
		}
	} else { // with grouping
		wg := CreateWindowGroup(e.window.GroupFieldNames)

		for {
			ingressRow := <-e.ingressFilterToWindowChannel
			t := operator.Timestamp(ingressRow, e.window.SequenceField)

			if hi.Before(t) { // hi < t
				// Close all windows and emit them.
				keys := wg.AllGroupKeys()
				for _, key := range keys {
					window, ok := wg.Close(key)
					if ok {
						e.windowToAggregateChannel <- window
					}
				}
				_, hi = surroundingTimeInterval(t, chunkDuration)
			}
			wg.Append(ingressRow)
		}
	}
}

func (e *Engine) ReplayDistanceWindowWorker() {
	chunkDistance := int(e.window.IntervalRows)
	var hi int

	if len(e.window.GroupFieldNames) == 0 { // without grouping
		window := Window{}
		for {
			ingressRow := <-e.ingressFilterToWindowChannel
			r := operator.Rowstamp(ingressRow, e.window.IntervalField)

			if hi < r {
				// Close the window and emit it, and add the current row to a new window.
				if len(window) > 0 {
					// Emit
					e.windowToAggregateChannel <- window
				}
				// Populate new window
				window = Window{ingressRow}
				_, hi = surroundingRowInterval(r, chunkDistance)
			} else { // r < hi
				window = append(window, ingressRow)
			}
		}
	} else { // with grouping
		wg := CreateWindowGroup(e.window.GroupFieldNames)

		for {
			ingressRow := <-e.ingressFilterToWindowChannel
			r := operator.Rowstamp(ingressRow, e.window.SequenceField)

			if hi < r {
				// Close all windows and emit them.
				keys := wg.AllGroupKeys()
				for _, key := range keys {
					window, ok := wg.Close(key)
					if ok {
						e.windowToAggregateChannel <- window
					}
				}
				_, hi = surroundingRowInterval(r, chunkDistance)
			}
			wg.Append(ingressRow)
		}
	}
}

func (e *Engine) AggregateWorker() {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}

	for {
		window := <-e.windowToAggregateChannel
		e.aggregate.Reset()
		if len(window) == 0 { // Nothing to aggregate over
			break
		}

		// for i, ingressRow := range window {
		// 	log.Info().Msgf("AggregateWorker: row %d: %v", i, ingressRow)
		// }

		var group data.Group
		ingressRow := window[0]
		if group, err = ingressRow.Group(); err != nil {
			panic(err)
		}

		for _, ingressRow := range window {
			e.aggregate.Update(*ingressRow)
		}

		var aggregateRow data.AggregateRow
		if aggregateRow, err = data.NewAggregateRow(seg); err != nil {
			panic(err)
		}
		e.aggregate.Value(&aggregateRow)

		if err = aggregateRow.SetGroup(group); err != nil {
			panic(err)
		}

		e.aggregateToAggregateFilterChannel <- &aggregateRow
	}
}

func (e *Engine) AggregateFilterWorker() {
	var filter functions.Filter
	for {
		aggregateRow := <-e.aggregateToAggregateFilterChannel
		pass := filter.EvalAggregateFilter(*aggregateRow)
		if pass {
			e.aggregateFilterToProjectChannel <- aggregateRow
		}
	}
}

func (e *Engine) ProjectWorker() {
	_, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		panic(err)
	}

	for {
		aggregateRow := <-e.aggregateFilterToProjectChannel
		var egressRow data.EgressRow
		if egressRow, err = data.NewEgressRow(seg); err != nil {
			panic(err)
		}
		e.project.Project(aggregateRow, &egressRow)
		e.projectToProjectFilterChannel <- &egressRow
	}
}

func (e *Engine) ProjectFilterWorker() {
	var filter functions.Filter
	for {
		egressRow := <-e.projectToProjectFilterChannel
		pass := filter.EvalProjectFilter(*egressRow)
		if pass {
			e.projectFilterToEgressChannel <- egressRow
		}
	}
}

func (e *Engine) EgressWorker() {
	csvWriter := csv.NewWriter(e.writer)
	csvWriter.Comma = common.CsvSeparator

	for {
		egressRow := <-e.projectFilterToEgressChannel

		var payload data.EgressPayload
		var err error
		if payload, err = egressRow.Payload(); err != nil {
			panic(err)
		}

		var group data.Group
		if group, err = egressRow.Group(); err != nil {
			panic(err)
		}

		var record []string
		for _, fieldName := range e.egress.OutputFieldNames {
			getMethodName := utility.UpcaseFirstLetter(fieldName)
			values := operator.InvokeWithoutParameters(payload, getMethodName)
			value := values[0]
			text := fmt.Sprintf("%v", value)
			record = append(record, text)
		}

		// Append the group values
		for _, fieldName := range e.egress.GroupFieldNames {
			getMethodName := utility.UpcaseFirstLetter(fieldName)
			values := operator.InvokeWithoutParameters(group, getMethodName)
			value := values[0]
			text := fmt.Sprintf("%v", value)
			record = append(record, text)
		}

		csvWriter.Write(record)
		csvWriter.Flush()
	}
}

// Finds the surrounding wall clock interval boundaries for a given timestamp
// and the width of the interval.
//
// Example: slice = 5 * time.Minute
// t:  2024-01-24T20:45:03-08:00
// lo: 2024-01-24T20:45:00-08:00
// hi: 2024-01-24T20:50:00-08:00
func surroundingTimeInterval(t time.Time, slice time.Duration) (lo time.Time, hi time.Time) {
	lo = t.Truncate(slice)
	hi = lo.Add(slice)
	return
}

func surroundingRowInterval(row int, slice int) (lo int, hi int) {
	lo = int(math.Floor(float64(row)/float64(slice))) * slice
	hi = lo + slice
	return
}
