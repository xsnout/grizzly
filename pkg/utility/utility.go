package utility

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"

	"capnproto.org/go/capnp/v3"
	"github.com/rs/zerolog"
	"github.com/xsnout/grizzly/capnp/grizzly"
	"github.com/xsnout/grizzly/pkg/plan"
)

var (
	log zerolog.Logger
)

func Init() {
	log = zerolog.New(os.Stderr).With().Caller().Logger()
	log.Info().Msg("Catalog says welcome!")
}

func ShowPlan() {
	p := PlanString()
	fmt.Printf("%v", p)
}

func PlanString() string {
	root := ReadBinaryPlan(os.Stdin)
	var bytes []byte
	var err error
	if bytes, err = json.Marshal(plan.GrizzlyNodeToPlan(root)); err != nil {
		panic(err)
	}
	return string(bytes)
}

func ReadBinaryFile(path string) (root grizzly.Node) {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	return ReadBinaryPlan(file)
}

func ReadBinaryPlan(reader io.Reader) (root grizzly.Node) {
	r := bufio.NewReader(reader)
	var msg *capnp.Message
	var err error
	if msg, err = capnp.NewDecoder(r).Decode(); err != nil {
		panic(err)
	}
	if root, err = grizzly.ReadRootNode(msg); err != nil {
		panic(err)
	}
	return
}

func WriteBinaryFile(msg *capnp.Message, path string) {
	var file *os.File
	var err error
	if file, err = os.Create(path); err != nil {
		panic(err)
	}
	defer file.Close()
	out := io.Writer(file)
	WriteBinary(msg, out)
}

func WriteBinary(msg *capnp.Message, writer io.Writer) {
	if err := capnp.NewEncoder(writer).Encode(msg); err != nil {
		panic(err)
	}
}

func FindFirstNodeByType(node *grizzly.Node, opType grizzly.OperatorType) (target *grizzly.Node, found bool) {
	if node == nil {
		return nil, false
	}
	if node.Type() == opType {
		return node, true
	}
	if !node.HasChildren() {
		return nil, false
	}

	var children capnp.StructList[grizzly.Node]
	var err error
	if children, err = node.Children(); err != nil {
		panic(err)
	}
	for i := 0; i < children.Len(); i++ {
		candidate := children.At(i)
		var ok bool
		if target, ok = FindFirstNodeByType(&candidate, opType); ok {
			return target, true
		}
	}

	return nil, false
}

func WriteJsonFile(root *grizzly.Node, filePath string) {
	CreateFile(WriteJson(root), filePath)
}

func WriteJson(root *grizzly.Node) (bytes []byte) {
	var err error
	if bytes, err = json.Marshal(plan.GrizzlyNodeToPlan(*root)); err != nil {
		panic(err)
	}
	return
}

func CreateFile(bytes []byte, filePath string) {
	if err := os.WriteFile(filePath, bytes, 0644); err != nil {
		panic(err)
	}
}

func CreateCapnpId() (capnpId string) {
	if output, err := exec.Command("capnp", "id").Output(); err != nil {
		panic(err)
	} else {
		capnpId = string(output)
		capnpId = strings.TrimSuffix(capnpId, "\n")
		return
	}
}

func RandomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(RandomInt(65, 90))
	}
	return string(bytes)
}

func RandomInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

// Example: "foo" --> "Foo"
func UpcaseFirstLetter(s string) string {
	return strings.ToUpper(s[:1]) + s[1:]
}

// Example: 1641147953906468000 --> "2022-01-02T10:25:53.906468-08:00"
func TimeString(i int64) (text string) {
	secs := i / 1e9
	nanos := i % 1e9
	t := time.Unix(secs, nanos)
	return t.Format("2006-01-02T15:04:05.999999999Z07:00")
	//return fmt.Sprintf("%s", tTime)
}

// Example: 1641147953906468000
func NanoNow() int64 {
	return time.Now().UnixNano()
}

// "2022-01-02T10:25:53.906468-08:00" --> 1641147953906468000
func NanoTime(s string) int64 {
	var utcTime time.Time
	var err error
	if utcTime, err = time.Parse(time.RFC3339Nano, s); err != nil {
		panic(err)
	}
	return utcTime.UnixNano()
}

// "1m1s1ms" --> 61001000000
func NanoDuration(s string) int64 {
	var duration time.Duration
	var err error
	if duration, err = time.ParseDuration(s); err != nil {
		panic(err)
	}
	return duration.Nanoseconds()
}

func AddDuration(t int64, d string) int64 {
	tUnix, duration := timeAndDuration(t, d)
	return tUnix.Add(duration).UnixNano()
}

func SubDuration(t int64, d string) int64 {
	tUnix, duration := timeAndDuration(t, d)
	return tUnix.Add(-duration).UnixNano()
}

func timeAndDuration(t int64, d string) (time.Time, time.Duration) {
	secs := t / 1e9
	nanos := t % 1e9
	tUnix := time.Unix(secs, nanos)
	var duration time.Duration
	var err error
	if duration, err = time.ParseDuration(d); err != nil {
		panic(err)
	}
	return tUnix, duration
}

func ExampleTimeStuff() {
	now := NanoNow()
	fmt.Printf("%v now\n", now)
	timeString := TimeString(now)
	fmt.Printf("%v\n", timeString)
	nanoTime := NanoTime(timeString)
	fmt.Printf("%v nanoTime\n", nanoTime)
	nanoDuration := NanoDuration("1m1s1ms")
	fmt.Printf("%v nanoDuration\n", nanoDuration)
	t := AddDuration(now, "1m")
	fmt.Printf("%v new time\n", t)
	delta := t - now
	fmt.Printf("%v delta ns\n", delta)
	fmt.Printf("%v delta s\n", delta/1e9)
}
