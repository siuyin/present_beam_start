package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"

	"mybeam/wordcount"
)

var (
	input  = flag.String("input", "gs://apache-beam-samples/shakespeare/kinglear.txt", "File(s) to read.")
	output = flag.String("output", "junk.txt", "Output (required).")
)

func main() {
	flag.Parse()
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	lines := textio.Read(s, *input)
	stamped := beam.ParDo(s, &addRandomTimestamp{Min: mtime.Now()}, lines)
	windowed := beam.WindowInto(s, window.NewFixedWindows(60*time.Minute), stamped)
	counted := wordcount.Words(s, windowed)
	formatted := beam.ParDo(s, formatFn, counted)
	merged := beam.WindowInto(s, window.NewGlobalWindows(), formatted)
	textio.Write(s, *output, merged)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

type addRandomTimestamp struct {
	Min beam.EventTime `json:"min"`
}

func (f *addRandomTimestamp) ProcessElement(x beam.X) (beam.EventTime, beam.X) { // beam.X is a Universal Type used to represent "generic" types in DoFn and PCollection signatures. Each universal type is distinct from all others.
	ts := f.Min.Add(fudgeHours(2))
	return ts, x
}
func fudgeHours(h int) time.Duration {
	randNanoSecs := rand.Int63n(int64(h) * time.Hour.Nanoseconds())
	return time.Duration(randNanoSecs)
}

func formatFn(iw beam.Window, et beam.EventTime, w string, c int) string {
	s := fmt.Sprintf("%v@%v %v %s: %v",
		et.ToTime().Format("15:04:05.000"), iw, iw.MaxTimestamp().ToTime().Format("15:04:05.000"),
		w, c)
	return s
}
