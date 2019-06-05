package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"

	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
)

func main() {
	beam.Init()
	p := beam.NewPipeline()
	s := p.Root()
	//10 OMIT
	lines := textio.Read(s, "../../testdata/marchToMay.csv")
	cleaned := beam.ParDo(s, cleanFn, lines)
	csvRecs := beam.ParDo(s, csvFn, cleaned)
	stamped := beam.ParDo(s, &addTimestampFn{}, csvRecs)
	monthWindow := beam.WindowInto(s, window.NewFixedWindows(24*30*time.Hour), stamped)
	count := beam.ParDo(s, func(id string, rec []string, emit func(string, float32)) {
		cnt, err := strconv.Atoi(rec[3])
		if err != nil {
			return
		}
		emit(id, float32(cnt))
	}, monthWindow)
	avg := beam.CombinePerKey(s, meanFn, count)

	formatted := beam.ParDo(s, fmtFn, avg)
	// we need to merge, otherwise textio.Write will overwrite report.txt
	// for each window
	merged := beam.WindowInto(s, window.NewGlobalWindows(), formatted)
	textio.Write(s, "report.txt", merged)
	//20 OMIT
	if err := direct.Execute(context.Background(), p); err != nil {
		log.Printf("pipeline execution error: %v", err)
	}
}

//30 OMIT
func meanFn(a, b float32) float32 {
	return float32(a+b) / 2.0
}

//40 OMIT

type addTimestampFn struct{}

func (f *addTimestampFn) ProcessElement(id string, rec []string, emit func(beam.EventTime, string, []string)) {
	t, err := time.Parse("2006-01-02T15:04:05", rec[0])
	if err != nil {
		return

	}
	emit(mtime.FromTime(t), id, rec)
}

func csvFn(line string, emit func(id string, rec []string)) {
	cr := csv.NewReader(strings.NewReader(line))
	rec, err := cr.Read()
	if err != nil {
		return
	}
	emit(rec[1], rec)
}
func fmtFn(et beam.EventTime, id string, avg float32) string {
	return fmt.Sprintf("%s: %v (%s)", id, avg, time.Unix(et.Milliseconds()/1000, 0))
}

var idRE = regexp.MustCompile(`\w{3}\d{6}`)

func cleanFn(line string, emit func(string)) {
	if idRE.FindStringIndex(line) == nil {
		return
	}
	emit(line)
}
