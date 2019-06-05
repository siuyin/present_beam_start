package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"

	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
)

func main() {
	beam.Init()
	p := beam.NewPipeline()
	s := p.Root()
	//10 OMIT
	lines := textio.Read(s, "../../testdata/inventory*.csv")
	cleaned := beam.ParDo(s, cleanFn, lines)
	csvRecs := beam.ParDo(s, csvFn, cleaned)
	stamped := beam.ParDo(s, &addTimestampFn{}, csvRecs) // HL
	formatted := beam.ParDo(s, csvFmtFn, stamped)
	textio.Write(s, "report.txt", formatted)
	//20 OMIT
	if err := direct.Execute(context.Background(), p); err != nil {
		log.Printf("pipeline execution error: %v", err)
	}
}

//30 OMIT
type addTimestampFn struct{}

func (f *addTimestampFn) ProcessElement(id string, rec []string,
	emit func(beam.EventTime, string, []string)) {

	t, err := time.Parse("2006-01-02T15:04:05", rec[0])
	if err != nil {
		return

	}
	emit(mtime.FromTime(t), id, rec)
}

//40 OMIT

func csvFn(line string, emit func(id string, rec []string)) {
	cr := csv.NewReader(strings.NewReader(line))
	rec, err := cr.Read()
	if err != nil {
		return
	}
	emit(rec[1], rec)
}

//50 OMIT
func csvFmtFn(et beam.EventTime, id string, rec []string) string {
	return fmt.Sprintf("%s: %s (%s)", id,
		strings.Join(rec, "|"), time.Unix(et.Milliseconds()/1000, 0))
}

//60 OMIT

var idRE = regexp.MustCompile(`\w{3}\d{6}`)

func cleanFn(line string, emit func(string)) {
	if idRE.FindStringIndex(line) == nil {
		return
	}
	emit(line)
}
