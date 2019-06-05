package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam"
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
	csvFmt := beam.ParDo(s, csvFmtFn, csvRecs)
	textio.Write(s, "report.txt", csvFmt)
	//20 OMIT

	if err := direct.Execute(context.Background(), p); err != nil {
		log.Printf("pipeline execution error: %v", err)
	}
}

//30 OMIT
func csvFn(line string, emit func(id string, rec []string)) {
	cr := csv.NewReader(strings.NewReader(line))
	rec, err := cr.Read()
	if err != nil {
		return
	}
	emit(rec[1], rec)
}

//40 OMIT
//50 OMIT
func csvFmtFn(id string, rec []string) string {
	return fmt.Sprintf("%s: %s", id, strings.Join(rec, "|"))
}

//60 OMIT
var idRE = regexp.MustCompile(`\w{3}\d{6}`)

func cleanFn(line string, emit func(string)) {
	if idRE.FindStringIndex(line) == nil {
		return
	}
	emit(line)
}
