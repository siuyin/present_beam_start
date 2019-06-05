package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"regexp"
	"strconv"
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
	count := beam.ParDo(s, func(id string, rec []string, emit func(string, int)) { // 1
		cnt, err := strconv.Atoi(rec[3])
		if err != nil {
			return
		}
		emit(id, cnt)
	}, csvRecs)
	latestCount := beam.CombinePerKey(s, latestCountCombineFn, count) // 2
	formatted := beam.ParDo(s, func(id string, cnt int) string {
		return fmt.Sprintf("%s,%d", id, cnt)
	}, latestCount)
	textio.Write(s, "output.csv", formatted)
	//20 OMIT

	if err := direct.Execute(context.Background(), p); err != nil {
		log.Printf("pipeline execution error: %v", err)
	}
}

//30 OMIT
func latestCountCombineFn(aCnt, bCnt int) int {
	// I'm ignoring the earlier aCnt value
	// and just returning the later bCnt value.
	return bCnt
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

var idRE = regexp.MustCompile(`\w{3}\d{6}`)

func cleanFn(line string, emit func(string)) {
	if idRE.FindStringIndex(line) == nil {
		return
	}
	emit(line)
}
