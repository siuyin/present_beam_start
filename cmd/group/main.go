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
	lines := textio.Read(s, "../../testdata/inventory*.csv")
	cleaned := beam.ParDo(s, cleanFn, lines)
	invLvl := beam.ParDo(s, cleanStruct{}, cleaned)

	sum := beam.CombinePerKey(s, func(aCnt, bCnt int) int {
		return aCnt + bCnt
	}, invLvl)
	sumFmt := beam.ParDo(s, func(id string, cnt int) string {
		return fmt.Sprintf("%s,%d", id, cnt)
	}, sum)
	textio.Write(s, "sum.csv", sumFmt)

	latest := beam.CombinePerKey(s, func(aCnt, bCnt int) int {
		return bCnt // return the latest value
	}, invLvl)
	formatted := beam.ParDo(s, func(id string, cnt int) string {
		return fmt.Sprintf("%s,%d", id, cnt)
	}, latest)
	textio.Write(s, "output.csv", formatted)

	if err := direct.Execute(context.Background(), p); err != nil {
		log.Printf("pipeline execution error: %v", err)
	}
}

type cleanStruct struct{}

func (cs cleanStruct) ProcessElement(line string, emit func(string, int)) {
	cr := csv.NewReader(strings.NewReader(line))
	rec, err := cr.Read()
	if err != nil || len(rec) == 0 {
		return
	}
	cnt, err := strconv.Atoi(rec[3])
	if err != nil {
		return
	}
	emit(rec[1], cnt)
}

var idRE = regexp.MustCompile(`\w{3}\d{6}`)

func cleanFn(line string, emit func(string)) {
	if idRE.FindStringIndex(line) == nil {
		return
	}
	emit(line)
}
