package main

import (
	"fmt"
	"strings"
)

// runPipeline executes decode1 -> encode -> decode2 -> compare -> aof for one
// fixture. The first failing stage stops the pipeline; every stage result is
// independent and reported per fixture.
func runPipeline(fx *fixture, tmpDir string) *fixtureResult {
	res := &fixtureResult{fx: fx}

	fail := func(stage string, err error) *fixtureResult {
		res.stage = stage
		res.err = err
		return res
	}

	d1 := fx.decoded
	if d1 != nil {
		res.features = d1.sortedFeatures()
	}
	if fx.decodeErr != nil {
		return fail(stageDecode1, fx.decodeErr)
	}

	encRes, err := reencode(d1, fx.magic == "VALKEY", tmpDir, safeTempName(fx.relPath))
	if err != nil {
		return fail(stageEncode, err)
	}
	res.losses = append(res.losses, encRes.losses...)

	d2, err := decodeFixture(encRes.path)
	if err != nil {
		return fail(stageDecode2, err)
	}

	diffs := compareDecodings(d1, d2)
	losses, failures := classifyDiffs(diffs)
	res.losses = append(res.losses, losses...)
	if len(failures) > 0 {
		strs := make([]string, 0, len(failures))
		for _, d := range failures {
			strs = append(strs, d.String())
		}
		return fail(stageCompare, fmt.Errorf("%d semantic differences: %s",
			len(failures), strings.Join(strs, "; ")))
	}

	if err := runAOFStage(fx, d1, tmpDir); err != nil {
		return fail(stageAOF, err)
	}
	return res
}

// collectTypes lists the distinct data types found in a decoded fixture.
func collectTypes(d *decodedFixture) []string {
	seen := map[string]bool{}
	var types []string
	for _, obj := range d.objects {
		t := obj.GetType()
		if !seen[t] {
			seen[t] = true
			types = append(types, t)
		}
	}
	return types
}
