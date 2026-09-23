package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// fixture is one collected .rdb file in the repository.
type fixture struct {
	absPath string
	relPath string // slash-separated path relative to repo root
	version string // e.g. "redis-11", "valkey-80", "unknown"
	valkey  bool
}

// collectFixtures discovers every .rdb file below root, sorted by relative
// path for deterministic logs. The workDir subtree (temporary artifacts) is
// excluded when it lives inside root.
func collectFixtures(root, workDir string) ([]fixture, error) {
	var fixtures []fixture
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		name := info.Name()
		if info.IsDir() {
			if name == ".git" || name == "vendor" || name == "node_modules" {
				return filepath.SkipDir
			}
			if workDir != "" {
				if rel, relErr := filepath.Rel(path, workDir); relErr == nil && rel == "." {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.EqualFold(filepath.Ext(name), ".rdb") {
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			fixtures = append(fixtures, fixture{absPath: path, relPath: filepath.ToSlash(rel)})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("collect fixtures failed: %v", err)
	}
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].relPath < fixtures[j].relPath })
	for i := range fixtures {
		readFixtureVersion(&fixtures[i])
	}
	return fixtures, nil
}

// readFixtureVersion reads the RDB header to identify the RDB version.
func readFixtureVersion(fx *fixture) {
	fx.version = "unknown"
	data, err := os.ReadFile(fx.absPath)
	if err != nil {
		return
	}
	if len(data) < 9 {
		return
	}
	var magic string
	var versionString string
	if string(data[:5]) == "REDIS" {
		magic = "redis"
		versionString = string(data[5:9])
	} else if string(data[:6]) == "VALKEY" {
		magic = "valkey"
		versionString = string(data[6:9])
		fx.valkey = true
	} else {
		return
	}
	version, err := strconv.Atoi(versionString)
	if err != nil {
		return
	}
	fx.version = fmt.Sprintf("%s-%d", magic, version)
}
