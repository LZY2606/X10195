package rdbverify

import "sort"

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedInts(in []int) []int {
	sort.Ints(in)
	return in
}

func sortedStrings(in []string) []string {
	sort.Strings(in)
	return in
}
