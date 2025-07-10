package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/PeterCullenBurbery/go_functions_002/v3/system_management_functions"
	"gopkg.in/yaml.v3"
)

type dag_file struct {
	Dag map[string][]string `yaml:"dag"`
}

type result struct {
	seen  map[string]int          // dependent -> depth
	depth int                     // max depth
	bylvl map[int][]string        // level -> dependents
	all   []string                // lexically sorted flat list
}

func main() {
	// Step 1: Convert blob URL to raw
	raw_url, err := system_management_functions.Convert_blob_to_raw_github_url(
		"https://github.com/PeterCullenBurbery/dag/blob/main/dag.yaml",
	)
	if err != nil {
		log.Fatalf("❌ url_conversion_failed: %v", err)
	}

	// Step 2: Download DAG
	local_path := filepath.Join(os.TempDir(), "dag.yaml")
	err = system_management_functions.Download_file(local_path, raw_url)
	if err != nil {
		log.Fatalf("❌ download_failed: %v", err)
	}

	// Step 3: Parse YAML
	content, err := os.ReadFile(local_path)
	if err != nil {
		log.Fatalf("❌ file_read_failed: %v", err)
	}

	var parsed dag_file
	err = yaml.Unmarshal(content, &parsed)
	if err != nil {
		log.Fatalf("❌ yaml_parse_failed: %v", err)
	}
	dag := parsed.Dag

	// Step 4: Analyze DAG
	stats := analyze_dag(dag)

	// Step 5: Output
	fmt.Println("\n📍 Nodes that are used as dependencies (recursively), sorted by depth and impact:")
	for _, stat := range stats {
		fmt.Printf("\n🔧 %s (%d dependents, max depth %d)\n", stat.name, stat.count, stat.max_depth)
		var levels []int
		for lvl := range stat.dependents_by_lvl {
			levels = append(levels, lvl)
		}
		sort.Ints(levels)
		for _, lvl := range levels {
			fmt.Printf("\n  Level %d:\n", lvl)
			for _, dep := range stat.dependents_by_lvl[lvl] {
				fmt.Printf("    - %s\n", dep)
			}
		}
	}
}

type stats_entry struct {
	name              string
	count             int
	max_depth         int
	dependents_by_lvl map[int][]string
	all_sorted        []string
}

func analyze_dag(dag map[string][]string) []stats_entry {
	// Step A: Reverse graph
	reverse := make(map[string][]string)
	for node, deps := range dag {
		for _, dep := range deps {
			reverse[dep] = append(reverse[dep], node)
		}
	}

	// Step B: Memoized recursive traversal
	cache := make(map[string]result)
	var visit func(string) result
	visit = func(node string) result {
		if val, ok := cache[node]; ok {
			return val
		}
		seen := make(map[string]int)
		bylvl := make(map[int][]string)
		max_depth := 0

		var recurse func(string, int)
		recurse = func(n string, depth int) {
			for _, dep := range reverse[n] {
				if prev_depth, exists := seen[dep]; exists && depth >= prev_depth {
					continue
				}
				seen[dep] = depth
				bylvl[depth] = append(bylvl[depth], dep)
				if depth > max_depth {
					max_depth = depth
				}
				recurse(dep, depth+1)
			}
		}
		recurse(node, 1)

		for _, level := range bylvl {
			sort.Strings(level)
		}
		all := make([]string, 0, len(seen))
		for dep := range seen {
			all = append(all, dep)
		}
		sort.Strings(all)

		res := result{seen: seen, depth: max_depth, bylvl: bylvl, all: all}
		cache[node] = res
		return res
	}

	// Step C: Build stats
	var entries []stats_entry
	for node := range dag {
		res := visit(node)
		if len(res.seen) == 0 {
			continue
		}
		entries = append(entries, stats_entry{
			name:              node,
			count:             len(res.seen),
			max_depth:         res.depth,
			dependents_by_lvl: res.bylvl,
			all_sorted:        res.all,
		})
	}

	// Step D: Sort by count, depth, lex
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.count != b.count {
			return a.count > b.count
		}
		if a.max_depth != b.max_depth {
			return a.max_depth > b.max_depth
		}
		return compare_lexicographic(a.all_sorted, b.all_sorted)
	})

	return entries
}

func compare_lexicographic(a, b []string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return len(a) < len(b)
}
