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

func main() {
	raw_url, err := system_management_functions.Convert_blob_to_raw_github_url(
		"https://github.com/PeterCullenBurbery/dag/blob/main/dag.yaml",
	)
	if err != nil {
		log.Fatalf("❌ url_conversion_failed: %v", err)
	}

	local_path := filepath.Join(os.TempDir(), "dag.yaml")
	err = system_management_functions.Download_file(local_path, raw_url)
	if err != nil {
		log.Fatalf("❌ download_failed: %v", err)
	}

	file_content, err := os.ReadFile(local_path)
	if err != nil {
		log.Fatalf("❌ file_read_failed: %v", err)
	}

	var parsed dag_file
	err = yaml.Unmarshal(file_content, &parsed)
	if err != nil {
		log.Fatalf("❌ yaml_parse_failed: %v", err)
	}
	dag := parsed.Dag

	// Step 4: Compute recursive dependents and level grouping
	dependents_map, depth_map, level_map := get_recursive_dependents_and_levels(dag)

	// Step 5: Rank
	type depStat struct {
		node      string
		count     int
		max_depth int
	}
	var stats []depStat
	for node, deps := range dependents_map {
		if len(deps) > 0 {
			stats = append(stats, depStat{node, len(deps), depth_map[node]})
		}
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].count == stats[j].count {
			return stats[i].max_depth > stats[j].max_depth
		}
		return stats[i].count > stats[j].count
	})

	// Step 6: Output
	fmt.Println("📍 Nodes that are used as dependencies (recursively), sorted by depth and impact:")
	for _, entry := range stats {
		fmt.Printf("\n🔧 %s (%d dependents, max depth %d)\n", entry.node, entry.count, entry.max_depth)
		levels := level_map[entry.node]
		var level_keys []int
		for lvl := range levels {
			level_keys = append(level_keys, lvl)
		}
		sort.Ints(level_keys)
		for _, lvl := range level_keys {
			sort.Strings(levels[lvl])
			fmt.Printf("\n  Level %d:\n", lvl)
			for _, dep := range levels[lvl] {
				fmt.Printf("    - %s\n", dep)
			}
		}
	}
}

// get_recursive_dependents_and_levels returns:
// - map[node] = list of all recursive dependents
// - map[node] = max depth from that node
// - map[node] = map[level] -> list of dependents at that level
func get_recursive_dependents_and_levels(dag map[string][]string) (map[string][]string, map[string]int, map[string]map[int][]string) {
	// Build reverse graph
	reverse := make(map[string][]string)
	for task, deps := range dag {
		for _, dep := range deps {
			reverse[dep] = append(reverse[dep], task)
		}
	}

	type result struct {
		seen  map[string]int // dependent -> depth
		depth int
	}
	cache := make(map[string]result)

	var visit func(string) result
	visit = func(node string) result {
		if cached, ok := cache[node]; ok {
			return cached
		}
		seen := make(map[string]int)
		max_depth := 0
		for _, dependent := range reverse[node] {
			seen[dependent] = 1
			res := visit(dependent)
			for sub, d := range res.seen {
				if prev, exists := seen[sub]; !exists || d+1 > prev {
					seen[sub] = d + 1
				}
			}
			if res.depth+1 > max_depth {
				max_depth = res.depth + 1
			}
		}
		cache[node] = result{seen, max_depth}
		return cache[node]
	}

	// Format results
	dependents := make(map[string][]string)
	depths := make(map[string]int)
	levels := make(map[string]map[int][]string)

	for node := range dag {
		res := visit(node)
		var list []string
		level_map := make(map[int][]string)
		for dep, lvl := range res.seen {
			list = append(list, dep)
			level_map[lvl] = append(level_map[lvl], dep)
		}
		sort.Strings(list)
		dependents[node] = list
		depths[node] = res.depth
		levels[node] = level_map
	}

	return dependents, depths, levels
}
