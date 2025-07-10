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
	// Step 1: Convert blob URL to raw
	raw_url, err := system_management_functions.Convert_blob_to_raw_github_url(
		"https://github.com/PeterCullenBurbery/dag/blob/main/dag.yaml",
	)
	if err != nil {
		log.Fatalf("❌ url_conversion_failed: %v", err)
	}

	// Step 2: Download dag.yaml
	local_path := filepath.Join(os.TempDir(), "dag.yaml")
	err = system_management_functions.Download_file(local_path, raw_url)
	if err != nil {
		log.Fatalf("❌ download_failed: %v", err)
	}

	// Step 3: Load and parse YAML
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

	// Step 4: Compute recursive dependents with depth
	dependents_map, depth_map := get_recursive_dependents_and_depth(dag)

	// Step 5: Count and sort
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
			return stats[i].max_depth > stats[j].max_depth // deeper wins
		}
		return stats[i].count > stats[j].count
	})

	// Step 6: Output
	fmt.Println("📍 Nodes that are used as dependencies (recursively), sorted by depth and impact:")
	for _, entry := range stats {
		fmt.Printf("  - %s (%d dependents, max depth %d)\n", entry.node, entry.count, entry.max_depth)
	}
}

// get_recursive_dependents_and_depth returns both:
// - map[node] = all transitive dependents
// - map[node] = max depth from each node to its deepest dependent
func get_recursive_dependents_and_depth(dag map[string][]string) (map[string][]string, map[string]int) {
	// Build reverse graph: dependency -> list of dependents
	reverse := make(map[string][]string)
	for task, deps := range dag {
		for _, dep := range deps {
			reverse[dep] = append(reverse[dep], task)
		}
	}

	type result struct {
		seen map[string]bool
		depth int
	}

	cache := make(map[string]result)

	var visit func(string) result
	visit = func(node string) result {
		if cached, ok := cache[node]; ok {
			return cached
		}
		seen := make(map[string]bool)
		max_depth := 0
		for _, dependent := range reverse[node] {
			seen[dependent] = true
			res := visit(dependent)
			for sub := range res.seen {
				seen[sub] = true
			}
			if res.depth+1 > max_depth {
				max_depth = res.depth + 1
			}
		}
		cache[node] = result{seen: seen, depth: max_depth}
		return cache[node]
	}

	dependents := make(map[string][]string)
	depths := make(map[string]int)
	for node := range dag {
		res := visit(node)
		var list []string
		for dep := range res.seen {
			list = append(list, dep)
		}
		sort.Strings(list)
		dependents[node] = list
		depths[node] = res.depth
	}
	return dependents, depths
}
