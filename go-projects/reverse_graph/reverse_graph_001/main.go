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

	// Step 4: Compute recursive dependents
	dependents_map := get_recursive_dependents(dag)

	// Step 5: Count and sort
	type depCount struct {
		node  string
		count int
	}
	var counts []depCount
	for node, deps := range dependents_map {
		if len(deps) > 0 {
			counts = append(counts, depCount{node, len(deps)})
		}
	}
	sort.Slice(counts, func(i, j int) bool {
		return counts[i].count > counts[j].count
	})

	// Step 6: Output
	fmt.Println("📍 Nodes that are used as dependencies (recursively):")
	for _, entry := range counts {
		fmt.Printf("  - %s (%d)\n", entry.node, entry.count)
	}
}

// get_recursive_dependents returns a map of node -> list of tasks that depend on it (recursively)
func get_recursive_dependents(dag map[string][]string) map[string][]string {
	// Build reverse graph: dependency -> list of dependents
	reverse := make(map[string][]string)
	for task, deps := range dag {
		for _, dep := range deps {
			reverse[dep] = append(reverse[dep], task)
		}
	}

	cache := make(map[string]map[string]bool)

	var visit func(string) map[string]bool
	visit = func(node string) map[string]bool {
		if cached, ok := cache[node]; ok {
			return cached
		}
		seen := make(map[string]bool)
		for _, dependent := range reverse[node] {
			seen[dependent] = true
			for sub := range visit(dependent) {
				seen[sub] = true
			}
		}
		cache[node] = seen
		return seen
	}

	result := make(map[string][]string)
	for node := range dag {
		seen := visit(node)
		var list []string
		for dep := range seen {
			list = append(list, dep)
		}
		sort.Strings(list)
		result[node] = list
	}
	return result
}