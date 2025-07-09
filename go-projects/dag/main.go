package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
	"github.com/PeterCullenBurbery/go_functions_002/v3/system_management_functions"
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

	// Step 4: Topologically sort the DAG
	execution_order, err := topological_sort(dag)
	if err != nil {
		log.Fatalf("❌ topological_sort_failed: %v", err)
	}

	// Step 5: Display the order
	fmt.Println("✅ topological_execution_order:")
	for i, task := range execution_order {
		fmt.Printf("%2d. %s\n", i+1, task)
	}
}

func topological_sort(graph map[string][]string) ([]string, error) {
	in_degree := make(map[string]int)
	for node := range graph {
		in_degree[node] = 0
	}
	for _, deps := range graph {
		for _, dep := range deps {
			in_degree[dep]++
		}
	}

	var queue []string
	for node, degree := range in_degree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	var sorted []string
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		sorted = append(sorted, current)

		for _, neighbor := range graph[current] {
			in_degree[neighbor]--
			if in_degree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(sorted) != len(graph) {
		return nil, fmt.Errorf("cycle detected: only sorted %d of %d tasks", len(sorted), len(graph))
	}

	return sorted, nil
}