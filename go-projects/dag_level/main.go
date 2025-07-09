package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

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

	// Step 4: Compute levels
	levels := compute_levels(dag)

	// Step 5: Display sorted results
	var tasks []string
	for task := range levels {
		tasks = append(tasks, task)
	}
	sort.Strings(tasks)

	fmt.Println("📊 DAG Levels:")
	for _, task := range tasks {
		fmt.Printf("Level %d: %s\n", levels[task], task)
	}
}

func compute_levels(dag map[string][]string) map[string]int {
	cache := make(map[string]int)

	var level_of func(string) int
	level_of = func(task string) int {
		if lvl, ok := cache[task]; ok {
			return lvl
		}
		deps := dag[task]
		if len(deps) == 0 {
			cache[task] = 1
			return 1
		}
		max_level := 0
		for _, dep := range deps {
			l := level_of(dep)
			if l > max_level {
				max_level = l
			}
		}
		cache[task] = max_level + 1
		return cache[task]
	}

	for task := range dag {
		level_of(task)
	}

	return cache
}