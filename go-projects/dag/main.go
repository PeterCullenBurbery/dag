package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
	"github.com/PeterCullenBurbery/go_functions_002/v3/system_management_functions"
	"github.com/PeterCullenBurbery/go_functions_002/v3/math_functions"
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

	// Step 4: Reverse topologically sort the DAG
	execution_order, err := math_functions.Reverse_topological_sort(dag)
	if err != nil {
		log.Fatalf("❌ reverse_topological_sort_failed: %v", err)
	}

	// Step 5: Display the reverse order
	fmt.Println("🔁 reverse_topological_execution_order:")
	for i, task := range execution_order {
		fmt.Printf("%2d. %s\n", i+1, task)
	}
}