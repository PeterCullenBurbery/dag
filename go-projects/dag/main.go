package main

import (
	"fmt"
)

// Your DAG from before
var dag = map[string][]string{
	// Base installs
	"install choco":              {},
	"install powershell 7":       {},
	"install vs code":            {},
	"install 7 zip":              {},
	"install voidtools everything": {},
	"install WinSCP":             {},
	"install miniconda":          {},

	// Depends on choco
	"install mobaxterm":         {"install choco"},
	"install go":                {"install choco"},
	"install notepad++":         {"install choco"},
	"install sqlitebrowser":     {"install choco"},
	"install java":              {"install choco"},
	"install sharex":            {"install choco"},

	// Depends on java
	"install cherry-tree":       {"install java"},
	"install sql developer":     {"install java"},
	"install nirsoft":           {"install java"},
	"install sys-internals":     {"install java"},

	// UI Customizations
	"set dark mode":             {},
	"set start menu to left":    {},
	"show file extensions":      {},
	"show hidden files":         {},
	"hide search box":           {},
	"show seconds in taskbar":   {},
	"set short date pattern":    {},
	"set long date pattern":     {},
	"set time pattern":          {},
	"set 24 hour format":        {},
	"set first day of week Monday": {},

	// VS Code Config
	"configure keyboard shortcuts for vs code": {"install vs code"},
	"configure settings for vs code":           {"install vs code"},
	"configure settings for windows terminal":  {"install powershell 7"},
	"set windows terminal as default terminal application": {},

	// VS Code extensions
	"install golang.go":                         {"install go"},
	"install ms-python.debugpy":                 {"install miniconda"},
	"install ms-python.python":                  {"install miniconda"},
	"install ms-python.vscode-pylance":          {"install miniconda"},
	"install ms-vscode.powershell":              {"install powershell 7"},
	"install redhat.java":                       {"install java"},
	"install vscjava.vscode-gradle":             {"install java"},
	"install vscjava.vscode-java-debug":         {"install java"},
	"install vscjava.vscode-java-dependency":    {"install java"},
	"install vscjava.vscode-java-pack":          {"install java"},
	"install vscjava.vscode-java-test":          {"install java"},
	"install vscjava.vscode-maven":              {"install java"},
	"install tomoki1207.pdf":                    {},
	"install visualstudioexptteam.intellicode-api-usage-examples": {},
	"install visualstudioexptteam.vscodeintellicode":              {},

	// Executables
	"run powershell_modules.exe":     {},
	"run powershell_005_profile.exe": {},
	"run powershell_007_profile":     {"install powershell 7"},
	"run pin_vs_code_to_taskbar.exe": {"install vs code"},
}

// TopologicalSort performs Kahn’s algorithm on a DAG
func TopologicalSort(graph map[string][]string) ([]string, error) {
	// Count incoming edges for each node
	inDegree := make(map[string]int)
	for node := range graph {
		inDegree[node] = 0
	}
	for _, deps := range graph {
		for _, dep := range deps {
			inDegree[dep]++
		}
	}

	// Collect all nodes with in-degree 0
	var queue []string
	for node, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, node)
		}
	}

	var sorted []string
	for len(queue) > 0 {
		// Pop from front of queue
		node := queue[0]
		queue = queue[1:]
		sorted = append(sorted, node)

		// Decrease in-degree of children
		for _, neighbor := range graph[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	// If not all nodes were visited, cycle exists
	if len(sorted) != len(graph) {
		return nil, fmt.Errorf("cycle detected: only sorted %d of %d tasks", len(sorted), len(graph))
	}

	return sorted, nil
}

func main() {
	order, err := TopologicalSort(dag)
	if err != nil {
		fmt.Println("❌ Error:", err)
		return
	}

	fmt.Println("✅ Topological execution order:")
	for i, task := range order {
		fmt.Printf("%2d. %s\n", i+1, task)
	}
}