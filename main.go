package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
)

func readAndParseCSV(filePath string, lines chan<- map[string]interface{}, estimatedTotalLines int, wg *sync.WaitGroup) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		close(lines)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	headers, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading CSV headers:", err)
		close(lines)
		return
	}

	bar := progressbar.Default(int64(estimatedTotalLines))

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Println("Error reading CSV record:", err)
			break
		}

		row := make(map[string]interface{})
		for i, value := range record {
			key := strings.ToLower(headers[i])
			row[key] = value
		}

		lines <- row

		bar.Add(1)
	}

	close(lines)
	bar.Finish()
}

func writeJSON(outputPath string, lines <-chan map[string]interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		fmt.Println("Error creating JSON file:", err)
		return
	}
	defer outputFile.Close()

	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")

	for line := range lines {
		if err := encoder.Encode(line); err != nil {
			fmt.Println("Error writing JSON:", err)
			return
		}
	}

}

func main() {
	args := os.Args
	fileIndex := -1
	outputIndex := -1

	for i, arg := range args {
		if arg == "--file" && i+1 < len(args) {
			fileIndex = i + 1
		} else if arg == "--output" && i+1 < len(args) {
			outputIndex = i + 1
		}
	}

	if fileIndex != -1 {
		filePath := args[fileIndex]
		outputPath := ""
		if outputIndex != -1 {
			outputPath = args[outputIndex]
		}

		startTime := time.Now()

		fmt.Println("Reading file...")
		fmt.Println("=================")

		estimatedTotalLines, err := evaluateTotalLines(filePath)
		if err != nil {
			fmt.Println("Error evaluating total lines:", err)
			return
		}

		fmt.Printf("Estimated total lines: %d\n", estimatedTotalLines)

		lines := make(chan map[string]interface{})

		var wg sync.WaitGroup

		wg.Add(1)
		go readAndParseCSV(filePath, lines, estimatedTotalLines, &wg)

		wg.Add(1)
		go writeJSON(outputPath, lines, &wg)

		wg.Wait()

		fmt.Println("Conversion complete!")
		endTime := time.Now()
		processTime := endTime.Sub(startTime).Seconds()
		fmt.Printf("File name: %s\n", filePath)
		fmt.Printf("Processing time: %.2f seconds\n", processTime)
	} else {
		fmt.Println("Please provide a file path using the --file argument.")
	}
}

func evaluateTotalLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Use a scanner to count the total number of lines
	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lineCount - 1, nil
}
