package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	// Mapping measurement IDs to names
	measurementMap = map[string]string{
		"1": "Temperature",
		"6": "Dewpoint",
		"5": "Windspeed",
		"4": "Wind Direction",
		"3": "Pressure",
	}
	// Required measurements
	requiredMeasurements = []string{"Temperature", "Dewpoint", "Windspeed", "Wind Direction", "Pressure"}
)

// Record represents a single data entry
type Record struct {
	StationID string
	Timestamp string
	Values    map[string]string
}

// parseRow interprets a single line of input data
func parseRow(line string) (Record, error) {
	fields := strings.Split(line, ",")
	if len(fields) < 7 {
		return Record{}, fmt.Errorf("invalid row: %s", line)
	}

	stationID := fields[1]
	time := fmt.Sprintf("%s:%s:%s", fields[2], fields[3], fields[4])
	date := fmt.Sprintf("%s-%s-%s", fields[5], fields[6], fields[7])
	timestamp := fmt.Sprintf("%sT%s", date, time)

	values := make(map[string]string)
	for i := 8; i < len(fields)-1; i += 3 {
		id := fields[i]
		measurementName := measurementMap[id]
		if measurementName != "" && i+2 < len(fields) {
			values[measurementName] = fields[i+2]
		}
	}

	return Record{StationID: stationID, Timestamp: timestamp, Values: values}, nil
}

// processFile processes an input file and appends valid records to the output
func processFile(filePath string, writer *csv.Writer, writeHeader bool) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("cannot open file %s: %v", filePath, err)
	}
	defer file.Close()

	if writeHeader {
		header := append([]string{"station_id", "timestamp"}, requiredMeasurements...)
		writer.Write(header)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "S,") {
			continue
		}

		record, err := parseRow(line)
		if err != nil {
			fmt.Printf("Skipping line due to error: %v\n", err)
			continue
		}

		row := []string{record.StationID, record.Timestamp}
		for _, param := range requiredMeasurements {
			row = append(row, record.Values[param])
		}

		writer.Write(row)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %v", filePath, err)
	}
	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <input_directory> <output_file>")
		return
	}

	inputDir := os.Args[1]
	outputFile := os.Args[2]

	out, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Cannot create output file: %v\n", err)
		return
	}
	defer out.Close()

	writer := csv.NewWriter(out)
	defer writer.Flush()

	writeHeader := true

	err = filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("cannot access %s: %v", path, err)
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".txt") {
			fmt.Printf("Processing file: %s\n", path)
			err := processFile(path, writer, writeHeader)
			if err != nil {
				fmt.Printf("Error processing file %s: %v\n", path, err)
			} else {
				writeHeader = false
			}
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking directory: %v\n", err)
	}
}
