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
	// Mapping measurement ID and processing ID to parameter names
	measurementMap = map[string]map[string]string{
		"1":  {"2": "Temperature_Avg", "3": "Temperature_Min", "4": "Temperature_Max"},
		"2":  {"2": "Humidity_Avg", "3": "Humidity_Min", "4": "Humidity_Max"},
		"9":  {"2": "Windspeed_Avg", "3": "Windspeed_Min", "4": "Windspeed_Max"},
		"4":  {"2": "Wind Direction_Avg", "3": "Wind Direction_Min", "4": "Wind Direction_Max"},
		"13": {"2": "Pressure_Avg", "3": "Pressure_Min", "4": "Pressure_Max"},
	}
	// Required measurements
	requiredMeasurements = []string{"Temperature_Avg", "Humidity_Avg", "Windspeed_Avg", "Wind Direction_Avg", "Pressure_Avg"}
)

// Record represents a single data entry
type Record struct {
	StationID string
	Timestamp string
	Values    map[string]string
}

// zeroPad ensures single-digit numbers are padded with a leading zero
func zeroPad(num string) string {
	if len(num) == 1 {
		return "0" + num
	}
	return num
}

// parseRow interprets a single line of input data
func parseRow(line string) (Record, error) {
	fields := strings.Split(line, ",")
	if len(fields) < 7 {
		return Record{}, fmt.Errorf("invalid row: %s", line)
	}

	stationID := strings.TrimLeft(fields[1], "0") // Remove leading zeros from the station ID
	hour := zeroPad(fields[2])
	minute := zeroPad(fields[3])
	second := zeroPad(fields[4])
	day := zeroPad(fields[5])
	month := zeroPad(fields[6])
	year := fields[7]
	timestamp := fmt.Sprintf("%s-%s-%sT%s:%s:%s", year, month, day, hour, minute, second)

	values := make(map[string]string)
	for i := 8; i < len(fields)-1; i += 3 {
		measurementID := fields[i]
		processingID := fields[i+1]
		measurementName := ""
		if processingMap, exists := measurementMap[measurementID]; exists {
			if name, ok := processingMap[processingID]; ok {
				measurementName = name
			}
		}
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
