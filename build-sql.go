package buildsql

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	SP_BUILD_ORDER_FILE     = "SpBuildOrder.txt"
	BUILD_UPDATE_ORDER_FILE = "BuildUpdateOrder.txt"
	SP_LOG_FILE             = "SpBuildLog.txt"
	BUILD_ORDER_FILE        = "BuildUpdateOrder.txt"
	LOG_FILE                = "BuildUpdateLog.txt"
)

// FileError represents an error when processing a SQL file
type FileError struct {
	FileName    string
	Description string
	LineNumber  int
	TimeStamp   string
	Content     string
}

// BuildSqlUpdates processes SQL files according to a build order file
// Returns the SQL content as a string and any errors encountered
func BuildSqlUpdates(sourcePath string) (string, error) {
	startTime := time.Now()
	fmt.Println("Starting BuildSqlUpdates...")

	// Initialize variables
	var (
		databaseScriptsPath = filepath.Join(sourcePath, "")
		buildOrderFilePath  = filepath.Join(databaseScriptsPath, BUILD_ORDER_FILE)
		logFilePath         = filepath.Join(databaseScriptsPath, LOG_FILE)
		errorFlag           = false
		successCount        = 0
		allFiles            = make(map[string]bool)
		sqlOutputBuffer     strings.Builder
	)

	// Create log file
	logFile, err := os.Create(logFilePath)
	if err != nil {
		fmt.Printf("Error creating log file %s: %v\n", logFilePath, err)
		os.Exit(1)
	}
	defer logFile.Close()

	// Open build order file
	buildOrderFile, err := os.Open(buildOrderFilePath)
	if err != nil {
		fmt.Printf("Error opening build order file %s: %v\n", buildOrderFilePath, err)
		os.Exit(1)
	}
	defer buildOrderFile.Close()

	// Load all file paths into a map
	loadFileList(databaseScriptsPath, allFiles)

	// Process build order file sequentially
	scanner := bufio.NewScanner(buildOrderFile)
	spFileLine := 0
	for scanner.Scan() {
		spFile := strings.TrimSpace(scanner.Text())
		spFileLine++

		if len(spFile) > 0 && !strings.HasPrefix(spFile, "'") {
			// Process each file sequentially
			fileName := spFile
			line := spFileLine

			// Check if file exists
			_, statErr := os.Stat(fileName)
			if statErr != nil {
				fmt.Printf("Error: File not found: %s (line %d): %v\n",
					fileName, line, statErr)
				errorFlag = true
				continue
			}

			// Try to open the file
			fileContent, err := os.ReadFile(fileName)
			if err != nil {
				fmt.Printf("Error reading file: %s (line %d): %v\n",
					fileName, line, err)
				errorFlag = true
				continue
			}

			// Create SQL content with header and print statement
			header := createHeader(fileName)
			printStatement := createPrintStatement(fileName)

			// Write to output buffer with proper spacing
			sqlOutputBuffer.WriteString("\n")
			sqlOutputBuffer.WriteString(header)
			sqlOutputBuffer.WriteString("\n\n")
			sqlOutputBuffer.WriteString(printStatement)
			sqlOutputBuffer.WriteString("\n\n")

			// Write the file content
			_, writeErr := sqlOutputBuffer.WriteString(string(fileContent))
			if writeErr != nil {
				fmt.Printf("Error writing content to buffer: %s (line %d): %v\n",
					fileName, line, writeErr)
				errorFlag = true
				continue
			}

			// Mark file as processed
			delete(allFiles, fileName)
			successCount++
		}
	}

	// Write final message to output buffer
	sqlOutputBuffer.WriteString("\n")
	sqlOutputBuffer.WriteString("print 'Schema Updates Completed...'\n")

	// Write to log file
	logWriter := bufio.NewWriter(logFile)
	if errorFlag {
		msg := "Errors occurred, check log file!\n"
		logWriter.WriteString(msg)
	}
	logWriter.WriteString(fmt.Sprintf("Success Count: %d\n", successCount))

	// Report extra files that weren't in the build list
	logWriter.WriteString("\nExtra File Report:\n")
	extraCount := 0
	for file := range allFiles {
		logWriter.WriteString(fmt.Sprintf("%s\n", file))
		extraCount++
	}
	logWriter.WriteString(fmt.Sprintf("Extra File Count: %d\n", extraCount))
	logWriter.Flush()

	elapsedTime := time.Since(startTime)
	fmt.Printf("BuildSqlUpdates completed in %v\n", elapsedTime)
	fmt.Printf("Success Count: %d\n", successCount)

	if errorFlag {
		fmt.Println("Errors occurred, check log file!")
		return sqlOutputBuffer.String(), fmt.Errorf("SQL update errors occurred, check log file: %s", logFilePath)
	}

	return sqlOutputBuffer.String(), nil
}

// BuildSqlProcs processes SQL procedure files according to build order files
// Returns the combined SQL output as a string
func BuildSqlProcs(sourcePath string) string {
	startTime := time.Now()
	fmt.Println("Starting BuildSqlProcs...")

	// Initialize variables
	var (
		databaseScriptsPath      = filepath.Join(sourcePath, "")
		spBuildOrderFilePath     = filepath.Join(databaseScriptsPath, SP_BUILD_ORDER_FILE)
		buildUpdateOrderFilePath = filepath.Join(databaseScriptsPath, BUILD_UPDATE_ORDER_FILE)
		logFilePath              = filepath.Join(databaseScriptsPath, SP_LOG_FILE)
		errorFlag                = false
		successCount             = 0
		allFiles                 = make(map[string]bool)
		sqlOutputBuffer          strings.Builder
	)

	// Create log file
	logFile, err := os.Create(logFilePath)
	if err != nil {
		fmt.Printf("Error creating log file %s: %v\n", logFilePath, err)
		os.Exit(1)
	}
	defer logFile.Close()

	// Load all file paths into a map
	loadFileList(databaseScriptsPath, allFiles)

	// Process both build order files sequentially
	processBuildOrderFile(
		spBuildOrderFilePath,
		&sqlOutputBuffer,
		logFile,
		allFiles,
		nil, // No need for waitgroup
		nil, // No need for mutex
		databaseScriptsPath,
		true, // Include server settings for SPs
	)

	processBuildOrderFile(
		buildUpdateOrderFilePath,
		&sqlOutputBuffer,
		logFile,
		allFiles,
		nil, // No need for waitgroup
		nil, // No need for mutex
		databaseScriptsPath,
		false, // Don't include server settings for updates
	)

	// Write final message to output buffer
	sqlOutputBuffer.WriteString("\n")
	sqlOutputBuffer.WriteString("print 'Stored Procedures Created Successfully...'\n")

	// Write success count
	if errorFlag {
		msg := "Errors occurred, check log file!\n"
		logFile.WriteString(msg)
	}
	logFile.WriteString(fmt.Sprintf("Success Count: %d\n", successCount))

	// Report extra files that weren't in the build list
	logFile.WriteString("\nExtra File Report:\n")
	extraCount := 0
	for file := range allFiles {
		logFile.WriteString(fmt.Sprintf("%s\n", file))
		extraCount++
	}
	logFile.WriteString(fmt.Sprintf("Extra File Count: %d\n", extraCount))

	elapsedTime := time.Since(startTime)
	fmt.Printf("BuildSqlProcs completed in %v\n", elapsedTime)
	fmt.Printf("Success Count: %d\n", successCount)
	if errorFlag {
		fmt.Println("Errors occurred, check log file!")
	}

	// Return the generated SQL content directly
	return sqlOutputBuffer.String()
}

// processBuildOrderFile processes SQL files according to a build order
func processBuildOrderFile(
	buildOrderFilePath string,
	sqlOutputBuffer *strings.Builder,
	logFile *os.File,
	allFiles map[string]bool,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	databaseScriptsPath string,
	includeServerSettings bool,
) {
	// Open build order file
	buildOrderFile, err := os.Open(buildOrderFilePath)
	if err != nil {
		fmt.Printf("Error opening build order file %s: %v\n", buildOrderFilePath, err)
		return
	}
	defer buildOrderFile.Close()

	// Write server settings if required
	if includeServerSettings {
		appendMiscSettings(sqlOutputBuffer, databaseScriptsPath)
	}

	// Process build order file
	scanner := bufio.NewScanner(buildOrderFile)
	spFileLine := 0

	// Process each line in the build order file
	for scanner.Scan() {
		spFile := strings.TrimSpace(scanner.Text())
		spFileLine++

		if len(spFile) > 0 && !strings.HasPrefix(spFile, "'") {
			// Prepare full path for file if not absolute
			if !filepath.IsAbs(spFile) {
				spFile = filepath.Join(databaseScriptsPath, spFile)
			}

			// Check if file exists
			_, statErr := os.Stat(spFile)
			if statErr != nil {
				// Just log the error to console
				fmt.Printf("Error: File not found: %s (line %d): %v\n",
					spFile, spFileLine, statErr)
				continue
			}

			// Read the file content
			fileContent, err := os.ReadFile(spFile)
			if err != nil {
				fmt.Printf("Error reading file: %s (line %d): %v\n",
					spFile, spFileLine, err)
				continue
			}

			// Create header and print statement
			var header, printStatement string

			if includeServerSettings {
				header = createHeader(spFile)
				printStatement = createStoredProcPrint(spFile)
			} else {
				header = createHeader(spFile)
				printStatement = createPrintStatement(spFile)
			}

			// Write to output buffer with proper spacing
			sqlOutputBuffer.WriteString("\n")
			sqlOutputBuffer.WriteString(header)
			sqlOutputBuffer.WriteString("\n\n")
			sqlOutputBuffer.WriteString(printStatement)
			sqlOutputBuffer.WriteString("\n\n")

			// Write content
			if _, err := sqlOutputBuffer.WriteString(string(fileContent)); err != nil {
				fmt.Printf("Error writing content to buffer: %s (line %d): %v\n",
					spFile, spFileLine, err)
				continue
			}

			// Mark file as processed
			delete(allFiles, spFile)
		}
	}
}

// createStoredProcPrint creates a print statement for each stored procedure
func createStoredProcPrint(fileName string) string {
	// Extract the procedure name from the file path
	lastSep := strings.LastIndex(fileName, "\\")
	procName := fileName
	if lastSep >= 0 {
		procName = fileName[lastSep+1:]
	}
	procName = strings.TrimSuffix(procName, ".sql")

	return fmt.Sprintf("PRINT 'Creating %s...'", procName)
}

// appendMiscSettings adds server settings before SP definitions
func appendMiscSettings(sqlOutputBuffer io.Writer, databaseScriptsPath string) {
	settingsFilePath := filepath.Join(databaseScriptsPath, "schema", "SetMiscServerSettings.txt")

	// Try to read the settings file
	settingsContent, err := os.ReadFile(settingsFilePath)
	if err != nil {
		// If settings file doesn't exist, provide default settings
		defaultSettings := `
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
`
		sqlOutputBuffer.Write([]byte(defaultSettings))
		return
	}

	// Write the settings to the output buffer
	sqlOutputBuffer.Write(settingsContent)
}

// createHeader creates a SQL comment header for a file
func createHeader(fileName string) string {
	return fmt.Sprintf("-- File: %s", fileName)
}

// createPrintStatement creates a print statement for each file
func createPrintStatement(fileName string) string {
	// Extract the procedure name from the file path
	lastSep := strings.LastIndex(fileName, "\\")
	procName := fileName
	if lastSep >= 0 {
		procName = fileName[lastSep+1:]
	}
	procName = strings.TrimSuffix(procName, ".sql")

	return fmt.Sprintf("PRINT 'Updating %s...'", procName)
}

// loadFileList loads all SQL files into the map
func loadFileList(currentDir string, allFiles map[string]bool) {
	filepath.Walk(currentDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip version control files
		if info.Name() == "vssver.scc" {
			return nil
		}

		// Process SQL files only
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".sql") {
			// Convert to relative path
			relPath, err := filepath.Rel(currentDir, path)
			if err == nil {
				// Use backslashes for Windows paths to match VBS behavior
				relPath = strings.ReplaceAll(relPath, "/", "\\")
				allFiles[relPath] = true
			}
		}
		return nil
	})
}
