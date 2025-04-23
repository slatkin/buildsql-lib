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
	DATABASE_SCRIPTS_PATH   = ""
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
		databaseScriptsPath = filepath.Join(sourcePath, DATABASE_SCRIPTS_PATH)
		buildOrderFilePath  = filepath.Join(databaseScriptsPath, BUILD_ORDER_FILE)
		logFilePath         = filepath.Join(databaseScriptsPath, LOG_FILE)
		errorFlag           = false
		successCount        = 0
		spFileLine          = 0
		errorChan           = make(chan FileError, 1000)
		resultChan          = make(chan bool, 1000)
		allFiles            = make(map[string]bool)
		wg                  sync.WaitGroup
		mu                  sync.Mutex      // For synchronized writes to output buffer
		sqlOutputBuffer     strings.Builder // Buffer to store SQL content instead of file
		errorMessages       []string        // To collect error messages
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

	// Process build order file
	scanner := bufio.NewScanner(buildOrderFile)
	for scanner.Scan() {
		spFile := strings.TrimSpace(scanner.Text())
		spFileLine++

		if len(spFile) > 0 && !strings.HasPrefix(spFile, "'") {
			wg.Add(1)
			go func(fileName string, line int) {
				defer wg.Done()

				// Check if file exists
				_, statErr := os.Stat(fileName)
				if statErr != nil {
					errorChan <- FileError{
						FileName:    fileName,
						Description: fmt.Sprintf("File not found: %v", statErr),
						LineNumber:  line,
						TimeStamp:   time.Now().Format("2006-01-02 15:04:05"),
					}
					resultChan <- false
					return
				}

				// Try to open the file
				fileContent, err := os.ReadFile(fileName)
				if err != nil {
					errorChan <- FileError{
						FileName:    fileName,
						Description: err.Error(),
						LineNumber:  line,
						TimeStamp:   time.Now().Format("2006-01-02 15:04:05"),
					}
					resultChan <- false
					return
				}

				// Create SQL content with header and print statement
				header := createHeader(fileName)
				printStatement := createPrintStatement(fileName) // Note: Using createPrintStatement for updates

				// Lock for synchronized writing to output buffer
				mu.Lock()
				defer mu.Unlock()

				// Write to output buffer with proper spacing
				sqlOutputBuffer.WriteString("\n")
				sqlOutputBuffer.WriteString(header)
				sqlOutputBuffer.WriteString("\n\n")
				sqlOutputBuffer.WriteString(printStatement)
				sqlOutputBuffer.WriteString("\n\n")

				// Write the file content
				_, writeErr := sqlOutputBuffer.WriteString(string(fileContent))
				if writeErr != nil {
					errorChan <- FileError{
						FileName:    fileName,
						Description: writeErr.Error(),
						LineNumber:  line,
						TimeStamp:   time.Now().Format("2006-01-02 15:04:05"),
						Content:     string(fileContent),
					}
					resultChan <- false
					return
				}

				// Mark file as processed
				delete(allFiles, fileName)
				resultChan <- true
			}(spFile, spFileLine)
		}
	}

	// Wait for all goroutines to complete and close channels
	go func() {
		wg.Wait()
		close(errorChan)
		close(resultChan)
	}()

	// Count successful operations
	for result := range resultChan {
		if result {
			successCount++
		} else {
			errorFlag = true
		}
	}

	// Write final message to output buffer
	sqlOutputBuffer.WriteString("\n")
	sqlOutputBuffer.WriteString("print 'Schema Updates Completed...'\n")

	// Write errors to log file and collect error messages
	logWriter := bufio.NewWriter(logFile)
	errorCount := 0
	for err := range errorChan {
		// Write to log file
		logWriter.WriteString(fmt.Sprintf("File Name: %s\n", err.FileName))
		logWriter.WriteString(fmt.Sprintf("Err Message: %s\n", err.Description))
		if err.LineNumber > 0 {
			logWriter.WriteString(fmt.Sprintf("Build File Line: %d\n", err.LineNumber))
		}
		logWriter.WriteString(fmt.Sprintf("Time Stamp: %s\n", err.TimeStamp))
		if err.Content != "" {
			logWriter.WriteString(fmt.Sprintf("Attempted to write: %s\n", err.Content))
		}
		logWriter.WriteString("\n")

		// Collect error messages for return
		errorCount++
		if errorCount <= 10 { // Limit number of errors included in the return
			errorMessages = append(errorMessages, fmt.Sprintf("Error in file %s: %s", err.FileName, err.Description))
		}
	}

	// Write success count
	var msg string
	if errorFlag {
		msg = "Errors occurred, check log file!\n"
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
		errorSummary := fmt.Sprintf("%d SQL update errors occurred", errorCount)
		if len(errorMessages) > 0 {
			errorSummary += ":\n- " + strings.Join(errorMessages, "\n- ")
			if errorCount > len(errorMessages) {
				errorSummary += fmt.Sprintf("\n(and %d more errors, see log file: %s)",
					errorCount-len(errorMessages), logFilePath)
			}
		}
		return sqlOutputBuffer.String(), fmt.Errorf(errorSummary)
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
		errorChan                = make(chan FileError, 1000)
		resultChan               = make(chan bool, 1000)
		allFiles                 = make(map[string]bool)
		wg                       sync.WaitGroup
		mu                       sync.Mutex
		sqlOutputBuffer          strings.Builder // Buffer to store SQL content instead of file
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

	// Process main build order file
	processedCount1, hasErrors1 := processBuildOrderFile(
		spBuildOrderFilePath,
		&sqlOutputBuffer, // Use string builder instead of file
		logFile,
		allFiles,
		&wg,
		&mu,
		errorChan,
		resultChan,
		databaseScriptsPath,
		true, // Include server settings for SPs
	)

	// Process update order file
	processedCount2, hasErrors2 := processBuildOrderFile(
		buildUpdateOrderFilePath,
		&sqlOutputBuffer, // Use string builder instead of file
		logFile,
		allFiles,
		&wg,
		&mu,
		errorChan,
		resultChan,
		databaseScriptsPath,
		false, // Don't include server settings for updates
	)

	// Wait for all goroutines to complete and close channels
	go func() {
		wg.Wait()
		close(errorChan)
		close(resultChan)
	}()

	// Count successful operations
	for result := range resultChan {
		if result {
			successCount++
		} else {
			errorFlag = true
		}
	}

	// Combine results from both file processing operations
	successCount += processedCount1 + processedCount2
	errorFlag = errorFlag || hasErrors1 || hasErrors2

	// Write final message to output buffer
	sqlOutputBuffer.WriteString("\n")
	sqlOutputBuffer.WriteString("print 'Stored Procedures Created Successfully...'\n")

	// Write errors to log file
	logWriter := bufio.NewWriter(logFile)
	for err := range errorChan {
		logWriter.WriteString(fmt.Sprintf("File Name: %s\n", err.FileName))
		logWriter.WriteString(fmt.Sprintf("Err Message: %s\n", err.Description))
		if err.LineNumber > 0 {
			logWriter.WriteString(fmt.Sprintf("Build File Line: %d\n", err.LineNumber))
		}
		logWriter.WriteString(fmt.Sprintf("Time Stamp: %s\n", err.TimeStamp))
		if err.Content != "" {
			logWriter.WriteString(fmt.Sprintf("Attempted to write: %s\n", err.Content))
		}
		logWriter.WriteString("\n")
	}

	// Write success count
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
	fmt.Printf("BuildSqlProcs completed in %v\n", elapsedTime)
	fmt.Printf("Success Count: %d\n", successCount)
	if errorFlag {
		fmt.Println("Errors occurred, check log file!")
	}

	// Return the generated SQL content directly
	return sqlOutputBuffer.String()
}

// processBuildOrderFile processes a single build order file
func processBuildOrderFile(
	buildOrderFilePath string,
	sqlOutputBuffer io.Writer,
	logFile *os.File,
	allFiles map[string]bool,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	errorChan chan FileError,
	resultChan chan bool,
	databaseScriptsPath string,
	includeServerSettings bool,
) (int, bool) {
	var (
		successCount = 0
		errorFlag    = false
		lineNumber   = 0 // Changed variable name to be clearer
	)

	// Open build order file
	buildOrderFile, err := os.Open(buildOrderFilePath)
	if err != nil {
		fmt.Printf("Error opening build order file %s: %v\n", buildOrderFilePath, err)
		return 0, true
	}
	defer buildOrderFile.Close()

	// Process build order file
	scanner := bufio.NewScanner(buildOrderFile)
	for scanner.Scan() {
		spFile := strings.TrimSpace(scanner.Text())
		lineNumber++ // Increment the line counter for each line processed

		if len(spFile) > 0 && !strings.HasPrefix(spFile, "'") {
			wg.Add(1)
			go func(fileName string, line int, includeSettings bool) {
				defer wg.Done()

				// Try to open the file
				fileContent, err := os.ReadFile(fileName)
				if err != nil {
					// Handle file not found or other read errors gracefully
					errorChan <- FileError{
						FileName:    fileName,
						Description: err.Error(),
						LineNumber:  line,
						TimeStamp:   time.Now().Format("2006-01-02 15:04:05"),
					}
					resultChan <- false
					return
				}

				// Create SQL content with header and print statement
				header := createHeader(fileName)
				printStatement := createStoredProcPrint(fileName)

				// Lock for synchronized writing to output buffer
				mu.Lock()
				defer mu.Unlock()

				// Write to output buffer with proper spacing
				sqlOutputBuffer.Write([]byte("\n"))
				sqlOutputBuffer.Write([]byte(header))
				sqlOutputBuffer.Write([]byte("\n\n"))
				sqlOutputBuffer.Write([]byte(printStatement))
				sqlOutputBuffer.Write([]byte("\n\n"))

				// Add server settings if needed
				if includeSettings {
					appendMiscSettings(sqlOutputBuffer, databaseScriptsPath)
				}

				_, writeErr := sqlOutputBuffer.Write(fileContent)
				if writeErr != nil {
					errorChan <- FileError{
						FileName:    fileName,
						Description: writeErr.Error(),
						LineNumber:  line,
						TimeStamp:   time.Now().Format("2006-01-02 15:04:05"),
						Content:     string(fileContent),
					}
					resultChan <- false
					return
				}

				// Mark file as processed
				delete(allFiles, fileName)
				resultChan <- true

			}(spFile, lineNumber, includeServerSettings)
		}
	}

	return successCount, errorFlag
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
