package buildsql

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// Constants for file paths
const (
	SQLDataFile         = "Build/ptrdata.sql"
	SQLSPFile           = "Build/ptrcrsp.sql"
	SQLTablesFile       = "Build/ptrcrtb.sql"
	SQLUpdateTablesFile = "Build/ptruptb.sql"
	SQLCRDBFile         = "Build/ptrcrdb.sql"
)

// AppendSQL appends the contents of one file to another
func AppendSQL(writer io.Writer, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// Add two blank lines before appending content
	if _, err := fmt.Fprintln(writer); err != nil {
		return fmt.Errorf("failed to write blank lines: %w", err)
	}

	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// BuildSQL is the main function that builds the SQL files
func BuildSQL(sqlPath string) error {
	// Create the Build directory if it doesn't exist
	if err := os.MkdirAll("Build", os.ModePerm); err != nil {
		return fmt.Errorf("failed to create Build directory: %w", err)
	}

	// Delete existing files if they exist (ignore errors)
	os.Remove(SQLDataFile)
	os.Remove(SQLSPFile)
	os.Remove(SQLTablesFile)
	os.Remove(SQLCRDBFile)
	os.Remove(SQLUpdateTablesFile)

	// Use WaitGroup to synchronize goroutines
	var wg sync.WaitGroup
	var spContent, updateContent string
	var dataContent string
	var spErr, updateErr, dataErr error

	// Run BuildSqlProcs concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		spContent = BuildSqlProcs(sqlPath)
		if spContent == "" {
			spErr = fmt.Errorf("failed to generate stored procedures content")
		}
	}()

	// Run BuildSqlUpdates concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		updateContent, updateErr = BuildSqlUpdates(sqlPath)
		if updateContent == "" && updateErr == nil {
			updateErr = fmt.Errorf("failed to generate updates content")
		}
	}()

	// Build SQL data content in memory concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		var dataBuffer strings.Builder

		// Append all SQL files to the buffer
		sqlFiles := []string{
			"Data/constant.sql",
			"Data/TableList.sql",
			"Data/DataValidation.sql",
			"Data/security.sql",
			"Data/menu.sql",
			"Data/toolbar.sql",
			"Data/error.sql",
			"Data/report.sql",
			"Data/TaxModule.sql",
		}

		for _, file := range sqlFiles {
			data, err := os.ReadFile(file)
			if err != nil {
				dataErr = fmt.Errorf("failed to read file %s: %w", file, err)
				return
			}

			// Add two blank lines before appending content
			dataBuffer.WriteString("\n\n")
			dataBuffer.Write(data)
		}

		dataContent = dataBuffer.String()
	}()

	// Wait for all goroutines to complete
	wg.Wait()

	// Check for errors
	if spErr != nil {
		return spErr
	}
	if updateErr != nil {
		return updateErr
	}
	if dataErr != nil {
		return dataErr
	}

	// Write the generated SQL content to files
	if err := os.WriteFile(SQLDataFile, []byte(dataContent), 0644); err != nil {
		return fmt.Errorf("failed to write data content: %w", err)
	}

	if err := os.WriteFile(SQLSPFile, []byte(spContent), 0644); err != nil {
		return fmt.Errorf("failed to write stored procedures content: %w", err)
	}

	if err := os.WriteFile(SQLUpdateTablesFile, []byte(updateContent), 0644); err != nil {
		return fmt.Errorf("failed to write updates content: %w", err)
	}

	// Copy tables.sql as before
	if err := copyFile("Schema/tables.sql", SQLTablesFile); err != nil {
		return fmt.Errorf("failed to copy tables.sql: %w", err)
	}

	return nil
}

// Helper function to copy files
func copyFile(src, dst string) error {
	sourceFile, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	return os.WriteFile(dst, sourceFile, 0644)
}
