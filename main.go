package buildsql

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	buildDir := filepath.Join(sqlPath, "Build")
	if err := os.MkdirAll(buildDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create Build directory: %w", err)
	}

	// Get full paths for SQL files
	dataFilePath := filepath.Join(sqlPath, SQLDataFile)
	spFilePath := filepath.Join(sqlPath, SQLSPFile)
	tablesFilePath := filepath.Join(sqlPath, SQLTablesFile)
	crdbFilePath := filepath.Join(sqlPath, SQLCRDBFile)
	updateTablesFilePath := filepath.Join(sqlPath, SQLUpdateTablesFile)

	// Delete existing files if they exist (ignore errors)
	os.Remove(dataFilePath)
	os.Remove(spFilePath)
	os.Remove(tablesFilePath)
	os.Remove(crdbFilePath)
	os.Remove(updateTablesFilePath)

	// Use error channels for each goroutine
	spErrCh := make(chan error, 1)
	updateErrCh := make(chan error, 1)
	dataErrCh := make(chan error, 1)

	// Results channels
	spContentCh := make(chan string, 1)
	updateContentCh := make(chan string, 1)
	dataContentCh := make(chan string, 1)

	// Use WaitGroup to synchronize goroutines
	var wg sync.WaitGroup

	// Run BuildSqlProcs concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		content := BuildSqlProcs(sqlPath)
		if content == "" {
			spErrCh <- fmt.Errorf("failed to generate stored procedures content")
		} else {
			spContentCh <- content
		}
	}()

	// Run BuildSqlUpdates concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		content, err := BuildSqlUpdates(sqlPath)
		if err != nil {
			updateErrCh <- err
			return
		}
		if content == "" {
			updateErrCh <- fmt.Errorf("failed to generate updates content")
			return
		}
		updateContentCh <- content
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
			fullPath := filepath.Join(sqlPath, file)
			data, err := os.ReadFile(fullPath)
			if err != nil {
				dataErrCh <- fmt.Errorf("failed to read file %s: %w", fullPath, err)
				return
			}

			// Add two blank lines before appending content
			dataBuffer.WriteString("\n\n")
			dataBuffer.Write(data)
		}

		dataContentCh <- dataBuffer.String()
	}()

	// Wait for all goroutines to complete
	wg.Wait()
	close(spErrCh)
	close(updateErrCh)
	close(dataErrCh)
	close(spContentCh)
	close(updateContentCh)
	close(dataContentCh)

	// Check for errors
	if err := <-spErrCh; err != nil {
		return err
	}
	if err := <-updateErrCh; err != nil {
		return err
	}
	if err := <-dataErrCh; err != nil {
		return err
	}

	// Retrieve results
	spContent := <-spContentCh
	updateContent := <-updateContentCh
	dataContent := <-dataContentCh

	// Write the generated SQL content to files
	if err := os.WriteFile(dataFilePath, []byte(dataContent), 0644); err != nil {
		return fmt.Errorf("failed to write data content: %w", err)
	}

	if err := os.WriteFile(spFilePath, []byte(spContent), 0644); err != nil {
		return fmt.Errorf("failed to write stored procedures content: %w", err)
	}

	if err := os.WriteFile(updateTablesFilePath, []byte(updateContent), 0644); err != nil {
		return fmt.Errorf("failed to write updates content: %w", err)
	}

	// Copy tables.sql as before
	if err := copyFile(filepath.Join(sqlPath, "Schema/tables.sql"), tablesFilePath); err != nil {
		return fmt.Errorf("failed to copy tables.sql: %w", err)
	}

	// Create the CRDB file
	if err := CreateCRDBFile(sqlPath); err != nil {
		return fmt.Errorf("failed to create CRDB file: %w", err)
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

// CreateCRDBFile creates the CRDB SQL file
func CreateCRDBFile(sqlPath string) error {
	// Create the CRDB file path
	crdbFilePath := filepath.Join(sqlPath, SQLCRDBFile)

	// Create a new file
	crdbFile, err := os.Create(crdbFilePath)
	if err != nil {
		return fmt.Errorf("failed to create CRDB file: %w", err)
	}
	defer crdbFile.Close()

	// Append the required SQL files in order
	if err := AppendSQL(crdbFile, filepath.Join(sqlPath, "Schema/Tables.sql")); err != nil {
		return fmt.Errorf("failed to append Tables.sql: %w", err)
	}

	if err := AppendSQL(crdbFile, filepath.Join(sqlPath, SQLDataFile)); err != nil {
		return fmt.Errorf("failed to append Data file: %w", err)
	}

	if err := AppendSQL(crdbFile, filepath.Join(sqlPath, SQLSPFile)); err != nil {
		return fmt.Errorf("failed to append SP file: %w", err)
	}

	return nil
}
