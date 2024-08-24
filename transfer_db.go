package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

// checkConnection verifies that a connection to the specified database can be established
func checkConnection(config DBConfig) error {
	// Construct the psql command to check the connection
	connCmd := exec.Command("psql",
		"-h", config.Host,
		"-p", config.Port,
		"-U", config.User,
		"-d", config.DBName,
		"-c", "SELECT 1")

	// Set the environment variable for the database password
	connCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", config.Password))

	// Run the command and check for errors
	if err := connCmd.Run(); err != nil {
		return fmt.Errorf("failed to connect to database %s: %v", config.DBName, err)
	}

	return nil
}

// transferDatabase transfers the data from the source database to the target database
func transferDatabase(sourceConfig, targetConfig DBConfig) error {
	// Create the pg_dump command to export the source database
	dumpCmd := exec.Command("pg_dump",
		"-h", sourceConfig.Host,
		"-p", sourceConfig.Port,
		"-U", sourceConfig.User,
		"-F", "c",
		"-b",
		"-v",
		sourceConfig.DBName)

	// Set the environment variable for the source database password
	dumpCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", sourceConfig.Password))

	// Create the pg_restore command to import the data into the target database
	restoreCmd := exec.Command("pg_restore",
		"-h", targetConfig.Host,
		"-p", targetConfig.Port,
		"-U", targetConfig.User,
		"-d", targetConfig.DBName,
		"-v")

	// Set the environment variable for the target database password
	restoreCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", targetConfig.Password))
	var err error
	// Link the output stream of pg_dump to the input stream of pg_restore
	restoreCmd.Stdin, err = dumpCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("error creating pipe: %v", err)
	}

	// Start the pg_restore command before pg_dump to ensure it is ready to receive data
	if err = restoreCmd.Start(); err != nil {
		return fmt.Errorf("error starting pg_restore: %v", err)
	}

	// Start the pg_dump command
	if err = dumpCmd.Start(); err != nil {
		return fmt.Errorf("error starting pg_dump: %v", err)
	}

	// Wait for pg_dump to finish
	if err = dumpCmd.Wait(); err != nil {
		return fmt.Errorf("error waiting for pg_dump: %v", err)
	}

	// Wait for pg_restore to finish
	if err = restoreCmd.Wait(); err != nil {
		return fmt.Errorf("error waiting for pg_restore: %v", err)
	}

	log.Println("Database successfully transferred")
	return nil
}

func main() {
	log.Println("Starting Database Transfer")

	// Load the source and target database configurations
	sourceConfig, targetConfig, err := loadConfig()
	if err != nil {
		log.Fatalf("error loading configuration: %v", err)
	}

	// Check the connection to the source database
	if err = checkConnection(sourceConfig); err != nil {
		log.Fatalf("error connecting to source database: %v", err)
	}
	log.Println("Successfully connected to source database")

	// Check the connection to the target database
	if err = checkConnection(targetConfig); err != nil {
		log.Fatalf("error connecting to target database: %v", err)
	}
	log.Println("Successfully connected to target database")

	// Transfer the data from the source database to the target database
	if err = transferDatabase(sourceConfig, targetConfig); err != nil {
		log.Printf("Failed to transfer database: %v\n", err)
		return
	}

	log.Println("Database transfer complete")
}

// loadConfig loads the source and target database configurations from environment variables
func loadConfig() (DBConfig, DBConfig, error) {
	sourceConfig := DBConfig{
		Host:     os.Getenv("SOURCE_DB_HOST"),
		Port:     os.Getenv("SOURCE_DB_PORT"),
		User:     os.Getenv("SOURCE_DB_USER"),
		Password: os.Getenv("SOURCE_DB_PASSWORD"),
		DBName:   os.Getenv("SOURCE_DB_NAME"),
	}

	targetConfig := DBConfig{
		Host:     os.Getenv("TARGET_DB_HOST"),
		Port:     os.Getenv("TARGET_DB_PORT"),
		User:     os.Getenv("TARGET_DB_USER"),
		Password: os.Getenv("TARGET_DB_PASSWORD"),
		DBName:   os.Getenv("TARGET_DB_NAME"),
	}

	return sourceConfig, targetConfig, nil
}
