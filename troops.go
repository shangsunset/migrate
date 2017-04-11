package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	_ "github.com/lib/pq"
)

const tableName = "migrations"

func main() {

	migrationPath := flag.String("path", "./migrations", "path to schema files")
	flag.Parse()

	db, err := initialize()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	files, err := getMigrationFiles(db, *migrationPath)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	if len(files) == 0 {
		fmt.Println("Nothing to migrate.")
		return
	}

	var wg sync.WaitGroup
	ch := make(chan error)
	finished := make(chan bool)
	for _, file := range files {
		wg.Add(1)
		go migrate(db, file, ch, &wg, *migrationPath)
	}

	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		fmt.Println("All done!")
		return
	case err = <-ch:
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}
	}
}

func initialize() (*sql.DB, error) {

	db, err := sql.Open("postgres", "user=shangyeshen dbname=hp sslmode=disable")
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	err = ensureVersionTableExists(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func ensureVersionTableExists(db *sql.DB) error {

	count := 0
	r := db.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema());", tableName)
	err := r.Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (migration varchar(256) not null);")
	if err != nil {
		return err
	}
	return nil
}

func getMigrationFiles(db *sql.DB, path string) ([]os.FileInfo, error) {

	var exists bool
	var filesToMigrate []os.FileInfo

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileName := strings.Split(file.Name(), ".")[0]
		row := db.QueryRow("SELECT EXISTS(SELECT 1 FROM "+tableName+" WHERE migration = $1);", fileName)
		err := row.Scan(&exists)
		if err != nil {
			return nil, err
		}
		if !exists {
			filesToMigrate = append(filesToMigrate, file)
		}
	}

	return filesToMigrate, nil
}

func migrate(db *sql.DB, file os.FileInfo, ch chan error, wg *sync.WaitGroup, path string) {

	defer wg.Done()
	filePath := path + "/" + file.Name()
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		ch <- err
		wg.Done()
	}

	fileName := strings.Split(file.Name(), ".")[0]
	_, err = db.Exec("INSERT INTO "+tableName+" (migration) VALUES ($1)", fileName)
	if err != nil {
		ch <- err
		wg.Done()
	}

	_, err = db.Exec(string(content))
	if err != nil {
		ch <- err
		wg.Done()
	}

	fmt.Println("Migrated: " + file.Name())
}
