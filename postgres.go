package messagestore

import (
  "fmt"
  "os"

  "database/sql"
  _ "github.com/lib/pq"
)

var (
  host = os.Getenv("DB_HOST")
  port = os.Getenv("DB_PORT")
  user = os.Getenv("DB_USER")
  password = os.Getenv("DB_USER_PASS")
  dbName = os.Getenv("DB_NAME")
)

// Retrieves the connection information for the PostgreSQL connection.
//
// Environment vars that must be set:
//  DB_HOST - Hostname for the pg instance.
//  DB_PORT - Port of the pg instance.
//  DB_USER - Database User.
//  DB_USER_PASS - Password for the provided user.
//  DB_NAME - Name of the database to use.
func getSQLInfo() string {
  return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
    host, port, user, password, dbName)
}

// Retrieves the DB interface.
func GetDBInstance() *sql.DB {
  db, err := sql.Open("postgres", getSQLInfo())
  if err != nil {
    panic(err)
  }

  fmt.Println("Connection established!")
  return db
}
