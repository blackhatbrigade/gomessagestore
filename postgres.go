import (
  "fmt"
  "os"

  "database/sql"
  _ "github.com/lib/pq"
)

var (
  host = os.Getenv("db_host")
  port = os.Getenv("db_port")
  user = os.Getenv("db_user")
  password = os.Getenv("db_user_pass")
  dbName = os.Getenv("db_name")
)

// Retrieves the connection information for the PostgreSQL connection.
func GetSQLInfo() string {
  return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
    host, port, user, password, dbName)
}

// Retrieves the DB interface.
func GetDBInstance() *sql.DB {
  db, err := sql.Open("postgres", GetSQLInfo())
  if err != nil {
    panic(err)
  }

  fmt.Println("Connection established!")
  return db
}
