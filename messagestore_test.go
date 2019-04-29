package testing

import (
  "fmt"
  "testing"
  "database/sql"
  _ "github.com/go-sql-driver/mysql"

  "github.com/blackhatbrigade/gomessagestore"
)

func TestEmitEvent(t *testing.T) {
  db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

//	mock.ExpectBegin()
//	mock.ExpectExec("UPDATE products").WillReturnResult(sqlmock.NewResult(1, 1))
//	mock.ExpectExec("INSERT INTO product_viewers").WithArgs(2, 3).WillReturnResult(sqlmock.NewResult(1, 1))
//	mock.ExpectCommit()

//	// now we execute our method
//	if err = recordStats(db, 2, 3); err != nil {
//		t.Errorf("error was not expected while updating stats: %s", err)
//	}
//
//	// we make sure that all expectations were met
//	if err := mock.ExpectationsWereMet(); err != nil {
//		t.Errorf("there were unfulfilled expectations: %s", err)
//	}
}
