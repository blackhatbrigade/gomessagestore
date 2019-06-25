package repository

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

//NewPostgresRepository creates a new in memory implementation for the messagestore reop
func NewPostgresRepository(db *sql.DB) Repository {
	r := new(postgresRepo)
	r.dbx = sqlx.NewDb(db, "postgres")
	return r
}

type postgresRepo struct {
	dbx *sqlx.DB
}

type returnPair struct {
	messages []*MessageEnvelope
	err      error
}
