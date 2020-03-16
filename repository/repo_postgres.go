package repository

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

//NewPostgresRepository creates a new in memory implementation for the messagestore reop
func NewPostgresRepository(db *sql.DB, log logrus.FieldLogger) Repository {
	r := new(postgresRepo)
	r.dbx = sqlx.NewDb(db, "postgres")
	return r
}

type postgresRepo struct {
	dbx *sqlx.DB
	log logrus.Logger
}

type returnPair struct {
	messages []*MessageEnvelope
	err      error
}
