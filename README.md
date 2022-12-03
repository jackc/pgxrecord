[![Go Reference](https://pkg.go.dev/badge/github.com/jackc/pgxrecord.svg)](https://pkg.go.dev/github.com/jackc/pgxrecord)
![Build Status](https://github.com/jackc/pgxrecord/actions/workflows/ci.yml/badge.svg)

# pgxrecord

Package pgxrecord is a tiny library for CRUD operations.

It does not and most likely will not have traditional ORM features such as associations. It's sole purpose is a simple way to read and write records.

## Package Status

pgxrecord is highly experimental. The API may change at any time or the package may be abandoned.

## Testing

The pgxrecord tests require a PostgreSQL database. It will use the standard PG* environment variables (PGHOST, PGDATABASE, etc.) for its connection settings. Each test is run inside of a transaction which is rolled back at the end of the test. No permanent changes will be made to the test database.
