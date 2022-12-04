package pgxrecord

import (
	"github.com/jackc/pgx/v5"
)

func Private_sanitizeIdentifier(s string) string {
	return sanitizeIdentifier(s)
}

func Private_insertRowSQL(tableName pgx.Identifier, values map[string]any, returningClause string) (sql string, args []any) {
	return insertRowSQL(tableName, values, returningClause)
}
