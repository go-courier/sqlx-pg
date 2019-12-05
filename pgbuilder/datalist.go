package pgbuilder

import (
	"github.com/go-courier/sqlx/v2"
	"github.com/go-courier/sqlx/v2/builder"
)

type DataList interface {
	sqlx.ScanIterator
	ConditionBuilder
	DoList(db sqlx.DBExecutor, pager *Pager, additions ...builder.Addition) error
}
