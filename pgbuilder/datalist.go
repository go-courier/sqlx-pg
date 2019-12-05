package pgbuilder

import (
	"github.com/go-courier/sqlx/v2"
	"github.com/go-courier/sqlx/v2/builder"
)

type DataList interface {
	sqlx.ScanIterator
	ConditionBuilder
	DoList(db sqlx.DBExecutor, conditionBuilder ConditionBuilder, pager *Pager, additions ...builder.Addition) error
}

func BatchDoList(db sqlx.DBExecutor, scanners ...DataList) (err error) {
	if len(scanners) == 0 {
		return nil
	}

	for i := range scanners {
		scanner := scanners[i]

		cond := scanner.ToCondition(db)

		if cond != nil {
			if err := scanner.DoList(db, ConditionBuilderFromCondition(cond), nil); err != nil {
				return err
			}
		}
	}

	return nil
}
