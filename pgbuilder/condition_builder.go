package pgbuilder

import (
	"github.com/go-courier/sqlx/v2"
	"github.com/go-courier/sqlx/v2/builder"
)

type ConditionBuilder interface {
	ToCondition(db sqlx.DBExecutor) builder.SqlCondition
}

func ToCondition(db sqlx.DBExecutor, b ConditionBuilder) builder.SqlCondition {
	if b == nil {
		return builder.EmptyCond()
	}
	return b.ToCondition(db)
}

func ConditionBuilderFromCondition(c builder.SqlCondition) ConditionBuilder {
	return &conditionWrapper{condition: c}
}

type conditionWrapper struct {
	condition builder.SqlCondition
}

func (c *conditionWrapper) ToCondition(db sqlx.DBExecutor) builder.SqlCondition {
	return c.condition
}
