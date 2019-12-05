package pgbuilder

import (
	"context"
	"time"

	"github.com/go-courier/sqlx/v2/builder"
	"github.com/go-courier/sqlx/v2/datatypes"
)

func (s *Stmt) Update(model builder.Model, modifiers ...string) *StmtUpdate {
	return &StmtUpdate{
		stmt:      s,
		model:     model,
		modifiers: modifiers,
		rc:        &RecordCollection{},
	}
}

/**
[ WITH [ RECURSIVE ] with_query [, ...] ]
UPDATE [ ONLY ] table_name [ * ] [ [ AS ] alias ]
    SET { column_name = { expression | DEFAULT } |
          ( column_name [, ...] ) = [ ROW ] ( { expression | DEFAULT } [, ...] ) |
          ( column_name [, ...] ) = ( sub-SELECT )
        } [, ...]
    [ FROM from_list ]
    [ WHERE condition | WHERE CURRENT OF cursor_name ]
    [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]
*/
type StmtUpdate struct {
	stmt      *Stmt
	modifiers []string
	model     builder.Model
	where     builder.SqlCondition
	rc        *RecordCollection
}

func (s *StmtUpdate) Do() error {
	if s.IsNil() {
		return nil
	}
	_, err := s.stmt.db.ExecExpr(s)
	return err
}

func (s *StmtUpdate) IsNil() bool {
	return s.stmt == nil || s.model == nil || s.rc == nil
}

func (s StmtUpdate) Where(where builder.SqlCondition) *StmtUpdate {
	s.where = where
	return &s
}

func (s StmtUpdate) SetBy(collect func(vc *RecordCollection), columns ...*builder.Column) *StmtUpdate {
	s.rc = RecordCollectionBy(collect, columns...)
	return &s
}

func (s StmtUpdate) SetWith(recordValues RecordValues, columns ...*builder.Column) *StmtUpdate {
	s.rc = RecordCollectionWith(recordValues, columns...)
	return &s
}

func (s StmtUpdate) SetFrom(model builder.Model, columnsCouldBeZeroValue ...*builder.Column) *StmtUpdate {
	s.rc = RecordCollectionFrom(s.stmt.db, model, columnsCouldBeZeroValue...)
	return &s
}

func (s *StmtUpdate) Returning(target builder.SqlExpr) CouldScan {
	return s.stmt.ReturningOf(s, target)
}

func (s *StmtUpdate) Ex(ctx context.Context) *builder.Ex {
	where := s.where

	// 已经删除不应该再次处理
	if modelWithDeleted, ok := s.model.(ModelWithDeletedAt); ok {
		where = builder.And(
			where,
			modelWithDeleted.FieldDeletedAt().Eq(0),
		)
	}

	rc := s.rc

	if modelWithUpdatedAt, ok := s.model.(ModelWithUpdatedAt); ok {
		// 补全更新时间
		if rc.Columns.F(modelWithUpdatedAt.FieldUpdatedAt().FieldName) == nil {
			rc = s.rc.WithExtendCol(modelWithUpdatedAt.FieldUpdatedAt(), datatypes.Timestamp(time.Now()))
		}
	}

	return builder.
		Update(s.stmt.T(s.model), s.modifiers...).
		Where(where).
		Set(rc.AsAssignments()...).
		Ex(ctx)
}
