package pgbuilder

import (
	"context"
	"time"

	"github.com/go-courier/sqlx/v2/builder"
	"github.com/go-courier/sqlx/v2/datatypes"
)

func (s *Stmt) Delete(model builder.Model) *StmtDelete {
	return &StmtDelete{
		stmt:  s,
		model: model,
	}
}

/**
[ WITH [ RECURSIVE ] with_query [, ...] ]
DELETE FROM [ ONLY ] table_name [ * ] [ [ AS ] alias ]
    [ USING using_list ]
    [ WHERE condition | WHERE CURRENT OF cursor_name ]
    [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]
*/
type StmtDelete struct {
	stmt  *Stmt
	model builder.Model
	where builder.SqlCondition
}

func (s *StmtDelete) IsNil() bool {
	return s.stmt == nil || s.model == nil
}

func (s StmtDelete) From(model builder.Model) *StmtDelete {
	s.model = model
	return &s
}

func (s StmtDelete) Where(where builder.SqlCondition) *StmtDelete {
	s.where = where
	return &s
}

func (s *StmtDelete) Returning(target builder.SqlExpr) CouldScan {
	return s.stmt.ReturningOf(s, target)
}

func (s *StmtDelete) Ex(ctx context.Context) *builder.Ex {
	table := s.stmt.T(s.model)

	if !builder.TogglesFromContext(ctx).Is(toggleKeyIgnoreDeletedAt) {
		if modelWithDeleted, ok := s.model.(ModelWithDeletedAt); ok {
			return s.stmt.
				Update(s.model).
				Where(s.where).
				SetBy(
					func(vc *RecordCollection) {
						vc.SetRecordValues(datatypes.Timestamp(time.Now()))
					},
					modelWithDeleted.FieldDeletedAt(),
				).
				Ex(ctx)
		}
	}

	return builder.
		Delete().
		From(table, append(builder.Additions{builder.Where(s.where)})...).
		Ex(ctx)
}

func (s *StmtDelete) Do() error {
	_, err := s.stmt.db.ExecExpr(s)
	return err
}