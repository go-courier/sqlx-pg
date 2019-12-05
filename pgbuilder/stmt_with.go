package pgbuilder

import (
	"context"
	"strings"

	"github.com/go-courier/sqlx/v2/builder"
)

func (s *Stmt) With(model builder.Model, build BuildSubExpr) *StmtWith {
	return (&StmtWith{stmt: s}).With(model, build)
}

func (s *Stmt) WithRecursive(model builder.Model, build BuildSubExpr) *StmtWith {
	return (&StmtWith{stmt: s, modifiers: []string{"RECURSIVE"}}).With(model, build)
}

type BuildSubExpr func(s *Stmt, model builder.Model) builder.SqlExpr

type StmtWith struct {
	stmt      *Stmt
	modifiers []string
	models    []builder.Model
	asList    []BuildSubExpr
	statement func(stmt *Stmt, model ...builder.Model) builder.SqlExpr
}

func (s StmtWith) With(model builder.Model, build BuildSubExpr) *StmtWith {
	s.models = append(s.models, model)
	s.asList = append(s.asList, build)
	return &s
}

func (s StmtWith) Exec(statement func(stmt *Stmt, model ...builder.Model) builder.SqlExpr) *StmtWith {
	s.statement = statement
	return &s
}

func (s *StmtWith) IsNil() bool {
	return s == nil || len(s.models) == 0 || len(s.asList) == 0 || s.statement == nil
}

func (s *StmtWith) Scan(v interface{}) error {
	return s.stmt.db.QueryExprAndScan(s, v)
}

func (s *StmtWith) Do() error {
	_, err := s.stmt.db.ExecExpr(s)
	return err
}

func (s *StmtWith) Ex(ctx context.Context) *builder.Ex {
	e := builder.Expr("WITH ")

	if len(s.modifiers) > 0 {
		e.WriteString(strings.Join(s.modifiers, " "))
		e.WriteString(" ")
	}

	for i := range s.models {
		if i > 0 {
			e.WriteString(", ")
		}

		model := s.models[i]

		table := s.stmt.T(model)

		e.WriteExpr(table)
		e.WriteGroup(func(e *builder.Ex) {
			e.WriteExpr(&table.Columns)
		})

		e.WriteString(" AS ")

		build := s.asList[i]

		e.WriteGroup(func(e *builder.Ex) {
			e.WriteByte('\n')
			e.WriteExpr(build(s.stmt, model))
			e.WriteByte('\n')
		})
	}

	e.WriteByte('\n')
	e.WriteExpr(s.statement(s.stmt, s.models...))

	return e.Ex(ctx)
}
