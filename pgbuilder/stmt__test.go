package pgbuilder_test

import (
	"testing"
	"time"

	"github.com/go-courier/sqlx-pg/pgbuilder"
	"github.com/go-courier/sqlx/v2"
	"github.com/go-courier/sqlx/v2/builder"
	"github.com/go-courier/sqlx/v2/datatypes"
	"github.com/go-courier/sqlx/v2/migration"
	"github.com/go-courier/sqlx/v2/postgresqlconnector"
	"github.com/go-courier/testingx"
	"github.com/google/uuid"
	"github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var (
	postgresConnector = &postgresqlconnector.PostgreSQLConnector{
		Host:       "postgres://postgres@0.0.0.0:5432",
		Extra:      "sslmode=disable",
		Extensions: []string{"postgis"},
	}

	DB        = sqlx.NewFeatureDatabase("test_for_pg_builder")
	TableUser = DB.Register(&User{})
	db        sqlx.DBExecutor
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)

	db = DB.OpenDB(postgresConnector)

	if err := migration.Migrate(db, nil); err != nil {
		panic(err)
	}
}

func TestStmt(t *testing.T) {
	t.Run("insert single", testingx.It(func(t *testingx.T) {
		user := &User{
			Name: "test",
		}

		err := pgbuilder.Use(db).
			Insert().Into(&User{}).
			ValuesFrom(user).
			OnConflictDoUpdateSet("i_name", TableUser.F("Name")).
			Returning(builder.Expr("*")).
			Scan(user)

		t.Expect(err).To(gomega.BeNil())
		t.Expect(user.ID).NotTo(gomega.Equal(0))
	}))

	t.Run("update simple", testingx.It(func(t *testingx.T) {
		err := pgbuilder.Use(db).
			Update(&User{}).
			SetWith(pgbuilder.RecordValues{uuid.New()}, TableUser.F("Name")).
			Where(TableUser.F("Name").Eq("test")).
			Do()

		t.Expect(err).To(gomega.BeNil())
	}))

	t.Run("insert multi", testingx.It(func(t *testingx.T) {
		err := pgbuilder.Use(db).
			Insert().Into(&User{}).
			ValuesBy(
				func(vc *pgbuilder.RecordCollection) {
					for i := 0; i < 100; i++ {
						vc.SetRecordValues(
							uuid.New(),
						)
					}
				},
				TableUser.F("Name"),
			).
			OnConflictDoNothing(pgbuilder.PrimaryKey).
			Do()

		t.Expect(err).To(gomega.BeNil())
	}))

	t.Run("select", testingx.It(func(t *testingx.T) {
		dataList := &UserDataList{}

		err := dataList.DoList(db, &pgbuilder.Pager{Size: -1})

		t.Expect(err).To(gomega.BeNil())
		t.Expect(len(dataList.Data) >= 1).To(gomega.BeTrue())
	}))

	t.Run("with Select", testingx.It(func(t *testingx.T) {
		count := 0

		err := pgbuilder.Use(db).
			With(builder.T("v_user", builder.Col("f_name"), builder.Col("f_age")), func(stmt *pgbuilder.Stmt, model builder.Model) builder.SqlExpr {
				return stmt.
					Select(builder.MultiMayAutoAlias(
						stmt.T(model).Col("f_name"),
						stmt.T(model).Col("f_age"),
					)).From(&User{}).
					Where(stmt.T(model).Col("f_age").Gt(1))
			}).
			Exec(func(stmt *pgbuilder.Stmt, models ...builder.Model) builder.SqlExpr {
				return stmt.Select(builder.Count()).From(models[0])
			}).
			Scan(&count)

		t.Expect(err).To(gomega.BeNil())
		t.Expect(count > 0).To(gomega.BeTrue())
	}))

	t.Run("delete soft", testingx.It(func(t *testingx.T) {
		err := pgbuilder.Use(db).
			Delete(&User{}).
			Do()

		t.Expect(err).To(gomega.BeNil())
	}))

	t.Run("delete ignore deletedAt", testingx.It(func(t *testingx.T) {
		err := pgbuilder.Use(db.WithContext(pgbuilder.ContextWithIgnoreDeletedAt(db.Context()))).
			Delete(&User{}).
			Do()

		t.Expect(err).To(gomega.BeNil())
	}))
}

type UserParams struct {
	Names []string `name:"name" in:"query"`
	Ages  []int    `name:"age" in:"query"`
}

func (u *UserParams) ToCondition(db sqlx.DBExecutor) builder.SqlCondition {
	where := builder.EmptyCond()

	if len(u.Names) > 0 {
		where = where.And(TableUser.F("Name").In(u.Names))
	}

	if len(u.Ages) > 0 {
		where = where.And(TableUser.F("Age").In(u.Ages))
	}

	return where
}

type UserDataList struct {
	UserParams `json:"-"`
	Data       []*User `json:"data"`
	pgbuilder.WithTotal
}

func (UserDataList) New() interface{} {
	return &User{}
}

func (u *UserDataList) Next(v interface{}) error {
	u.Data = append(u.Data, v.(*User))
	return nil
}

func (u *UserDataList) DoList(db sqlx.DBExecutor, pager *pgbuilder.Pager, additions ...builder.Addition) error {
	return pgbuilder.Use(db).Select(nil).From(&User{}).Where(u.ToCondition(db), additions...).List(u, pager)
}

type User struct {
	ID   uint64 `db:"f_id,autoincrement"`
	Name string `db:"f_name,size=255,default=''"`
	Age  int    `db:"f_age,default='18'"`

	OperationTimesWithDeletedAt
}

func (user *User) TableName() string {
	return "t_user"
}

func (user *User) PrimaryKey() []string {
	return []string{"ID"}
}

func (user *User) UniqueIndexes() builder.Indexes {
	return builder.Indexes{
		"i_name": {"Name"},
	}
}

func (User) FieldDeletedAt() *builder.Column {
	return TableUser.F("DeletedAt")
}

func (User) FieldCreatedAt() *builder.Column {
	return TableUser.F("CreatedAt")
}

func (User) FieldUpdatedAt() *builder.Column {
	return TableUser.F("UpdatedAt")
}

type OperationTimes struct {
	CreatedAt datatypes.Timestamp `db:"f_created_at,default='0'" json:"createdAt" `
	UpdatedAt datatypes.Timestamp `db:"f_updated_at,default='0'" json:"updatedAt"`
}

func (times *OperationTimes) MarkUpdatedAt() {
	times.UpdatedAt = datatypes.Timestamp(time.Now())
}

func (times *OperationTimes) MarkCreatedAt() {
	times.MarkUpdatedAt()
	times.CreatedAt = times.UpdatedAt
}

type OperationTimesWithDeletedAt struct {
	OperationTimes
	DeletedAt datatypes.Timestamp `db:"f_deleted_at,default='0'" json:"-"`
}
