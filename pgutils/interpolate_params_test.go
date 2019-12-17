package pgutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-courier/geography"
	"github.com/go-courier/sqlx/v2/builder"
	. "github.com/onsi/gomega"
)

func TestInterpolateParams(t *testing.T) {
	e := builder.Expr("INSERT INTO t (f_id, f_name, f_photo, f_created, f_location) VALUES (?, ?, ?, ?, ?)", 1, "name", []byte("0101000020110F00006E6BA55CA07A694154C51D5FC4715541"), time.Now(), geography.Point{120, 45})

	s, err := InterpolateParams(context.Background(), e)
	NewWithT(t).Expect(err).To(BeNil())
	fmt.Println(s)
}
