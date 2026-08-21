//go:build e2e

package e2e_test

import (
	"os"
	"testing"
	"time"

	"github.com/compforge/case-harness/go/e2e/caserun"
	"github.com/compforge/case-harness/go/e2e/testrun"
)

var (
	systemRun = testrun.New(
		"agentd-system",
		testrun.WithRunID(valueOr(os.Getenv("AGENTD_E2E_RUN_ID"), time.Now().UTC().Format("20060102T150405.000000000Z"))),
		testrun.WithRunsDir(valueOr(os.Getenv("AGENTD_E2E_RUNS_DIR"), "runs")),
	)
	systemCaseBudgets = caserun.Budgets{
		Prepare: 2 * time.Minute,
		Execute: 20 * time.Minute,
		Judge:   30 * time.Second,
		Cleanup: 30 * time.Second,
	}
)

func recordSystemCase(t *testing.T, result caserun.Result) {
	t.Helper()
	systemRun.Assert(t, result)
}

func TestMain(m *testing.M) {
	os.Exit(systemRun.Main(m.Run))
}
