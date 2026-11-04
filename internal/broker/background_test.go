package broker

import (
	"fmt"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
)

func TestBackgroundAdmissionIsBoundedAndIdempotent(t *testing.T) {
	f := newFixture(t)
	task := f.running("background-capacity")
	args := Args{TaskID: task.ID, AssignmentID: task.AssignmentID, OperationKey: "bg-0", Command: "sleep 30", Background: true}
	first, err := f.agent("bash", args)
	if err != nil {
		t.Fatal(err)
	}
	c := first.(model.CommandResult).Command
	if !c.Background || c.TimeoutMS != min(int64(900000), task.DeadlineAt-c.CreatedAt) {
		t.Fatalf("background defaults: %+v", c)
	}
	again, err := f.agent("bash", args)
	if err != nil || again.(model.CommandResult).Command.ID != c.ID {
		t.Fatalf("retry: %+v %v", again, err)
	}
	args.Background = false
	_, err = f.agent("bash", args)
	requireCode(t, err, "idempotency_conflict")
	args.Background = true
	for i := 1; i < 8; i++ {
		args.OperationKey = fmt.Sprintf("bg-%d", i)
		if _, err = f.agent("bash", args); err != nil {
			t.Fatal(err)
		}
	}
	args.OperationKey = "bg-overflow"
	_, err = f.agent("bash", args)
	requireCode(t, err, "command_busy")
}
