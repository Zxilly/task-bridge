package storage

import "github.com/Zxilly/task-bridge/internal/model"

func (t *Tx) ResumeRequest(id string) (model.TaskResumeRequest, error) {
	return decode[model.TaskResumeRequest](t.QueryRowContext(t.ctx, "SELECT payload FROM task_resume_requests WHERE id=?", id))
}

func (t *Tx) ResumeRequestByKey(requester, key string) (model.TaskResumeRequest, error) {
	return decode[model.TaskResumeRequest](t.QueryRowContext(t.ctx, "SELECT payload FROM task_resume_requests WHERE requester=? AND idem=?", requester, key))
}

func (t *Tx) SaveResumeRequest(request model.TaskResumeRequest) error {
	_, err := t.ExecContext(t.ctx, "INSERT INTO task_resume_requests(id,task,requester,idem,payload) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", request.ID, request.TaskID, request.RequesterID, request.IdempotencyKey, model.JSON(request))
	return err
}

// FailTaskResume updates the historical receipt and its latest task summary in
// the same transaction. The caller persists the task alongside its transition.
func (t *Tx) FailTaskResume(task *model.Task, code, message string, states ...string) error {
	if task.ResumeRequest == nil {
		return nil
	}
	for _, state := range states {
		if task.ResumeRequest.State == state {
			request := *task.ResumeRequest
			request.State = "failed"
			request.UpdatedAt = model.Now()
			request.Error = &model.Error{Code: code, Message: message}
			if err := t.SaveResumeRequest(request); err != nil {
				return err
			}
			task.ResumeRequest = &request
			return nil
		}
	}
	return nil
}
