package file

import "github.com/google/uuid"

// ApplicationName is the task list for this workflow
const ApplicationName = "fileTaskGroup"

// HostID - Use a new uuid just for demo so we can run 2 host specific activity workers on same machine.
// In real world case, you would use a hostname or ip address as HostID.
var HostID = ApplicationName + "_" + uuid.New().String()
