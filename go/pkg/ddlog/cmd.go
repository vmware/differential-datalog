package ddlog

/*
#include "ddlog.h"
*/
import "C"

import "unsafe"

// Command is a wrapper around a DDlog command (ddlog_cmd *). Creating a Command with one of the
// functions below will never fail; however the command it creates may fail to execute.
type Command struct {
	ptr unsafe.Pointer
}

// NewInsertCommand creates an insert command.
func NewInsertCommand(tableID TableID, r Record) Command {
	cmd := C.ddlog_insert_cmd(C.size_t(tableID), r.ptr())
	return Command{unsafe.Pointer(cmd)}
}

// NewInsertOrUpdateCommand creates an insert-or-update command.
func NewInsertOrUpdateCommand(tableID TableID, r Record) Command {
	cmd := C.ddlog_insert_or_update_cmd(C.size_t(tableID), r.ptr())
	return Command{unsafe.Pointer(cmd)}
}

// NewDeleteValCommand creates a delete-by-value command.
func NewDeleteValCommand(tableID TableID, r Record) Command {
	cmd := C.ddlog_delete_val_cmd(C.size_t(tableID), r.ptr())
	return Command{unsafe.Pointer(cmd)}
}

// NewDeleteKeyCommand creates a delete-by-key command. tableID must have a primary key for this
// command to work.
func NewDeleteKeyCommand(tableID TableID, r Record) Command {
	cmd := C.ddlog_delete_key_cmd(C.size_t(tableID), r.ptr())
	return Command{unsafe.Pointer(cmd)}
}
