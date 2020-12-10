#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include <string.h> // required for strncmp()
#include <errno.h>  // required for error code identifiers

#include "ddlog.h"

bool print_records_callback(uintptr_t arg, const ddlog_record *rec, ssize_t weight) {
    if (rec == NULL) {
        return false;
    }

    char* record_str = ddlog_dump_record(rec);
    if (record_str == NULL) {
        fprintf(stderr, "failed to dump record\n");
        exit(EXIT_FAILURE);
    }
    printf("Record: %s\n", record_str);

    return true;
}

int main(int args, char **argv)
{
    // Start the DDlog program and connect to it
	ddlog_prog prog = ddlog_run(1, true, NULL, 0, NULL, NULL);
	if (prog == NULL) {
		fprintf(stderr, "failed to initialize DDlog program\n");
		return EXIT_FAILURE;
	};

    // Print IDs of tables corresponding to `Links` and `ConnectedNodes` relations
    printf("Links table id: %lu\n", ddlog_get_table_id("Links"));
    printf("ConnectedNodes table id: %lu\n", ddlog_get_table_id("ConnectedNodes"));

    char *src_line_ptr = NULL;
    char *dst_line_ptr = NULL;
    char *link_status_line_ptr = NULL;
    size_t n = 0;
    bool link_status = false;

    // Getting new record values from standard input
    printf("Please enter source name > ");
    if (getline(&src_line_ptr, &n, stdin) < 0) {
        return -EINVAL;
    }
    printf("Please enter destination name > ");
    if (getline(&dst_line_ptr, &n, stdin) < 0) {
        return -EINVAL;
    }
    printf("Please enter the link status between source and destination > ");
    if (getline(&link_status_line_ptr, &n, stdin) < 0) {
        return -EINVAL;
    }

    // Trimming newline characters
    src_line_ptr[strlen(src_line_ptr) - 1] = '\0';
    dst_line_ptr[strlen(dst_line_ptr) - 1] = '\0';
    // Parsing value for the link status
    link_status = (strncmp("true", link_status_line_ptr, 4) == 0) ? true : false;

    // Creating record values in DDlog format
    ddlog_record* src = ddlog_string(src_line_ptr);
    ddlog_record* dst = ddlog_string(dst_line_ptr);
    ddlog_record* lstatus = ddlog_bool(link_status);

    // Placing new record values in one struct to become a single record
    ddlog_record *struct_args[3];
    struct_args[0] = src;
    struct_args[1] = dst;
    struct_args[2] = lstatus;
    ddlog_record* new_record = ddlog_struct("Links", struct_args, 3);

    printf("Inserting the following record: %s\n", ddlog_dump_record(new_record));

    // Start transaction
    if (ddlog_transaction_start(prog) < 0) {
        fprintf(stderr, "failed to start transaction\n");
        return EXIT_FAILURE;
    };

    // apply updates
    ddlog_cmd *cmds[1];
    ddlog_cmd* cmd = ddlog_insert_cmd(1, new_record);
    cmds[0] = cmd;
    if (cmd == NULL) {
        fprintf(stderr, "failed to create insert command\n");
        return EXIT_FAILURE;
    }
    if (ddlog_apply_updates(prog, cmds, 1) < 0) {
        fprintf(stderr, "failed to apply updates\n");
        return EXIT_FAILURE;
    };
    printf("Applied update.\n");

    // Free'ing memory
    free(src_line_ptr);
    free(dst_line_ptr);
    free(link_status_line_ptr);

    // Commit transaction
    if (ddlog_transaction_commit(prog) < 0) {
        fprintf(stderr, "failed to commit transaction\n");
        return EXIT_FAILURE;
    };

    // Printing records in the `ConnectedNodes` relation
    ddlog_dump_table(prog, 0, &print_records_callback, 1);

    // Stopping DDlog program
	if (ddlog_stop(prog) < 0) {
		fprintf(stderr, "failed to stop DDlog program\n");
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}
