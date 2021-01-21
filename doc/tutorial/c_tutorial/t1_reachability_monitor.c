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

    char *record_as_string = ddlog_dump_record(rec);
    if (record_as_string == NULL) {
        fprintf(stderr, "failed to dump record\n");
        exit(EXIT_FAILURE);
    }
    printf("Record: %s\n", record_as_string);
    // Records returned as strings using `ddlog_dump_record()`
    // API call should be deallocated to avoid memory leaks
    ddlog_string_free(record_as_string);

    return true;
}

int main(int args, char **argv)
{
    // Start the DDlog program and connect to it
    ddlog_prog prog = ddlog_run(1, true, NULL, NULL);
    if (prog == NULL) {
        fprintf(stderr, "failed to initialize DDlog program\n");
        exit(EXIT_FAILURE);
    };

    // Get table IDs for `Links` and `ConnectedNodes` relations
    table_id LinksTableID = ddlog_get_table_id("Links");
    table_id ConnectedNodesTableID = ddlog_get_table_id("ConnectedNodes");
    printf("Links table ID: %lu\n", LinksTableID);
    printf("ConnectedNodes table ID: %lu\n", ConnectedNodesTableID);

    char *src_line_ptr = NULL;
    char *dst_line_ptr = NULL;
    char *link_status_line_ptr = NULL;
    size_t n_src = 0;
    size_t n_dst = 0;
    size_t n_link_status = 0;
    bool link_status = false;

    // Prompt user to enter record values
    // and collect them from the standard input
    printf("Please enter source name > ");
    if (getline(&src_line_ptr, &n_src, stdin) < 0) {
        if (src_line_ptr != NULL) free(src_line_ptr);
        return -EINVAL;
    }
    printf("Please enter destination name > ");
    if (getline(&dst_line_ptr, &n_dst, stdin) < 0) {
        free(src_line_ptr);
        if (dst_line_ptr != NULL) free(dst_line_ptr);
        return -EINVAL;
    }
    printf("Please enter the link status between source and destination > ");
    if (getline(&link_status_line_ptr, &n_link_status, stdin) < 0) {
        free(src_line_ptr);
        free(dst_line_ptr);
        if (link_status_line_ptr != NULL) free(link_status_line_ptr);
        return -EINVAL;
    }

    // Trimming newline characters
    src_line_ptr[strlen(src_line_ptr) - 1] = '\0';
    dst_line_ptr[strlen(dst_line_ptr) - 1] = '\0';
    // Parsing value for the link status
    // Anything different from `true` will be considered as `false`
    if (strlen(link_status_line_ptr) == 5) {
        link_status = (strncmp("true", link_status_line_ptr, 4) == 0) ? true : false;
    }

    // Creating record values in the DDlog format
    ddlog_record *src = ddlog_string(src_line_ptr);
    ddlog_record *dst = ddlog_string(dst_line_ptr);
    ddlog_record *lstatus = ddlog_bool(link_status);

    // Constructing a single record from separate values
    ddlog_record **struct_args;
    struct_args = (ddlog_record**)malloc(3 * sizeof(ddlog_record*));
    struct_args[0] = src;
    struct_args[1] = dst;
    struct_args[2] = lstatus;
    ddlog_record *new_record = ddlog_struct("Links", struct_args, 3);

    // Let's print the record that we are about to insert
    // to the `Links` relation
    char *record_to_insert_as_string = ddlog_dump_record(new_record);
    printf("Inserting the following record: %s\n", record_to_insert_as_string);
    ddlog_string_free(record_to_insert_as_string);

    // Start transaction
    if (ddlog_transaction_start(prog) < 0) {
        fprintf(stderr, "failed to start transaction\n");
        exit(EXIT_FAILURE);
    };

    // Create `insert` command
    ddlog_cmd *cmd = ddlog_insert_cmd(LinksTableID, new_record);
    if (cmd == NULL) {
        fprintf(stderr, "failed to create insert command\n");
        exit(EXIT_FAILURE);
    }

    // Apply updates to the relation with records
    // specified in the provided command `cmd`
    if (ddlog_apply_updates(prog, &cmd, 1) < 0) {
        fprintf(stderr, "failed to apply updates\n");
        exit(EXIT_FAILURE);
    };

    // Commit transaction
    if (ddlog_transaction_commit(prog) < 0) {
        fprintf(stderr, "failed to commit transaction\n");
        exit(EXIT_FAILURE);
    };

    // Print records in the `ConnectedNodes` relation
    printf("Content of the ConnectedNodes relation:\n");
    ddlog_dump_table(prog, ConnectedNodesTableID, &print_records_callback, (uintptr_t)(void*)(NULL));

    // Freeing memory
    ddlog_free(struct_args);
    free(src_line_ptr);
    free(dst_line_ptr);
    free(link_status_line_ptr);

    // Stop the DDlog program
    if (ddlog_stop(prog) < 0) {
        fprintf(stderr, "failed to stop DDlog program\n");
        exit(EXIT_FAILURE);
    }

    return EXIT_SUCCESS;
}
