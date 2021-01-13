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
        return EXIT_FAILURE;
    };

    // Print IDs of tables corresponding to `Links` and `ConnectedNodes` relations
    table_id LinksTableID = ddlog_get_table_id("Links");
    table_id ConnectedNodesTableID = ddlog_get_table_id("ConnectedNodes");
    printf("Links table id: %lu\n", LinksTableID);
    printf("ConnectedNodes table id: %lu\n", ConnectedNodesTableID);

    char *src_line_ptr = NULL;
    char *dst_line_ptr = NULL;
    char *link_status_line_ptr = NULL;
    size_t n_src = 0;
    size_t n_dst = 0;
    size_t n_link_status = 0;
    bool link_status = false;

    // Getting new record values from standard input
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
    if (strlen(link_status_line_ptr) == 5) {
        link_status = (strncmp("true", link_status_line_ptr, 4) == 0) ? true : false;
    }

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

    char *record_to_insert_as_string = ddlog_dump_record(new_record);
    printf("Inserting the following record: %s\n", record_to_insert_as_string);
    ddlog_string_free(record_to_insert_as_string);

    // Start transaction
    if (ddlog_transaction_start(prog) < 0) {
        fprintf(stderr, "failed to start transaction\n");
        return EXIT_FAILURE;
    };

    // Apply updates
    ddlog_cmd *cmd = ddlog_insert_cmd(LinksTableID, new_record);
    if (cmd == NULL) {
        fprintf(stderr, "failed to create insert command\n");
        return EXIT_FAILURE;
    }
    if (ddlog_apply_updates(prog, &cmd, 1) < 0) {
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
    printf("ConnectedNodes Table Contents:\n");
    ddlog_dump_table(prog, ConnectedNodesTableID, &print_records_callback, (uintptr_t)(void*)(NULL));

    // Stopping DDlog program
    if (ddlog_stop(prog) < 0) {
        fprintf(stderr, "failed to stop DDlog program\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
