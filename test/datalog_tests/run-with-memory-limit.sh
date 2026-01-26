#!/bin/bash
# Helper script to run a command with memory limit if MEMORY_LIMIT_GB is set
# Can be used as: 
#   1. Executable: ./run-with-memory-limit.sh <command> [args...]
#   2. Sourced function: source run-with-memory-limit.sh; run_with_memory_limit <command> [args...]

run_with_memory_limit() {
    if [ -n "${MEMORY_LIMIT_GB}" ]; then
        MEMORY_LIMIT_BYTES=$((MEMORY_LIMIT_GB * 1024 * 1024 * 1024))
        # Use systemd-run to create a transient scope with memory limit
        systemd-run --user --scope \
            --property=MemoryMax=${MEMORY_LIMIT_BYTES} \
            --property=MemoryHigh=${MEMORY_LIMIT_BYTES} \
            --property=MemorySwapMax=0 \
            --quiet \
            -- "$@"
    else
        # No memory limit, run normally
        "$@"
    fi
}

# If script is executed directly (not sourced), run the command
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    run_with_memory_limit "$@"
fi
