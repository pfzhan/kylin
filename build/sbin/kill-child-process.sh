#!/bin/bash
# Kyligence Inc. License


function help_func {
    echo "Usage: kill-child-process.sh [SIG](the default signal is '-SIGTERM') <PID>(process id)"
    echo "       kill-child-process.sh -15 12345"
    echo "       kill-child-process.sh -SIGTERM 12345"
    echo "       kill-child-process.sh -9 12345"
    echo "       kill-child-process.sh -SIGKILL 12345"
    exit 1
}

function killTree() {
    local parent=$1 child
    for child in $(ps ax -o ppid= -o pid= | awk "\$1==$parent {print \$2}"); do
        killTree ${child}
    done
    kill ${signal} ${parent}
}

if [[ $# -eq 1 ]]; then
    signal='-15'
    process_id=$1
elif [[ $# -eq 2 ]]; then
    signal=$1
    process_id=$2
else
    help_func
fi

killTree ${process_id}