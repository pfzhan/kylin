#!/bin/bash
# Kyligence Inc. License

function killTree() {
    local parent=$1 child
    for child in $(ps -o ppid= -o pid= | awk "\$1==$parent {print \$2}"); do
        killTree $child
    done
    kill $parent
}

killTree $1