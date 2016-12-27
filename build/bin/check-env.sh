#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

mkdir -p ${KYLIN_HOME}/logs
LOG=${KYLIN_HOME}/logs/check-env.out
BYPASS=${dir}/check-env-bypass

if [[ "$1" != "if-not-yet" || ! -f ${BYPASS} ]]; then

    echo "Checking environment, log is at ${LOG}"
    rm -f ${LOG}

    for f in ${dir}/check-*.sh
    do
        if [[ ! $f == *check-env.sh ]]; then
            echo "Running $(basename $f)"
            echo ""                                                                             >>${LOG}
            echo "============================================================================" >>${LOG}
            echo "Running $(basename $f)"                                                       >>${LOG}
            echo "----------------------------------------------------------------------------" >>${LOG}
            sh $f >>${LOG} 2>&1
            if [[ $? != 0 ]]; then
                echo "----------------------------------------------------------------------------"
                echo "Tail of ${LOG} is"
                tail ${LOG}
                quit "ERROR: $(basename $f) failed, full log is at ${LOG}"
            fi
        fi
    done
    
    cat ${LOG} | grep "WARN"
    
    touch ${BYPASS}
    echo "Checking environment was successful and is now suppressed. To check again, run 'bin/check-env.sh' manually."
fi