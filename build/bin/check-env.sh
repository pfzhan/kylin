#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

mkdir -p ${KYLIN_HOME}/logs
LOG=${KYLIN_HOME}/logs/check-env.out
BYPASS=${dir}/check-env-bypass

if [[ "$1" != "if-not-yet" || ! -f ${BYPASS} ]]; then

    echo "Checking environment, log is at ${LOG}"
    echo ""
    rm -f ${LOG}
    
    export CHECKENV_REPORT_PFX=">   "

    for f in ${dir}/check-*.sh
    do
        if [[ ! $f == *check-env.sh ]]; then
            echo "Checking $(basename $f)"
            echo ""                                                                             >>${LOG}
            echo "============================================================================" >>${LOG}
            echo "Checking $(basename $f)"                                                       >>${LOG}
            echo "----------------------------------------------------------------------------" >>${LOG}
            bash $f >>${LOG} 2>&1
            if [[ $? != 0 ]]; then
                echo "----------------------------------------------------------------------------"
                echo "Tail of ${LOG} is"
                tail ${LOG}
                quit "ERROR: $(basename $f) failed, full log is at ${LOG}"
            else
                echo "......................................[`setColor 32 PASS`]"
            fi
        fi
    done
    echo ""
    echo "----------------------- Report ------------------------"
    cat ${LOG} | grep ${CHECKENV_REPORT_PFX}
    
    touch ${BYPASS}
    echo "Checking environment was successful and is now suppressed. To check again, run 'bin/check-env.sh' manually."
fi