#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
if [ "$1" == "-v" ]; then
    shift
fi

# avoid re-entering
if [[ "$CHECKENV_ING" == "" ]]; then
    export CHECKENV_ING=true

    mkdir -p ${KYLIN_HOME}/logs
    LOG=${KYLIN_HOME}/logs/check-env.out
    ERRORS=${KYLIN_HOME}/logs/check-env.error
    BYPASS=${dir}/check-env-bypass
    TITLE="#title"
    
    if [[ "$1" != "if-not-yet" || ! -f ${BYPASS} ]]; then
    
        echo ""
        echo `setColor 33 "KAP is checking installation environment, log is at ${LOG}"`
        echo ""

        rm -rf ${KYLIN_HOME}/logs/tmp
        rm -f ${LOG}
        rm -f ${ERRORS}
        touch ${ERRORS}
        
        export CHECKENV_REPORT_PFX=">   "
        export QUIT_MESSAGE_LOG=${ERRORS}
        
        for f in ${dir}/check-*.sh
        do
            if [[ ! $f == *check-env.sh ]]; then
                echo `getValueByKey ${TITLE} ${f}`
                echo ""                                                                             >>${LOG}
                echo "============================================================================" >>${LOG}
                echo "Checking $(basename $f)"                                                      >>${LOG}
                echo "----------------------------------------------------------------------------" >>${LOG}
                bash $f >>${LOG} 2>&1
                if [[ $? != 0 ]]; then
                    echo "...................................................[`setColor 31 FAIL`]"
                    cat  ${ERRORS} >> ${LOG}
                    tail ${ERRORS}
                    echo `setColor 33 "Full log is at: ${LOG}"`
                    exit 1
                else
                    echo "...................................................[`setColor 32 PASS`]"
                fi
            fi
        done
        echo ""
        cat ${LOG} | grep "${CHECKENV_REPORT_PFX}"
        touch ${BYPASS}
        echo `setColor 33 "Checking environment was successful and is now suppressed. To check again, run 'bin/check-env.sh' manually."`
        echo ""
    fi
    
    export CHECKENV_ING=
fi