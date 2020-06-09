#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh
if [ "$1" == "-v" ]; then
    shift
fi

KYLIN_ENV_CHANNEL=`$KYLIN_HOME/bin/get-properties.sh kylin.env.channel`
export KYLIN_CONFIG_FILE="${KYLIN_HOME}/conf/kylin.properties"
export SPARK_HOME=${KYLIN_HOME}/spark

# avoid re-entering
if [[ "$CHECKENV_ING" == "" ]]; then
    export CHECKENV_ING=true

    mkdir -p ${KYLIN_HOME}/logs
    LOG=${KYLIN_HOME}/logs/check-env.out
    ERRORS=${KYLIN_HOME}/logs/check-env.error
    BYPASS=${KYLIN_HOME}/bin/check-env-bypass
    TITLE="#title"

    if [[ "$1" != "if-not-yet" || ! -f ${BYPASS} ]]; then

        echo ""
        echo `setColor 33 "Kyligence Enterprise is checking installation environment, log is at ${LOG}"`
        echo ""

        rm -rf ${KYLIN_HOME}/logs/tmp
        rm -f ${LOG}
        rm -f ${ERRORS}
        touch ${ERRORS}

        export CHECKENV_REPORT_PFX=">   "
        export QUIT_MESSAGE_LOG=${ERRORS}

        CHECK_FILES=
        if [[ ${KYLIN_ENV_CHANNEL} == "on-premises" || -z ${KYLIN_ENV_CHANNEL} ]]; then
            CHECK_FILES=`ls ${KYLIN_HOME}/sbin/check-*.sh`
        else
            CHECK_FILES=("${KYLIN_HOME}/sbin/check-1400-java.sh"
                         "${KYLIN_HOME}/sbin/check-1500-ports.sh")
        fi
        for f in ${CHECK_FILES}
        do
            if [[ ! $f == *check-env.sh ]]; then
                echo `getValueByKey ${TITLE} ${f}`
                echo ""                                                                             >>${LOG}
                echo "============================================================================" >>${LOG}
                echo "Checking $(basename $f)"                                                      >>${LOG}
                echo "----------------------------------------------------------------------------" >>${LOG}
                bash $f >>${LOG} 2>&1
                rtn=$?
                if [[ $rtn == 0 ]]; then
                    echo "...................................................[`setColor 32 PASS`]"
                elif [[ $rtn == 3 ]]; then
                    echo "...................................................[`setColor 33 SKIP`]"
                else
                    echo "...................................................[`setColor 31 FAIL`]"
                    cat  ${ERRORS} >> ${LOG}
                    tail ${ERRORS}
                    echo `setColor 33 "Full log is at: ${LOG}"`
                    exit 1
                fi
            fi
        done
        echo ""
        cat ${LOG} | grep "^${CHECKENV_REPORT_PFX}"
        touch ${BYPASS}
        echo `setColor 33 "Checking environment finished successfully. To check again, run 'bin/check-env.sh' manually."`
        echo ""
    fi

    export CHECKENV_ING=
fi