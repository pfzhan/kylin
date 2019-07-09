
#!/bin/bash
# Kyligence Inc. License
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh $@
mkdir -p ${KYLIN_HOME}/logs
ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout
${KYLIN_HOME}/sbin/rotate-logs.sh $@
echo "-----------------------  log start  -----------------------" >>${ERR_LOG}
echo "-----------------------  log start  -----------------------" >>${OUT_LOG}
bash -x ${KYLIN_HOME}/sbin/bootstrap.sh $@ 2>>${ERR_LOG}  | tee -a ${OUT_LOG}
ret=${PIPESTATUS[0]}
echo "-----------------------  log end  -------------------------" >>${ERR_LOG}
echo "-----------------------  log end  -------------------------" >>${OUT_LOG}
exit ${ret}