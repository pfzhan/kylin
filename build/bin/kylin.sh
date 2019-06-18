
#!/bin/bash
# Kyligence Inc. License
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh $@
mkdir -p ${KYLIN_HOME}/logs
ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
OUT_LOG=${KYLIN_HOME}/logs/shell.stdout
${dir}/rotate-logs.sh $@
echo "-----------------------  log start  -----------------------" >>${ERR_LOG}
echo "-----------------------  log start  -----------------------" >>${OUT_LOG}
bash -x ${KYLIN_HOME}/bin/bootstrap.sh $@ 2>>${ERR_LOG}  | tee -a ${OUT_LOG}
echo "-----------------------  log end  -------------------------" >>${ERR_LOG}
echo "-----------------------  log end  -------------------------" >>${OUT_LOG}