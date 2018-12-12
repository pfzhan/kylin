#!/bin/bash

if [ -n ${KYLIN_HOME} ]; then
    java -cp ${KYLIN_HOME}/tool/kap-tool-*.jar io.kyligence.kap.tool.MetadataTool $@
else
    echo "please set KYLIN_HOME"
fi