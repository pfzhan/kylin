#!/bin/bash
# Kyligence Inc. License

function exportKRB5CCNAME() {
    KAP_KERBEROS_CACHE=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.cache`
    export KRB5CCNAME=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_CACHE}
}

function initKerberos() {
    KAP_KERBEROS_PRINCIPAL=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.principal`
    KAP_KERBEROS_KEYTAB=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.keytab`
    KAP_KERBEROS_KEYTAB_PATH=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_KEYTAB}
    echo "Kerberos is enabled, init..."
    kinit -kt $KAP_KERBEROS_KEYTAB_PATH $KAP_KERBEROS_PRINCIPAL
}

function initKerberosIfNeeded(){
    KAP_KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.enabled`
    if [[ "${KAP_KERBEROS_ENABLED}" == "true" ]]
    then
        export SKIP_KERB=1
        if [[ -z "$(command -v klist)" ]]
        then
             quit "Kerberos command not found! Please check configuration of Kerberos in kylin.properties or check Kerberos installation."
        fi

        exportKRB5CCNAME

        if ! klist -s
        then
            initKerberos
        else
           echo "Kerberos ticket is valid, skip init."
        fi

        # check if kerberos init success
        if ! klist -s
        then
            quit "Kerberos ticket is not valid, please run 'kinit -kt <keytab_path> <principal>' manually or set configuration of Kerberos in kylin.properties."
        fi
    fi
}