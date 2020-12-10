#!/bin/bash

##
## Copyright (C) 2020 Kyligence Inc. All rights reserved.
##
## http://kyligence.io
##
## This software is the confidential and proprietary information of
## Kyligence Inc. ("Confidential Information"). You shall not disclose
## such Confidential Information and shall use it only in accordance
## with the terms of the license agreement you entered into with
## Kyligence Inc.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
## A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
## OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
## SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
## LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
## THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
## OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##

dir=$(dirname ${0})
cd ${dir}/../..

export PACKAGE_TIMESTAMP=1
export PACKAGE_SPARK=1
export SKIP_FRONT=0
export SKIP_OBF=0
for PARAM in $@; do
    if [[ "$PARAM" == "-noTimestamp" ]]; then
        echo "Package without timestamp..."
        export PACKAGE_TIMESTAMP=0
        shift
    fi

    if [[ "$PARAM" == "-noSpark" ]]; then
        echo "Skip packaging Spark..."
        export PACKAGE_SPARK=0
        shift
    fi

    if [[ "$PARAM" == "-skipObf" ]]; then
        echo "Skip Obfuscation..."
        export SKIP_OBF=1
        shift
    fi

    if [[ "$PARAM" == "-skipFront" ]]; then
        echo 'Skip install front-end dependencies...'
        export SKIP_FRONT=1
        shift
    fi
done

if [[ -z ${release_version} ]]; then
    release_version='staging'
fi
if [[ "${PACKAGE_TIMESTAMP}" = "1" ]]; then
    timestamp=`date '+%Y%m%d%H%M%S'`
    export release_version=${release_version}.${timestamp}
fi
export package_name="Kyligence-Enterprise-${release_version}"

sh build/script_newten/package.sh $@

echo "Kyligence Enterprise Release Version: ${release_version}"

package_name="Kyligence-Enterprise-${release_version}.tar.gz"
if [[ -f LICENSE ]]; then
    kap_dir=`tar -tf dist/${package_name}|head -1`
    rm -rf $kap_dir
    mkdir $kap_dir
    cp LICENSE $kap_dir
    gzip -d dist/${package_name}
    tar_name=`ls dist/Kyligence-Enterprise*.tar`
    tar -uf ${tar_name} ${kap_dir%/}/LICENSE
    gzip ${tar_name}
    rm -rf $kap_dir
fi

sha256sum dist/$package_name > dist/${package_name}.sha256sum
echo "sha256: `cat dist/${package_name}.sha256sum`"