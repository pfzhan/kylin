#!/usr/bin/env bash

cd $(dirname ${0})/../..
dir=`pwd`

#create python virtual venv
python_env=$dir/venv
if [ -d ${python_env} ]
then
    echo "python virtual venv exists"
else
    python3 -m venv ${python_env}
fi

#update python packages
source ${python_env}/bin/activate
pip3 install -r requirements.txt
deactivate
