#!/bin/bash
lib_dir=$1

rm -rf ${lib_dir}/kap
mkdir ${lib_dir}/kap
cd ${lib_dir}/kap

for in_jar in `ls ../kylin-*.jar` `ls ../kap-*.jar`;do
    unzip -o -qq $in_jar
    rm $in_jar
    rm -rf META-INF
done

jar cvf kap.jar *
mv kap.jar ../

cd ..
rm -rf kap
