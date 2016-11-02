#!/bin/bash

# ============================================================================

kapbase=kap21
kylinbase=yang21
suffix="cdh5.7"

# ============================================================================


git fetch origin
cd kylin
git fetch apache
git fetch origin

cd kylin
git checkout ${kylinbase}-${suffix}
git reset origin/${kylinbase}-${suffix} --hard

cd ..
git checkout ${kapbase}-${suffix}
git reset origin/${kapbase}-${suffix} --hard

