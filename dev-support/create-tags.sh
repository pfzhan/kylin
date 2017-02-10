#!/usr/bin/env bash

VERSION=$1

KAP_REMOTE=origin
KYLIN_REMOTE=backup

# prepare
git fetch --all
git branch -D tmp
git checkout -b tmp

cd kylin
git remote remove $KYLIN_REMOTE
git remote add $KYLIN_REMOTE https://github.com/Kyligence/apache-kylin-backup.git
git fetch --all
git checkout master
git reset origin/master --hard
git push $KYLIN_REMOTE master
git checkout master-hbase0.98
git reset origin/master-hbase0.98 --hard
git push -f $KYLIN_REMOTE master-hbase0.98
cd ..

# clean up
git tag -d kap-$VERSION
git tag -d kap-$VERSION-hbase0.98
git push $KAP_REMOTE :kap-$VERSION
git push $KAP_REMOTE :kap-$VERSION-hbase0.98
cd kylin
git tag -d kylin-$VERSION
git tag -d kylin-$VERSION-hbase0.98
git push $KYLIN_REMOTE :kylin-$VERSION
git push $KYLIN_REMOTE :kylin-$VERSION-hbase0.98
cd ..

# create tags for master branch
git reset $KAP_REMOTE/master --hard
git submodule update
git tag kap-$VERSION
git push $KAP_REMOTE kap-$VERSION

cd kylin
git tag kylin-$VERSION
git push $KYLIN_REMOTE kylin-$VERSION
cd ..

# create tags for master branch
git reset $KAP_REMOTE/master-hbase0.98 --hard
git submodule update
git tag kap-$VERSION-hbase0.98
git push $KAP_REMOTE kap-$VERSION-hbase0.98

cd kylin
git tag kylin-$VERSION-hbase0.98
git push $KYLIN_REMOTE kylin-$VERSION-hbase0.98
cd ..

# tear down
git checkout master
git branch -D tmp