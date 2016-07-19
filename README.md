Kyligence Analytics Platform
=====
This is source code repository for Kyligence Analytics Platform(KAP), owned by Kyligence Inc.

To setup development environment:

1. Clone repository to your local disk:
> git clone https://github.com/Kyligence/KAP.git


2. Switch to `master` branch
> git checkout master

3. Sync kylin module
> git submodule update --init kylin

Or you can just clone from code repository with one go:
> git clone --recursive https://github.com/Kyligence/KAP.git

To build,

1. go to extensions/storage-parquet-protocol and run "mvn clean install -DskipTests"

2. go to kap home and run "mvn clean install -DskipTests"