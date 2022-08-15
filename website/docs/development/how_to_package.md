---
sidebar_position: 1
---

# How to package

### Environment Requirement

| Software      | Comment                                      |    Version     |
|---------------| ---------------------------------------------|----------------|
| Git           |  Fetch branch name and hash of latest commit | latest         |
| Apache Maven  |  Build Java and Scala source code            | 3.8.2 or latest         |  
| Node.js       |  Build front end                             | 12.14.0 is recommended ( or 12.x ~ 14.x) |


### Options for Packaging Script

|         Option       |     Comment                                        | 
|--------------------  | ---------------------------------------------------|
| -official            | If add this option, package name contains timestamp| 
| -noThirdParty        | If add this option, third party binary be packaging into binary, current they are influxdb,grafana and postgresql |
| -noSpark             | If add this option, spark won't packaging into Kylin binary |
| -noHive1             | By default kylin 5.0 will support Hive 1.2, if add this option, this binary will support Hive 2.3+ |
| -skipFront           | If add this option, front-end won't be build and packaging |
| -skipCompile         | Add this option will assume java source code no need be compiled again |

### Package Content

|         Option       |     Comment    | 
|--------------------  | ---------------|
| VERSION              | `Apache Kylin ${release_version}`  |
| commit_SHA1          | `${HASH_COMMIT}@${BRANCH_NAME}`    |

### Package Name convention

Package name is `apache-kylin-${release_version}.tar.gz`, while `${release_version}` is `{project.version}.YYYYmmDDHHMMSS` by default.
For example, an unofficial package could be `apache-kylin-5.0.0-SNAPSHOT.20220812161045.tar.gz` while an official package could be `apache-kylin-5.0.0.tar.gz`

### Example for developer and release manager

```shell

## Case 1: For developer who want to package for testing purpose
./build/release/release.sh 

## Case 2: Official apache release,  kylin binary for deploy on Hadoop3+ and Hive2.3+, 
# and third party cannot be distributed because of apache distribution policy(size and license)
./build/release/release.sh -noSpark -official 
```

### How to switch to older node.js

If you install node.js which is higher than 14.X, I recommended you downgrade to lower version with some tools like https://github.com/nvm-sh/nvm.

```shell
## switch to specific version
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
nvm install 12.14.0
nvm use 12.14.0

## switch to original version
nvm use system
```