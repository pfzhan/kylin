---
title: Quick Start
language: en
sidebar_label: Quick Start
pagination_label: Quick Start
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: overview
pagination_next: quickstart/tutorial
keywords:
    - Deploy
draft: false
last_update:
    date: 09/13/2024
---

In this guide, we will explain how to quickly install and start Kylin 5. Before you begin, ensure you have met the [Prerequisites](../deployment/on-premises/prerequisite.md).

## <span id="docker">Play Kylin in docker</span>

To explore new features in Kylin 5 on a laptop, we recommend pulling the Docker image and checking the [Apache Kylin Standalone Image on Docker Hub](https://hub.docker.com/r/apachekylin/apache-kylin-standalone).

```shell
docker pull apachekylin/apache-kylin-standalone:5.0.0-GA
```

## <span id="install">Install Kylin in Single Node</span>

1. Get Kylin installation package.

   Please download official release binary from [Download Page](../download.md) . <br></br>
   For developer who want to package from source code, please refer to [How To Package](../development/how_to_package.md).

2. Decide the installation location and the Linux account to run Kylin. All the examples below are based on the following assumptions:

   - The installation location is `/usr/local/`
   - Linux account to run Kylin is `KyAdmin`. It is called the **Linux account** hereafter.
   - **For all commands in the rest of the document**, please replace the above parameters with your real installation location and Linux account. 

3. Copy and uncompress Kylin software package to your server or virtual machine.

   ```shell
   cd /usr/local
   tar -zxvf apache-kylin-[Version].tar.gz
   ```
   The decompressed directory is referred to as `$KYLIN_HOME` or **root directory**.

4. Download Spark

   ```shell
   bash $KYLIN_HOME/sbin/download-spark-user.sh
   ```
   After executing above script, there will be a `spark` directory under `$KYLIN_HOME` .

5. Prepare RDBMS metastore.

   If you have PostgreSQL or MySQL installed in your environment, you can use either as a metastore. Follow the links below for installation and configuration instructions:

    * [Use PostgreSQL as Metastore](../deployment/on-premises/rdbms_metastore/usepg_as_metadb.md).
    * [Use MySQL as Metastore](../deployment/on-premises/rdbms_metastore/use_mysql_as_metadb.md).

   For production environments, we strongly recommend setting up a dedicated metastore using either PostgreSQL or MySQL to ensure reliability.

6. Install InfluxDB(**Deprecated**).

   Kylin utilizes InfluxDB to store system monitoring data. This step is optional, but highly recommended for production environments to leverage monitoring capabilities.
   
   ```sh
   # download influxdb
   $KYLIN_HOME/sbin/download-influxdb.sh
   
   cd $KYLIN_HOME/influxdb
   
   # install influxdb
   rpm -ivh influxdb-1.6.5.x86_64.rpm
   ```
   
   For more details, please refer to [Use InfluxDB as Time-Series Database](../operations/system-monitoring/influxdb/influxdb.md).
   
7. Create a working directory on HDFS and grant permissions.

   The default working directory is `/kylin`. Also ensure the Linux account has access to its home directory on HDFS. Meanwhile, create directory `/kylin/spark-history` to store the spark log files.

   ```sh
   hadoop fs -mkdir -p /kylin
   hadoop fs -chown root /kylin
   hadoop fs -mkdir -p /kylin/spark-history
   hadoop fs -chown root /kylin/spark-history
   ```

   If necessary, you can modify the path of the Kylin working directory in `$KYLIN_HOME/conf/kylin.properties`.

   **Note**: If you do not have the permission to create `/kylin/spark-history`, you can configure `kylin.engine.spark-conf.spark.eventLog.dir` and `kylin.engine.spark-conf.spark.history.fs.logDirectory` with an available directory.

### <span id="configuration">Quick Configuration</span>

In the `conf` directory under the root directory of the installation package, you should configure the parameters in the file `kylin.properties` as follows:

1. According to the PostgreSQL configuration, configure the following metadata parameters. Pay attention to replace the corresponding ` {metadata_name} `, `{host} `, ` {port} `, ` {user} `, ` {password} ` value, the maximum length of `metadata_name` allowed is 28.

   ```properties
   kylin.metadata.url={metadata_name}@jdbc,driverClassName=org.postgresql.Driver,url=jdbc:postgresql://{host}:{port}/kylin,username={user},password={password}
   ```
   For more PostgreSQL configuration, please refer to [Use PostgreSQL as Metastore](../deployment/on-premises/rdbms_metastore/usepg_as_metadb.md). For information for MySQL configuration, please refer to [Use MySQL as Metastore](../deployment/on-premises/rdbms_metastore/use_mysql_as_metadb.md). 

   > **Note**: please name the `{metadata_name}` with letters, numbers, or underscores. The name can't start with numbers, such as `1a` is illegal and `a1` is legal.

2. When executing jobs, Kylin will submit the build task to Yarn. You can set and replace `{queue}` in the following parameters as the queue you actually use, and require the build task to be submitted to the specified queue.

   ```properties
   kylin.engine.spark-conf.spark.yarn.queue={queue_name}
   ```


3. Configure the ZooKeeper service.

   Kylin uses ZooKeeper for service discovery, which will ensure that when an instance starts, stops, or unexpectedly interrupts communication during cluster deployment, other instances in the cluster can automatically discover and update the status. For more details, pleaser refer to [Service Discovery](../deployment/on-premises/deploy_mode/service_discovery.md).
   
   Please add ZooKeeper's connection configuration `kylin.env.zookeeper-connect-string=host:port`. You can modify the cluster address and port according to the following example.
   
   ```properties
   kylin.env.zookeeper-connect-string=10.1.2.1:2181,10.1.2.2:2181,10.1.2.3:2181
   ```
   
   If you use ACL for Zookeeper, need setting the follow configuration

   | Properties                                                  | Description                                                                                                          |
   | ------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
   | kylin.env.zookeeper-acl-enabled                             | Whether to enable Zookeeper ACL. The default value is disabled.                                                      |
   | kylin.env.zookeeper.zk-auth                                 | The user password and authentication method used by Zookeeper. The default value is empty.                           |
   | kylin.env.zookeeper.zk-acl                                  | ACL permission setting. The default value is `world:anyone:rwcda`. By default, all users can perform all operations. |

   If you need to encrypt kylin.env.zookeeper.zk-auth , you can do it like this：

   **i.** run following commands in `${KYLIN_HOME}`, it will print encrypted value
    ```
    ./bin/kylin.sh org.apache.kylin.tool.general.CryptTool -e AES -s <value>
    ```
   **ii.** config  kylin.env.zookeeper.zk-auth like this
    ```
    kylin.env.zookeeper.zk-auth=ENC('${encrypted_value}')
    ```

4. (optional) Configure Spark Client node information
   Since Spark is started in yarn-client mode, if the IP information of Kylin is not configured in the hosts file of the Hadoop cluster, please add the following configurations in `kylin.properties`:
    `kylin.storage.columnar.spark-conf.spark.driver.host={hostIp}`
    `kylin.engine.spark-conf.spark.driver.host={hostIp}`

  You can modify the `{hostIp}` according to the following example:
  ```properties
  kylin.storage.columnar.spark-conf.spark.driver.host=10.1.3.71
  kylin.engine.spark-conf.spark.driver.host=10.1.3.71
  ```




### <span id="start">Start Kylin</span>

1. Check the version of `curl`.

   Since `check-env.sh` needs to rely on the support of GSS-Negotiate during the installation process, it is recommended that you check the relevant components of your curl first. You can use the following commands in your environment:

   ```shell
   curl --version
   ```
   If GSS-Negotiate is displayed in the interface, the curl version is available. If not, you can reinstall curl or add GSS-Negotiate support.
   ![Check GSS-Negotiate dependency](images/gss_negotiate.png)

2. Start Kylin with the startup script.
   Run the following command to start Kylin. When it is first started, the system will run a series of scripts to check whether the system environment has met the requirements. For details, please refer to the [Environment Dependency Check](../operations/system-operation/cli_tool/environment_dependency_check.md) chapter.
   
   ```shell
   ${KYLIN_HOME}/bin/kylin.sh start
   ```
   > **Note**：If you want to observe the detailed startup progress, run:
   >
   > ```shell
   > tail -f $KYLIN_HOME/logs/kylin.log
   > ```
   

Once the startup is completed, you will see information prompt in the console. Run the command below to check the Kylin process at any time.

   ```shell
   ps -ef | grep kylin
   ```

3. Get login information.

   After the startup script has finished, the random password of the default user `ADMIN` will be displayed on the console. You are highly recommended to save this password. If this password is accidentally lost, please refer to [ADMIN User Reset Password](../operations/access-control/user_management.md).

### <span id="use">How to Use</span>

After Kylin is started, open web GUI at `http://{host}:7070/kylin`. Please replace `host` with your host name, IP address, or domain name. The default port is `7070`. 

The default user name is `ADMIN`. The random password generated by default will be displayed on the console when Kylin is started for the first time. After the first login, please reset the administrator password according to the password rules.

- At least 8 characters.
- Contains at least one number, one letter, and one special character ```(~!@#$%^&*(){}|:"<>?[];',./`)```.

Kylin uses the open source **SSB** (Star Schema Benchmark) dataset for star schema OLAP scenarios as a test dataset. You can verify whether the installation is successful by running a script to import the SSB dataset into Hive. The SSB dataset is from multiple CSV files.

**Import Sample Data**

Run the following command to import the sample data:

```shell
$KYLIN_HOME/bin/sample.sh
```

The script will create 1 database **SSB** and 6 Hive tables then import data into it.

After running successfully, you should be able to see the following information in the console:

```shell
Sample hive tables are created successfully
```


We will be using SSB dataset as the data sample to introduce Kylin in several sections of this product manual. The SSB dataset simulates transaction data for the online store, see more details in [Sample Dataset](./tutorial.md#ssb). Below is a brief introduction.


| Table       | Description                           | Introduction                                                 |
| ----------- | ------------------------------------- | ------------------------------------------------------------ |
| CUSTOMER    | customer information                  | includes customer name, address, contact information .etc.   |
| DATES       | order date                            | includes a order's specific date, week, month, year .etc.    |
| LINEORDER   | order information                     | includes some basic information like order date, order amount, order revenue, supplier ID, commodity ID, customer Id .etc. |
| PART        | product information                   | includes some basic information like product name, category, brand .etc. |
| P_LINEORDER | view based on order information table | includes all content in the order information table and new content in the view |
| SUPPLIER    | supplier information                  | includes supplier name, address, contact information .etc.   |


**Validate Product Functions**

You can create a sample project and model according to [Kylin 5 Tutorial](tutorial.md). The project should validate basic features such as source table loading, model creation, index build etc. 

On the **Data Asset -> Model** page, you should see an example model with some storage over 0.00 KB, this indicates the data has been loaded for this model.

![model list](images/list.png)

On the **Monitor** page, you can see all jobs have been completed successfully in **Batch Job** pages. 

![job monitor](images/job.png)

**Validate Query Analysis**

When the metadata is loaded successfully, at the **Insight** page, 6 sample hive tables would be shown at the left panel. User could input query statements against these tables. For example, the SQL statement queries different product group by order date, and in descending order by total revenue: 

```sql
SELECT LO_PARTKEY, SUM(LO_REVENUE) AS TOTAL_REVENUE
FROM SSB.P_LINEORDER
WHERE LO_ORDERDATE between '1993-06-01' AND '1994-06-01' 
group by LO_PARTKEY
order by SUM(LO_REVENUE) DESC 
```


The query result will be displayed at the **Insight** page, showing that the query hit the sample model.

![query result](images/installation_query_result.png)

You can also use the same SQL statement to query on Hive to verify the result and performance.



### <span id="stop">Stop Kylin</span>

Run the following command to stop Kylin:

```shell
$KYLIN_HOME/bin/kylin.sh stop
```

You can run the following command to check if the Kylin process has stopped.

```shell
ps -ef | grep kylin
```

### <span id="faq">FAQ</span>

**Q: How do I change the service default port?**

You can modify the following configuration in the `$KYLIN_HOME/conf/kylin.properties`, here is an example for setting the server port to 7070.

```properties
server.port=7070
```

**Q: Is the query pushdown engine turned on by default?**

Yes, if you want to turn it off, please refer to [Pushdown to SparkSQL](../query/principles/push_down.md).

