---
title: "Ranger Integrate"
sidebar_position: 7
toc: true
last_modified_at: 2021-12-23T18:25:57-04:00
---

This guide provides quick setup for integrate with Ranger

## Build Ranger Spark plugin from source

For spark 2.4, ranger 2.1 and scala 2.11

```bash
git clone https://github.com/izhangzhihao/spark-security.git
cd spark-security
mvn clean package -Dmaven.javadoc.skip=true -DskipTests -pl :submarine-spark-security -Pspark-2.4 -Pranger-2.1
```

You can found the jar file in `spark-security/target/submarine-spark-security-0.7.0-SNAPSHOT.jar`

## Setup testing env

### Settings for Ranger

Create `ranger-spark-security.xml` in `$SPARK_HOME/conf` and add the following configurations
for pointing to the right Apache Ranger admin server.


```xml

<configuration>

    <property>
        <name>ranger.plugin.spark.policy.rest.url</name>
        <value>https://ranger.com:port</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.rest.ssl.config.file</name>
        <value>/etc/spark/conf/ranger-spark-policymgr-ssl.xml</value>
    </property>

    <property>
        <name>ranger.plugin.spark.service.name</name>
        <value>cm_hive</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.cache.dir</name>
        <value>/tmp</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.pollIntervalMs</name>
        <value>5000</value>
    </property>

    <property>
        <name>ranger.plugin.spark.policy.source.impl</name>
        <value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>
    </property>

</configuration>
```

Create `ranger-spark-audit.xml` in `$SPARK_HOME/conf` and add the following configurations
to enable/disable auditing.

```xml
<configuration>
    <property>
        <name>xasecure.audit.is.enabled</name>
        <value>true</value>
    </property>
</configuration>
```

Create `ranger-spark-policymgr-ssl.xml` in `$SPARK_HOME/conf`.

```xml
<configuration xmlns:xi="http://www.w3.org/2001/XInclude">
	<property>
    <name>xasecure.policymgr.clientssl.truststore</name>
    <value>/home/bigdatauser/cm-auto-global_truststore.jks</value>
  </property>
  <property>
    <name>xasecure.policymgr.clientssl.truststore.credential.file</name>
    <value>jceks://file/home/bigdatauser/ranger-truststore.jceks</value>
  </property>
  <property>
    <name>xasecure.policymgr.clientssl.keystore</name>
    <value>/home/bigdatauser/cm-auto-host_keystore.jks</value>
  </property>
  <property>
    <name>xasecure.policymgr.clientssl.keystore.credential.file</name>
    <value>jceks://file/home/bigdatauser/ranger-keystore.jceks</value>
  </property>
  <property>
    <name>xasecure.policymgr.clientssl.keystore.type</name>
    <value>jks</value>
  </property>
  <property>
    <name>xasecure.policymgr.clientssl.truststore.type</name>
    <value>jks</value>
  </property>
</configuration>
```

### Generate `jceks` file

```bash
java -cp "/opt/cloudera/parcels/CDH/lib/ranger-hive-plugin/install/lib/*" org.apache.ranger.credentialapi.buildks create sslKeyStore -value 'yourpassword' -provider jceks://file/home/bigdatauser/ranger-keystore.jceks
java -cp "/opt/cloudera/parcels/CDH/lib/ranger-hive-plugin/install/lib/*" org.apache.ranger.credentialapi.buildks create sslTrustStore -value 'yourpassword' -provider jceks://file/home/bigdatauser/ranger-truststore.jceks
```

### Config Ranger

skipped

### Testsing using `spark-shell` or `spark-sql`

```bash
spark-shell --master yarn --deploy-mode client --conf spark.sql.extensions=org.apache.submarine.spark.security.api.RangerSparkSQLExtension --jars=submarine-spark-security-0.7.0-SNAPSHOT.jar --driver-class-path=slib/*
```

```bash
spark-sql --master yarn --deploy-mode client --conf spark.sql.extensions=org.apache.submarine.spark.security.api.RangerSparkSQLExtension --jars=submarine-spark-security-0.7.0-SNAPSHOT.jar --driver-class-path=slib/*
```

### Testing using `spark-submit`

```bash
spark-submit --master yarn  --deploy-mode client --conf spark.sql.extensions=org.apache.submarine.spark.security.api.RangerSparkSQLExtension --jars=/opt/cloudera/parcels/CDH-7.1.6-1.cdh7.1.6.p0.10506313/jars/hive-common-3.1.3000.7.1.6.0-297.jar,/opt/cloudera/parcels/CDH-7.1.6-1.cdh7.1.6.p0.10506313/jars/hive-metastore-3.1.3000.7.1.6.0-297.jar,submarine-spark-security-0.7.0-SNAPSHOT.jar --driver-class-path=slib/* --class com.github.sharpdata.sharpetl.spark.Entrypoint hdfs:///user/admin/demo_workflow/spark-1.0.0-SNAPSHOT.jar single-job --name=test --period=10 --env=qa --once --skip-running=false --property=hdfs:///user/admin/etl-conf/etl.properties
```

Succeful logs:

```log
21/12/23 10:10:12 INFO config.RangerConfiguration: addResourceIfReadable(ranger-spark-audit.xml): resource file is file:/etc/spark/conf.cloudera.spark_on_yarn/ranger-spark-audit.xml
21/12/23 10:10:12 INFO config.RangerConfiguration: addResourceIfReadable(ranger-spark-security.xml): resource file is file:/etc/spark/conf.cloudera.spark_on_yarn/ranger-spark-security.xml
21/12/23 10:10:12 INFO config.RangerConfiguration: addResourceIfReadable(ranger-spark-policymgr-ssl.xml): resource file is file:/etc/spark/conf.cloudera.spark_on_yarn/ranger-spark-policymgr-ssl.xml
21/12/23 10:10:12 ERROR config.RangerConfiguration: addResourceIfReadable(ranger-spark-cm_hive-audit.xml): couldn't find resource file location
21/12/23 10:10:12 ERROR config.RangerConfiguration: addResourceIfReadable(ranger-spark-cm_hive-security.xml): couldn't find resource file location
21/12/23 10:10:12 ERROR config.RangerConfiguration: addResourceIfReadable(ranger-spark-cm_hive-policymgr-ssl.xml): couldn't find resource file location
21/12/23 10:10:12 INFO config.RangerPluginConfig: PolicyEngineOptions: { evaluatorType: auto, evaluateDelegateAdminOnly: false, disableContextEnrichers: false, disableCustomConditions: false, disableTagPolicyEvaluation: false, enableTagEnricherWithLocalRefresher: false, disableTrieLookupPrefilter: false, optimizeTrieForRetrieval: false, cacheAuditResult: false }
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AuditProviderFactory: creating..
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AuditProviderFactory: initializing..
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.truststore=/home/bigdatauser/cm-auto-global_truststore.jks
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.source.impl=org.apache.ranger.admin.client.RangerAdminRESTClient
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.service.name=cm_hive
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.cache.dir=/tmp
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.keystore.type=jks
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.rest.ssl.config.file=/etc/spark/conf/ranger-spark-policymgr-ssl.xml
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.rest.url=https://ranger.com:port/
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.keystore.credential.file=jceks://file/home/bigdatauser/ranger-keystore.jceks
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.audit.is.enabled=true
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.pollIntervalMs=5000
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.keystore=/home/bigdatauser/cm-auto-host_keystore.jks
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.truststore.type=jks
21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.truststore.credential.file=jceks://file/home/bigdatauser/ranger-truststore.jceks
21/12/23 10:10:12 INFO provider.AuditProviderFactory: No v3 audit configuration found. Trying v2 audit configurations
21/12/23 10:10:12 INFO provider.AuditProviderFactory: RangerAsyncAuditCleanup: Waiting to audit cleanup start signal
21/12/23 10:10:12 INFO service.RangerBasePlugin: Created PolicyRefresher Thread(PolicyRefresher(serviceName=cm_hive)-125)
21/12/23 10:10:13 INFO util.RangerRolesProvider: RangerRolesProvider(serviceName=cm_hive): found updated version. lastKnownRoleVersion=-1; newVersion=9
21/12/23 10:10:13 INFO util.PolicyRefresher: PolicyRefresher(serviceName=cm_hive): found updated version. lastKnownVersion=-1; newVersion=189
21/12/23 10:10:13 INFO policyengine.RangerPolicyRepository: This policy engine contains 23 policy evaluators
21/12/23 10:10:13 INFO conditionevaluator.RangerScriptConditionEvaluator: ScriptEngine for engineName=[JavaScript] is successfully created
21/12/23 10:10:13 INFO policyengine.RangerPolicyRepository: This policy engine contains 1 policy evaluators
21/12/23 10:10:13 INFO contextenricher.RangerTagEnricher: Created RangerTagRefresher Thread(RangerTagRefresher(serviceName=cm_hive)-130)
21/12/23 10:10:14 INFO contextenricher.RangerTagEnricher: There are no tagged resources for service cm_hive
21/12/23 10:10:14 INFO contextenricher.RangerTagEnricher$RangerTagRefresher: RangerTagRefresher(serviceName=cm_hive).populateTags() - Updated tags-cache to new version of tags, lastKnownVersion=-1; newVersion=1
21/12/23 10:10:14 INFO security.RangerSparkPlugin$: Policy cache directory successfully set to /tmp
```