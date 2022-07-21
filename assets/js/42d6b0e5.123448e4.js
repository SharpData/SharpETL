"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[651],{3905:(e,r,n)=>{n.d(r,{Zo:()=>c,kt:()=>d});var a=n(7294);function t(e,r,n){return r in e?Object.defineProperty(e,r,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[r]=n,e}function i(e,r){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);r&&(a=a.filter((function(r){return Object.getOwnPropertyDescriptor(e,r).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var r=1;r<arguments.length;r++){var n=null!=arguments[r]?arguments[r]:{};r%2?i(Object(n),!0).forEach((function(r){t(e,r,n[r])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(r){Object.defineProperty(e,r,Object.getOwnPropertyDescriptor(n,r))}))}return e}function s(e,r){if(null==e)return{};var n,a,t=function(e,r){if(null==e)return{};var n,a,t={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],r.indexOf(n)>=0||(t[n]=e[n]);return t}(e,r);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],r.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(t[n]=e[n])}return t}var l=a.createContext({}),p=function(e){var r=a.useContext(l),n=r;return e&&(n="function"==typeof e?e(r):o(o({},r),e)),n},c=function(e){var r=p(e.components);return a.createElement(l.Provider,{value:r},e.children)},u={inlineCode:"code",wrapper:function(e){var r=e.children;return a.createElement(a.Fragment,{},r)}},g=a.forwardRef((function(e,r){var n=e.components,t=e.mdxType,i=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),g=p(n),d=t,m=g["".concat(l,".").concat(d)]||g[d]||u[d]||i;return n?a.createElement(m,o(o({ref:r},c),{},{components:n})):a.createElement(m,o({ref:r},c))}));function d(e,r){var n=arguments,t=r&&r.mdxType;if("string"==typeof e||t){var i=n.length,o=new Array(i);o[0]=g;var s={};for(var l in r)hasOwnProperty.call(r,l)&&(s[l]=r[l]);s.originalType=e,s.mdxType="string"==typeof e?e:t,o[1]=s;for(var p=2;p<i;p++)o[p]=n[p];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}g.displayName="MDXCreateElement"},3902:(e,r,n)=>{n.r(r),n.d(r,{assets:()=>l,contentTitle:()=>o,default:()=>u,frontMatter:()=>i,metadata:()=>s,toc:()=>p});var a=n(7462),t=(n(7294),n(3905));const i={title:"Ranger Integrate",sidebar_position:7,toc:!0,last_modified_at:new Date("2021-12-23T22:25:57.000Z")},o=void 0,s={unversionedId:"ranger-integrate",id:"ranger-integrate",title:"Ranger Integrate",description:"This guide provides quick setup for integrate with Ranger",source:"@site/docs/ranger-integrate.md",sourceDirName:".",slug:"/ranger-integrate",permalink:"/SharpETL/docs/ranger-integrate",draft:!1,editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/docs/ranger-integrate.md",tags:[],version:"current",sidebarPosition:7,frontMatter:{title:"Ranger Integrate",sidebar_position:7,toc:!0,last_modified_at:"2021-12-23T22:25:57.000Z"},sidebar:"docs",previous:{title:"transformer guide",permalink:"/SharpETL/docs/transformer-guide"}},l={},p=[{value:"Build Ranger Spark plugin from source",id:"build-ranger-spark-plugin-from-source",level:2},{value:"Setup testing env",id:"setup-testing-env",level:2},{value:"Settings for Ranger",id:"settings-for-ranger",level:3},{value:"Generate <code>jceks</code> file",id:"generate-jceks-file",level:3},{value:"Config Ranger",id:"config-ranger",level:3},{value:"Testsing using <code>spark-shell</code> or <code>spark-sql</code>",id:"testsing-using-spark-shell-or-spark-sql",level:3},{value:"Testing using <code>spark-submit</code>",id:"testing-using-spark-submit",level:3}],c={toc:p};function u(e){let{components:r,...n}=e;return(0,t.kt)("wrapper",(0,a.Z)({},c,n,{components:r,mdxType:"MDXLayout"}),(0,t.kt)("p",null,"This guide provides quick setup for integrate with Ranger"),(0,t.kt)("h2",{id:"build-ranger-spark-plugin-from-source"},"Build Ranger Spark plugin from source"),(0,t.kt)("p",null,"For spark 2.4, ranger 2.1 and scala 2.11"),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-bash"},"git clone https://github.com/izhangzhihao/spark-security.git\ncd spark-security\nmvn clean package -Dmaven.javadoc.skip=true -DskipTests -pl :submarine-spark-security -Pspark-2.4 -Pranger-2.1\n")),(0,t.kt)("p",null,"You can found the jar file in ",(0,t.kt)("inlineCode",{parentName:"p"},"spark-security/target/submarine-spark-security-0.7.0-SNAPSHOT.jar")),(0,t.kt)("h2",{id:"setup-testing-env"},"Setup testing env"),(0,t.kt)("h3",{id:"settings-for-ranger"},"Settings for Ranger"),(0,t.kt)("p",null,"Create ",(0,t.kt)("inlineCode",{parentName:"p"},"ranger-spark-security.xml")," in ",(0,t.kt)("inlineCode",{parentName:"p"},"$SPARK_HOME/conf")," and add the following configurations\nfor pointing to the right Apache Ranger admin server."),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-xml"},"\n<configuration>\n\n    <property>\n        <name>ranger.plugin.spark.policy.rest.url</name>\n        <value>https://ranger.com:port</value>\n    </property>\n\n    <property>\n        <name>ranger.plugin.spark.policy.rest.ssl.config.file</name>\n        <value>/etc/spark/conf/ranger-spark-policymgr-ssl.xml</value>\n    </property>\n\n    <property>\n        <name>ranger.plugin.spark.service.name</name>\n        <value>cm_hive</value>\n    </property>\n\n    <property>\n        <name>ranger.plugin.spark.policy.cache.dir</name>\n        <value>/tmp</value>\n    </property>\n\n    <property>\n        <name>ranger.plugin.spark.policy.pollIntervalMs</name>\n        <value>5000</value>\n    </property>\n\n    <property>\n        <name>ranger.plugin.spark.policy.source.impl</name>\n        <value>org.apache.ranger.admin.client.RangerAdminRESTClient</value>\n    </property>\n\n</configuration>\n")),(0,t.kt)("p",null,"Create ",(0,t.kt)("inlineCode",{parentName:"p"},"ranger-spark-audit.xml")," in ",(0,t.kt)("inlineCode",{parentName:"p"},"$SPARK_HOME/conf")," and add the following configurations\nto enable/disable auditing."),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-xml"},"<configuration>\n    <property>\n        <name>xasecure.audit.is.enabled</name>\n        <value>true</value>\n    </property>\n</configuration>\n")),(0,t.kt)("p",null,"Create ",(0,t.kt)("inlineCode",{parentName:"p"},"ranger-spark-policymgr-ssl.xml")," in ",(0,t.kt)("inlineCode",{parentName:"p"},"$SPARK_HOME/conf"),"."),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-xml"},'<configuration xmlns:xi="http://www.w3.org/2001/XInclude">\n    <property>\n    <name>xasecure.policymgr.clientssl.truststore</name>\n    <value>/home/bigdatauser/cm-auto-global_truststore.jks</value>\n  </property>\n  <property>\n    <name>xasecure.policymgr.clientssl.truststore.credential.file</name>\n    <value>jceks://file/home/bigdatauser/ranger-truststore.jceks</value>\n  </property>\n  <property>\n    <name>xasecure.policymgr.clientssl.keystore</name>\n    <value>/home/bigdatauser/cm-auto-host_keystore.jks</value>\n  </property>\n  <property>\n    <name>xasecure.policymgr.clientssl.keystore.credential.file</name>\n    <value>jceks://file/home/bigdatauser/ranger-keystore.jceks</value>\n  </property>\n  <property>\n    <name>xasecure.policymgr.clientssl.keystore.type</name>\n    <value>jks</value>\n  </property>\n  <property>\n    <name>xasecure.policymgr.clientssl.truststore.type</name>\n    <value>jks</value>\n  </property>\n</configuration>\n')),(0,t.kt)("h3",{id:"generate-jceks-file"},"Generate ",(0,t.kt)("inlineCode",{parentName:"h3"},"jceks")," file"),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-bash"},"java -cp \"/opt/cloudera/parcels/CDH/lib/ranger-hive-plugin/install/lib/*\" org.apache.ranger.credentialapi.buildks create sslKeyStore -value 'yourpassword' -provider jceks://file/home/bigdatauser/ranger-keystore.jceks\njava -cp \"/opt/cloudera/parcels/CDH/lib/ranger-hive-plugin/install/lib/*\" org.apache.ranger.credentialapi.buildks create sslTrustStore -value 'yourpassword' -provider jceks://file/home/bigdatauser/ranger-truststore.jceks\n")),(0,t.kt)("h3",{id:"config-ranger"},"Config Ranger"),(0,t.kt)("p",null,"skipped"),(0,t.kt)("h3",{id:"testsing-using-spark-shell-or-spark-sql"},"Testsing using ",(0,t.kt)("inlineCode",{parentName:"h3"},"spark-shell")," or ",(0,t.kt)("inlineCode",{parentName:"h3"},"spark-sql")),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-bash"},"spark-shell --master yarn --deploy-mode client --conf spark.sql.extensions=org.apache.submarine.spark.security.api.RangerSparkSQLExtension --jars=submarine-spark-security-0.7.0-SNAPSHOT.jar --driver-class-path=slib/*\n")),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-bash"},"spark-sql --master yarn --deploy-mode client --conf spark.sql.extensions=org.apache.submarine.spark.security.api.RangerSparkSQLExtension --jars=submarine-spark-security-0.7.0-SNAPSHOT.jar --driver-class-path=slib/*\n")),(0,t.kt)("h3",{id:"testing-using-spark-submit"},"Testing using ",(0,t.kt)("inlineCode",{parentName:"h3"},"spark-submit")),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-bash"},"spark-submit --master yarn  --deploy-mode client --conf spark.sql.extensions=org.apache.submarine.spark.security.api.RangerSparkSQLExtension --jars=/opt/cloudera/parcels/CDH-7.1.6-1.cdh7.1.6.p0.10506313/jars/hive-common-3.1.3000.7.1.6.0-297.jar,/opt/cloudera/parcels/CDH-7.1.6-1.cdh7.1.6.p0.10506313/jars/hive-metastore-3.1.3000.7.1.6.0-297.jar,submarine-spark-security-0.7.0-SNAPSHOT.jar --driver-class-path=slib/* --class com.github.sharpdata.sharpetl.spark.Entrypoint hdfs:///user/admin/demo_workflow/spark-1.0.0-SNAPSHOT.jar single-job --name=test --period=10 --env=qa --once --skip-running=false --property=hdfs:///user/admin/etl-conf/etl.properties\n")),(0,t.kt)("p",null,"Succeful logs:"),(0,t.kt)("pre",null,(0,t.kt)("code",{parentName:"pre",className:"language-log"},"21/12/23 10:10:12 INFO config.RangerConfiguration: addResourceIfReadable(ranger-spark-audit.xml): resource file is file:/etc/spark/conf.cloudera.spark_on_yarn/ranger-spark-audit.xml\n21/12/23 10:10:12 INFO config.RangerConfiguration: addResourceIfReadable(ranger-spark-security.xml): resource file is file:/etc/spark/conf.cloudera.spark_on_yarn/ranger-spark-security.xml\n21/12/23 10:10:12 INFO config.RangerConfiguration: addResourceIfReadable(ranger-spark-policymgr-ssl.xml): resource file is file:/etc/spark/conf.cloudera.spark_on_yarn/ranger-spark-policymgr-ssl.xml\n21/12/23 10:10:12 ERROR config.RangerConfiguration: addResourceIfReadable(ranger-spark-cm_hive-audit.xml): couldn't find resource file location\n21/12/23 10:10:12 ERROR config.RangerConfiguration: addResourceIfReadable(ranger-spark-cm_hive-security.xml): couldn't find resource file location\n21/12/23 10:10:12 ERROR config.RangerConfiguration: addResourceIfReadable(ranger-spark-cm_hive-policymgr-ssl.xml): couldn't find resource file location\n21/12/23 10:10:12 INFO config.RangerPluginConfig: PolicyEngineOptions: { evaluatorType: auto, evaluateDelegateAdminOnly: false, disableContextEnrichers: false, disableCustomConditions: false, disableTagPolicyEvaluation: false, enableTagEnricherWithLocalRefresher: false, disableTrieLookupPrefilter: false, optimizeTrieForRetrieval: false, cacheAuditResult: false }\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AuditProviderFactory: creating..\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AuditProviderFactory: initializing..\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.truststore=/home/bigdatauser/cm-auto-global_truststore.jks\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.source.impl=org.apache.ranger.admin.client.RangerAdminRESTClient\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.service.name=cm_hive\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.cache.dir=/tmp\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.keystore.type=jks\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.rest.ssl.config.file=/etc/spark/conf/ranger-spark-policymgr-ssl.xml\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.rest.url=https://ranger.com:port/\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.keystore.credential.file=jceks://file/home/bigdatauser/ranger-keystore.jceks\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.audit.is.enabled=true\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: ranger.plugin.spark.policy.pollIntervalMs=5000\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.keystore=/home/bigdatauser/cm-auto-host_keystore.jks\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.truststore.type=jks\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: AUDIT PROPERTY: xasecure.policymgr.clientssl.truststore.credential.file=jceks://file/home/bigdatauser/ranger-truststore.jceks\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: No v3 audit configuration found. Trying v2 audit configurations\n21/12/23 10:10:12 INFO provider.AuditProviderFactory: RangerAsyncAuditCleanup: Waiting to audit cleanup start signal\n21/12/23 10:10:12 INFO service.RangerBasePlugin: Created PolicyRefresher Thread(PolicyRefresher(serviceName=cm_hive)-125)\n21/12/23 10:10:13 INFO util.RangerRolesProvider: RangerRolesProvider(serviceName=cm_hive): found updated version. lastKnownRoleVersion=-1; newVersion=9\n21/12/23 10:10:13 INFO util.PolicyRefresher: PolicyRefresher(serviceName=cm_hive): found updated version. lastKnownVersion=-1; newVersion=189\n21/12/23 10:10:13 INFO policyengine.RangerPolicyRepository: This policy engine contains 23 policy evaluators\n21/12/23 10:10:13 INFO conditionevaluator.RangerScriptConditionEvaluator: ScriptEngine for engineName=[JavaScript] is successfully created\n21/12/23 10:10:13 INFO policyengine.RangerPolicyRepository: This policy engine contains 1 policy evaluators\n21/12/23 10:10:13 INFO contextenricher.RangerTagEnricher: Created RangerTagRefresher Thread(RangerTagRefresher(serviceName=cm_hive)-130)\n21/12/23 10:10:14 INFO contextenricher.RangerTagEnricher: There are no tagged resources for service cm_hive\n21/12/23 10:10:14 INFO contextenricher.RangerTagEnricher$RangerTagRefresher: RangerTagRefresher(serviceName=cm_hive).populateTags() - Updated tags-cache to new version of tags, lastKnownVersion=-1; newVersion=1\n21/12/23 10:10:14 INFO security.RangerSparkPlugin$: Policy cache directory successfully set to /tmp\n")))}u.isMDXComponent=!0}}]);