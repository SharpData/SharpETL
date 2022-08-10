"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[361],{3905:(e,a,t)=>{t.d(a,{Zo:()=>s,kt:()=>k});var n=t(7294);function r(e,a,t){return a in e?Object.defineProperty(e,a,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[a]=t,e}function o(e,a){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);a&&(n=n.filter((function(a){return Object.getOwnPropertyDescriptor(e,a).enumerable}))),t.push.apply(t,n)}return t}function i(e){for(var a=1;a<arguments.length;a++){var t=null!=arguments[a]?arguments[a]:{};a%2?o(Object(t),!0).forEach((function(a){r(e,a,t[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(a){Object.defineProperty(e,a,Object.getOwnPropertyDescriptor(t,a))}))}return e}function l(e,a){if(null==e)return{};var t,n,r=function(e,a){if(null==e)return{};var t,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)t=o[n],a.indexOf(t)>=0||(r[t]=e[t]);return r}(e,a);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)t=o[n],a.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(r[t]=e[t])}return r}var p=n.createContext({}),m=function(e){var a=n.useContext(p),t=a;return e&&(t="function"==typeof e?e(a):i(i({},a),e)),t},s=function(e){var a=m(e.components);return n.createElement(p.Provider,{value:a},e.children)},c={inlineCode:"code",wrapper:function(e){var a=e.children;return n.createElement(n.Fragment,{},a)}},d=n.forwardRef((function(e,a){var t=e.components,r=e.mdxType,o=e.originalType,p=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),d=m(t),k=r,N=d["".concat(p,".").concat(k)]||d[k]||c[k]||o;return t?n.createElement(N,i(i({ref:a},s),{},{components:t})):n.createElement(N,i({ref:a},s))}));function k(e,a){var t=arguments,r=a&&a.mdxType;if("string"==typeof e||r){var o=t.length,i=new Array(o);i[0]=d;var l={};for(var p in a)hasOwnProperty.call(a,p)&&(l[p]=a[p]);l.originalType=e,l.mdxType="string"==typeof e?e:r,i[1]=l;for(var m=2;m<o;m++)i[m]=t[m];return n.createElement.apply(null,i)}return n.createElement.apply(null,t)}d.displayName="MDXCreateElement"},2323:(e,a,t)=>{t.r(a),t.d(a,{assets:()=>p,contentTitle:()=>i,default:()=>c,frontMatter:()=>o,metadata:()=>l,toc:()=>m});var n=t(7462),r=(t(7294),t(3905));const o={title:"transformer guide",sidebar_position:10,toc:!0,last_modified_at:new Date("2021-12-23T22:25:57.000Z")},i=void 0,l={unversionedId:"transformer-guide",id:"transformer-guide",title:"transformer guide",description:"transformer\u7684\u5b9a\u4e49\u548c\u4f7f\u7528",source:"@site/docs/transformer-guide.md",sourceDirName:".",slug:"/transformer-guide",permalink:"/SharpETL/docs/transformer-guide",draft:!1,editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/docs/transformer-guide.md",tags:[],version:"current",sidebarPosition:10,frontMatter:{title:"transformer guide",sidebar_position:10,toc:!0,last_modified_at:"2021-12-23T22:25:57.000Z"},sidebar:"docs",previous:{title:"batch-job-guide",permalink:"/SharpETL/docs/batch-job-guide"},next:{title:"UDF guide",permalink:"/SharpETL/docs/UDF-guide"}},p={},m=[{value:"transformer\u7684\u5b9a\u4e49\u548c\u4f7f\u7528",id:"transformer\u7684\u5b9a\u4e49\u548c\u4f7f\u7528",level:2},{value:"\u81ea\u5b9a\u4e49transformer\u7684\u4f7f\u7528",id:"\u81ea\u5b9a\u4e49transformer\u7684\u4f7f\u7528",level:3},{value:"\u5982\u4f55\u81ea\u5b9a\u4e49transformer",id:"\u5982\u4f55\u81ea\u5b9a\u4e49transformer",level:3},{value:"\u52a0\u8f7d\u5916\u90e8transformer",id:"\u52a0\u8f7d\u5916\u90e8transformer",level:3},{value:"Pro tips",id:"pro-tips",level:3}],s={toc:m};function c(e){let{components:a,...t}=e;return(0,r.kt)("wrapper",(0,n.Z)({},s,t,{components:a,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"transformer\u7684\u5b9a\u4e49\u548c\u4f7f\u7528"},"transformer\u7684\u5b9a\u4e49\u548c\u4f7f\u7528"),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"transformer"),"\u662f\u7528\u4e8e\u6ee1\u8db3\u7528\u6237\u5bf9\u4e8e\u7279\u5b9a\u573a\u666f\u4e0b\u901a\u8fc7\u4ee3\u7801\u903b\u8f91\u5b9e\u73b0\u7684\u6269\u5c55\u8bc9\u6c42\uff0c\u901a\u8fc7\u53cd\u5c04\u52a0\u8f7djar\u6216\u8005\u6587\u672c\u6587\u4ef6\u4e2d\u7684scala\u4ee3\u7801\u3002",(0,r.kt)("inlineCode",{parentName:"p"},"transformer"),"\u5141\u8bb8\u5728job step\u4e2d\u6267\u884c\u4e00\u6bb5\u4ee3\u7801\u5757\uff0c\u901a\u8fc7java class path\u6216\u8005\u6587\u4ef6\u540d\u6765\u533a\u5206\u4e0d\u540c\u7684",(0,r.kt)("inlineCode",{parentName:"p"},"transformer"),"\uff0c\u4e0d\u540c\u7684",(0,r.kt)("inlineCode",{parentName:"p"},"transformer"),"\u4f1a\u6709\u4e0d\u540c\u7684\u81ea\u5b9a\u4e49\u53c2\u6570\uff0c\u5982\uff1a"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.HttpTransformer\n--  methodName=transform\n--  transformerType=object\n--  url=https:xxxx\n--  connectionName=connection_demo\n--  fieldName=centerIds\n--  jsonPath=$.centers[*].id\n--  splitBy=,\n-- target=variables\n-- checkPoint=false\n-- dateRangeInterval=0\n")),(0,r.kt)("p",null,"\u4ee5HttpTransformer\u4e3a\u4f8b\uff0c\u8be5transformer\u662f\u5c06\u4eceapi\u83b7\u5f97\u7684json\u7c7b\u578b\u6570\u636e\u8fdb\u884c\u89e3\u6790\u548c\u843d\u8868\uff0c\u8fd8\u652f\u6301url\u4e2d\u7684\u52a8\u6001\u4f20\u53c2\uff0c\u53ef\u4ee5\u91c7\u7528variables\u5b9a\u4e49\u5177\u4f53\u53c2\u6570\u5e76\u8c03\u7528\u3002\u5728\u6240\u6709\u4f7f\u7528transformer\u7684step\u4e2d\uff0c",(0,r.kt)("inlineCode",{parentName:"p"},"dataSourceType=transformation, methodName=transform, transformerType=object"),"\uff0cclassName\u5219\u4e3a\u5b9a\u4e49transformer\u7684\u7c7b\u540d\uff0c\u9664\u6b64\u4e4b\u5916\u7684",(0,r.kt)("inlineCode",{parentName:"p"},"url, connectionName, fieldName, jsonPath, splitBy"),"\u5219\u4e3ahttpTransformer\u7684\u81ea\u5b9a\u4e49\u53c2\u6570\u3002"),(0,r.kt)("h3",{id:"\u81ea\u5b9a\u4e49transformer\u7684\u4f7f\u7528"},"\u81ea\u5b9a\u4e49transformer\u7684\u4f7f\u7528"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"JdbcResultSetTransformer"),": \u8be5transformer\u4e3b\u8981\u7528\u4e8e\u5bf9source\u6570\u636e\u5e93\u4e2d\u7684\u8868\u6267\u884c",(0,r.kt)("inlineCode",{parentName:"p"},"insert"),"\uff0c",(0,r.kt)("inlineCode",{parentName:"p"},"update"),"\uff0c",(0,r.kt)("inlineCode",{parentName:"p"},"delete"),"\u7b49\u65e0\u8fd4\u56de\u503c\u7684\u64cd\u4f5c"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcResultSetTransformer\n--  dbType=yellowbrick\n--  dbName=bigdata\n--  methodName=transform\n-- target=do_nothing\n-- checkPoint=false\n-- dateRangeInterval=0\ndelete from demo_table where to_char(\"HIST_DT\", 'yyyyMMdd') = '${TODAY}';\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"dataSourceType"),", ",(0,r.kt)("inlineCode",{parentName:"li"},"className"),"\u548c",(0,r.kt)("inlineCode",{parentName:"li"},"methodName"),"\u4e0e\u524d\u6587\u4fdd\u6301\u4e00\u81f4"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"dbType"),"\u548c",(0,r.kt)("inlineCode",{parentName:"li"},"dbName"),"\u7528\u4e8e\u6784\u5efajdbc\u8fde\u63a5\u53c2\u6570\uff0c",(0,r.kt)("inlineCode",{parentName:"li"},"dbType"),"\u4e3a",(0,r.kt)("inlineCode",{parentName:"li"},"jdbc transformer"),"\u7684\u81ea\u5b9a\u4e49\u53c2\u6570"),(0,r.kt)("li",{parentName:"ul"},"\u8be5step\u8868\u793a\u4ece",(0,r.kt)("inlineCode",{parentName:"li"},"demo_table"),"\u4e2d\u5220\u9664\u5f53\u5929\u7684\u6570\u636e"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"DDLTransformer"),": \u8be5transformer\u4e3b\u8981\u7528\u4e8e\u901a\u8fc7\u5efa\u8868\u8bed\u53e5\u5728",(0,r.kt)("inlineCode",{parentName:"p"},"hive"),"\u6216\u8005",(0,r.kt)("inlineCode",{parentName:"p"},"yellobrick"),"\u5efa\u8868\uff0c\u9700\u8981\u4f20\u5165\u5efa\u8868\u8bed\u53e5\u7684\u8def\u5f84"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.DDLTransformer\n--  methodName=transform\n--  transformerType=object\n--  dbName=bigdata\n--  dbType=yellowbrick\n--  ddlPath=/demo_ddl\n-- target=do_nothing\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"dbType"),"&",(0,r.kt)("inlineCode",{parentName:"li"},"dbName"),": \u7528\u4e8e\u6784\u5efajdbc\u8fde\u63a5\uff0c\u76ee\u524d\u53ea\u652f\u6301\u901a\u8fc7ddl\u5728hive\u548cyb\u5efa\u8868"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"ddlPath"),": \u9ed8\u8ba4\u503c\u4e3a",(0,r.kt)("inlineCode",{parentName:"li"},"/user/hive/sharp-etl/ddl"),"\uff0c\u4e3a\u5b58\u653eddl\u7684\u5177\u4f53\u8def\u5f84"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"JobDependencyCheckTransformer"),": \u8be5transformer\u4e3b\u8981\u7528\u4e8e\u5bf9job\u8fd0\u884c\u65f6\u4e0a\u6e38\u4f9d\u8d56job\u662f\u5426\u8fd0\u884c\u7ed3\u675f\u7684\u68c0\u6d4b\uff0c\u9700\u8981\u8f93\u5165",(0,r.kt)("inlineCode",{parentName:"p"},"decencies"),"\u548c",(0,r.kt)("inlineCode",{parentName:"p"},"jobName"),"\uff0c\u68c0\u67e5\u8be5jobName\u662f\u5426\u6709\u4f9d\u8d56job\u672a\u8fd0\u884c\u5b8c\u6210"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.JobDependencyCheckTransformer\n--  methodName=transform\n--  transformerType=object\n--  dependencies=job1,job2,job3\n--  jobName=test_dependency_demo\n-- target=do_nothing\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"dependencies"),": \u4e0a\u6e38\u4f9d\u8d56job\u7684\u540d\u79f0\uff0c",(0,r.kt)("inlineCode",{parentName:"li"},"jobName"),"\u5373\u4e3a\u9700\u8981\u68c0\u6d4b\u7684job\u540d"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"FileCleanTransformer"),": \u8be5transformer\u5c06\u5220\u9664\u76ee\u6807\u8def\u5f84\u4e0b\u56fa\u5b9a\u683c\u5f0f\u7684\u6587\u4ef6\uff0c\u9700\u8981\u8f93\u5165",(0,r.kt)("inlineCode",{parentName:"p"},"filePath"),"\u548c",(0,r.kt)("inlineCode",{parentName:"p"},"fileNamePattern"),"\uff0c\u540e\u8005\u652f\u6301\u6b63\u5219\u8868\u8fbe\u5f0f"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.FileCleanTransformer\n--  methodName=transform\n--  transformerType=object\n--  filePath=test_fileClean\n--  fileNamePattern=((\\w*_test_fileClean.txt))\n-- target=do_nothing\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"filePath"),": \u6587\u4ef6\u5b58\u50a8\u8def\u5f84"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"fileNamePattern"),": \u6587\u4ef6\u540d\uff0c\u4e5f\u53ef\u4f20\u5165\u6b63\u5219\u8868\u8fbe\u5f0f"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"DropExternalTableTransformer"),": \u8be5transformer\u5c06\u5220\u9664hive\u548chdfs\u4e2d\u4ee5",(0,r.kt)("inlineCode",{parentName:"p"},"tableNamePrefix"),"\u5f00\u5934\u7684\u5728",(0,r.kt)("inlineCode",{parentName:"p"},"databaseName"),"\u6570\u636e\u5e93\u4e2d\u76f8\u5e94",(0,r.kt)("inlineCode",{parentName:"p"},"partition"),"\u7684\u8868\u548c\u6587\u4ef6, ",(0,r.kt)("inlineCode",{parentName:"p"},"partition"),"\u652f\u6301\u52a8\u6001\u4f20\u53c2\uff0c\u53ef\u4ee5\u4e0e",(0,r.kt)("inlineCode",{parentName:"p"},"variables"),"\u7ed3\u5408\u4f7f\u7528"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- stepId=2\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.DropExternalTableTransformer\n--  methodName=transform\n--  transformerType=object\n--  databaseName=testDB\n--  tableNamePrefix=pre_ods__\n--  partition=year=${YEAR},month=${MONTH},day=${7_DAYS_BEFORE}\n-- target=do_nothing\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"partition"),": \u9700\u8981\u5220\u9664\u7684\u5206\u533a\uff0c\u4f1a\u5220\u9664",(0,r.kt)("inlineCode",{parentName:"li"},"day=${7_DAYS_BEFORE}"),"\u53737\u5929\u524d\u7684\u5206\u533a\u6587\u4ef6"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"tableNamePrefix"),": \u9700\u8981\u5220\u9664\u7684\u8868\u524d\u7f00"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"DailyJobsSummaryReportTransformer"),": \u8be5transformer\u4e3b\u8981\u7528\u4e8e\u53d1\u9001\u9644\u4ef6\u4e3a",(0,r.kt)("inlineCode",{parentName:"p"},"dailyReportSummary"),"\u7684csv\u90ae\u4ef6\uff0c\u5305\u542b\u5f53\u65e5\u6240\u6709\u7684job\uff0c\u7c92\u5ea6\u4e3astep\uff0c\u5bf9\u4e8e\u5931\u8d25job\uff0c\u8fd8\u4f1a\u6c47\u603b\u5177\u4f53\u7684\u5931\u8d25\u4fe1\u606f\u548c\u5177\u4f53step\uff0c\u4e3b\u8981\u4f9d\u8d56\u4e8e",(0,r.kt)("inlineCode",{parentName:"p"},"step_log"),"\u548c",(0,r.kt)("inlineCode",{parentName:"p"},"job_log"),"\u4e24\u5f20\u8868\uff0c\u9700\u8981\u5728\u914d\u7f6e\u6587\u4ef6\u4e2d\u914d\u7f6e",(0,r.kt)("inlineCode",{parentName:"p"},"projectName"),"\u548c",(0,r.kt)("inlineCode",{parentName:"p"},"jobName"),"\uff08\u5982\u679c\u4e0d\u914d\u7f6e\uff0c\u5219",(0,r.kt)("inlineCode",{parentName:"p"},"dailyReport"),"\u7684csv\u6587\u4ef6\u4e2d\u6ca1\u6709\u5177\u4f53\u7684",(0,r.kt)("inlineCode",{parentName:"p"},"projectName"),"\uff09"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.DailyJobsSummaryReportTransformer\n--  methodName=transform\n--  transformerType=object\n--  datasource=hive,yellowbrick\n-- target=do_nothing\n-- checkPoint=false\n-- dateRangeInterval=0\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"datasource"),": \u5199\u5165\u7684\u6570\u636e\u5e93\u7c7b\u578b\uff0c\u7528\u4e8e\u6c47\u603b\u5177\u4f53\u7684",(0,r.kt)("inlineCode",{parentName:"li"},"errorMessage")))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"CheckAllConnectorStatusTransformer"),": \u8be5transformer\u96c6\u6210\u4e86",(0,r.kt)("inlineCode",{parentName:"p"},"kafka restapi"),"\uff0c\u901a\u8fc7\u8c03\u7528\u63a5\u53e3\u8fd4\u56de\u5404\u4e2a",(0,r.kt)("inlineCode",{parentName:"p"},"connector"),"\u7684\u8fd0\u884c\u72b6\u6001\uff0c\u82e5\u6709",(0,r.kt)("inlineCode",{parentName:"p"},"connector"),"\u72b6\u6001\u4e3a",(0,r.kt)("inlineCode",{parentName:"p"},"failed"),"\u6216",(0,r.kt)("inlineCode",{parentName:"p"},"pause"),"\uff0c\u5219\u4f1a\u53ca\u65f6\u9884\u8b66"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.CheckAllConnectorStatusTransformer\n--  methodName=transform\n--  transformerType=object\n--  uri=https://xxx.com:28085\n-- target=do_nothing\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"uri"),": \u4e3a\u5177\u4f53\u7684",(0,r.kt)("inlineCode",{parentName:"li"},"kafka restapi"),"\u7aef\u53e3\u4fe1\u606f\uff0c\u4f1a\u5728\u4ee3\u7801\u4e2d\u62fc\u63a5\u771f\u6b63\u9700\u8981\u8bbf\u95ee\u7684uri\u4fe1\u606f"))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"CheckConnectorStatusTransformer"),": \u8be5transformer\u4e0e",(0,r.kt)("inlineCode",{parentName:"p"},"CheckAllConnectorStatusTransformer"),"\u4e00\u81f4\uff0c\u5747\u7528\u505amonitor\uff0c\u9700\u8981\u6307\u5b9a\u5177\u4f53",(0,r.kt)("inlineCode",{parentName:"p"},"connector"),"\u7684\u540d\u5b57\uff0c\u4e0e",(0,r.kt)("inlineCode",{parentName:"p"},"connectorName"),"\u53c2\u6570\u8054\u5408\u4f7f\u7528\uff0c\u53ef\u4ee5\u4e00\u6b21\u8f93\u5165\u4e00\u4e2a\u6216\u591a\u4e2a",(0,r.kt)("inlineCode",{parentName:"p"},"connector"),"\uff0c\u901a\u5e38\u7528\u5728kafka\u4e0b\u6e38\u4efb\u52a1\u4e2d\u7684step1\uff0c\u5373\u9996\u5148\u5224\u65ad",(0,r.kt)("inlineCode",{parentName:"p"},"connector"),"\u662f\u5426\u6b63\u5e38\u5de5\u4f5c\uff0c\u82e5",(0,r.kt)("inlineCode",{parentName:"p"},"connector"),"\u62a5\u9519\u5219\u4e0b\u6e38\u4efb\u52a1\u4e0d\u4f1a\u6267\u884c\uff0c\u9700\u8981\u5728\u914d\u7f6e\u6587\u4ef6\u4e2d\u914d\u7f6e",(0,r.kt)("inlineCode",{parentName:"p"},"kafka.restapi"),"\u53c2\u6570\uff0c\u7528\u4e8e\u6784\u5efa\u5177\u4f53api"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.CheckConnectorStatusTransformer\n--  methodName=transform\n--  transformerType=object\n--  connectorName=connector1, connector2\n-- target=do_nothing\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"connectorName"),": \u9700\u8981monitor\u7684",(0,r.kt)("inlineCode",{parentName:"li"},"connector"),"\u540d\u5b57\uff0c\u901a\u5e38\u4e3a\u63a5\u5165\u8868\u65f6\u7684",(0,r.kt)("inlineCode",{parentName:"li"},"source connector"),"\u548c",(0,r.kt)("inlineCode",{parentName:"li"},"sink connector")))),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},(0,r.kt)("inlineCode",{parentName:"p"},"EnsureSinkConnectorFinished"),": \u8be5transformer\u7528\u4e8e\u786e\u8ba4",(0,r.kt)("inlineCode",{parentName:"p"},"sink connector"),"\u5373",(0,r.kt)("inlineCode",{parentName:"p"},"consumer"),"\u7aef\u662f\u5426\u6d88\u8d39\u5b8c\u6570\u636e\uff0c\u662f\u5426\u5c06\u6570\u636e\u5168\u90e8\u5199\u5165hive/hdfs\uff0c\u4e00\u822c\u63a5\u5728",(0,r.kt)("inlineCode",{parentName:"p"},"CheckConnectorStatusTransformer"),"\u540e\uff0c\u4e3astep2\uff0c\u82e5",(0,r.kt)("inlineCode",{parentName:"p"},"consumer"),"\u8fd8\u672a\u5199\u5165\u5b8c\u6210\uff0c\u4f1a\u7b49\u5f855\u5206\u949f\uff0c\u82e55\u5206\u949f\u540e\u8fd8\u672a\u5b8c\u6210\uff0c\u5219\u4e0b\u6e38\u4efb\u52a1\u4e0d\u4f1a\u6267\u884c"),(0,r.kt)("pre",{parentName:"li"},(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- stepId=2\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.EnsureSinkConnectorFinished\n--  methodName=transform\n--  transformerType=object\n--  group=consumer-group1\n--  kafkaTopic=topic1\n-- target=do_nothing\n")),(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"group"),": \u4e3a",(0,r.kt)("inlineCode",{parentName:"li"},"kafka consumer group"),"\u540d\u79f0\uff0c\u901a\u5e38\u4e00\u4e2a",(0,r.kt)("inlineCode",{parentName:"li"},"group"),"\u5bf9\u5e94\u4e00\u4e2a",(0,r.kt)("inlineCode",{parentName:"li"},"topic"),"\uff0c\u82e5\u4e00\u4e2a",(0,r.kt)("inlineCode",{parentName:"li"},"consumer group"),"\u5305\u542b\u591a\u4e2a",(0,r.kt)("inlineCode",{parentName:"li"},"topic"),"\uff0c\u9700\u8981\u6307\u5b9a",(0,r.kt)("inlineCode",{parentName:"li"},"kafkaTopic"),"\u540d\u79f0"),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("inlineCode",{parentName:"li"},"kafkaTopic"),": ",(0,r.kt)("inlineCode",{parentName:"li"},"topic"),"\u540d\u79f0\uff0c\u82e5\u4e0d\u6307\u5b9a\uff0c\u5219\u9ed8\u8ba4\u4e3a\u68c0\u67e5",(0,r.kt)("inlineCode",{parentName:"li"},"group"),"\u91cc\u5168\u90e8",(0,r.kt)("inlineCode",{parentName:"li"},"topic"),"\u4e2d\u7684",(0,r.kt)("inlineCode",{parentName:"li"},"message"),"\u662f\u5426\u5168\u88ab\u6d88\u8d39")))),(0,r.kt)("h3",{id:"\u5982\u4f55\u81ea\u5b9a\u4e49transformer"},"\u5982\u4f55\u81ea\u5b9a\u4e49transformer"),(0,r.kt)("p",null,"\u53ef\u4ee5\u5728",(0,r.kt)("inlineCode",{parentName:"p"},"com.github.sharpdata.sharpetl.spark.transformation"),"\u5305\u4e2d\u5b9a\u5236",(0,r.kt)("inlineCode",{parentName:"p"},"transformer"),"\uff0c\u901a\u8fc7",(0,r.kt)("inlineCode",{parentName:"p"},"override transform"),"\u65b9\u6cd5\u5b9e\u73b0\u5177\u4f53\u903b\u8f91\uff0c\u800c",(0,r.kt)("inlineCode",{parentName:"p"},"transformer"),"\u7684\u8c03\u7528\u5219\u4e3b\u8981\u901a\u8fc7\u53cd\u5c04\u8fdb\u884c\uff0c\u53ea\u9700\u5728sql\u811a\u672c\u4e2d\u6307\u5b9a\u5177\u4f53\u7684",(0,r.kt)("inlineCode",{parentName:"p"},"transformer"),"\u540d\u79f0\u548c\u76f8\u5e94\u53c2\u6570\u5373\u53ef\u3002"),(0,r.kt)("h3",{id:"\u52a0\u8f7d\u5916\u90e8transformer"},"\u52a0\u8f7d\u5916\u90e8transformer"),(0,r.kt)("p",null,"\u6846\u67b6\u8fd8\u652f\u6301\u52a8\u6001\u52a0\u8f7dscala\u811a\u672c\u6587\u4ef6\uff0c\u4e00\u4e2a\u793a\u4f8b\u5982\u4e0b\uff1a"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-scala"},"import com.fasterxml.jackson.annotation.JsonInclude.Include\nimport com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}\nimport com.fasterxml.jackson.module.scala.DefaultScalaModule\nimport com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper\nimport com.jayway.jsonpath.{JsonPath, PathNotFoundException}\nimport com.github.sharpdata.sharpetl.core.util.ETLLogger\nimport com.github.sharpdata.sharpetl.spark.common.ETLSparkSession\nimport com.github.sharpdata.sharpetl.spark.transformation._\nimport com.github.sharpdata.sharpetl.spark.utils.HttpStatusUtils\nimport net.minidev.json.JSONArray\nimport org.apache.http.impl.client._\nimport org.apache.http.util.EntityUtils\nimport org.apache.spark.sql.DataFrame\nimport org.apache.spark.sql.functions._\nimport org.apache.spark.sql.types.{StringType, StructField, StructType}\n\nobject LoopHttpTransformer extends Transformer {\n\n  val mapper = new ObjectMapper with ScalaObjectMapper\n  mapper.setSerializationInclusion(Include.NON_ABSENT)\n  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)\n  mapper.registerModule(DefaultScalaModule)\n\n  override def transform(args: scala.collection.mutable.Map[String, String]): DataFrame = {\n    ???\n  }\n}\n")),(0,r.kt)("h3",{id:"pro-tips"},"Pro tips"),(0,r.kt)("admonition",{type:"tip"},(0,r.kt)("p",{parentName:"admonition"},"\u7f16\u5199scala\u811a\u672c\u5b9e\u73b0\u7684transformer\uff0c\u9700\u8981\u6ce8\u610f\u51e0\u4e2a\u8981\u70b9\uff1a"),(0,r.kt)("ul",{parentName:"admonition"},(0,r.kt)("li",{parentName:"ul"},"\u6587\u4ef6\u5f00\u5934\u4e0d\u53ef\u5e26\u6709package\u4fe1\u606f\uff0c\u5728sql\u4e2d\u8c03\u7528\u65f6\uff0c\u4f1a\u6839\u636epackage.fileName\u4e2d\u7684fileName\u627e\u5230scala\u811a\u672c"),(0,r.kt)("li",{parentName:"ul"},"sql\u5728\u5f15\u7528scala\u811a\u672c\u65f6\u9700\u8981\u8bbe\u7f6e",(0,r.kt)("inlineCode",{parentName:"li"},"transformerType=dynamic_object"),"\uff0c\u9664\u6b64\u4e4b\u5916\u4f7f\u7528\u65b9\u5f0f\u4e0ejar\u4e2d\u7684transformer\u76f8\u540c"),(0,r.kt)("li",{parentName:"ul"},"\u90e8\u5206package\u9700\u4f7f\u7528\u5168\u540d\u4f8b\u5982 ",(0,r.kt)("inlineCode",{parentName:"li"},"scala.collection.mutable.Map[String, String]"),", \u800c\u4e0d\u662f ",(0,r.kt)("inlineCode",{parentName:"li"},"mutable.Map[String, String]")),(0,r.kt)("li",{parentName:"ul"},"\u5982\u679c\u4f60\u9047\u5230\u4e86\u7c7b\u4f3c\u4e8e",(0,r.kt)("inlineCode",{parentName:"li"},"illegal cyclic reference involving object InterfaceAudience"),"\u7684\u9519\u8bef\uff0c\u4f60\u9700\u8981spark-submit option ",(0,r.kt)("inlineCode",{parentName:"li"},'--conf  "spark.executor.userClassPathFirst=true" --conf  "spark.driver.userClassPathFirst=true"')),(0,r.kt)("li",{parentName:"ul"},"\u5982\u679c\u4f60\u9047\u5230\u4e86\u9519\u8bef",(0,r.kt)("inlineCode",{parentName:"li"},"object x is not a member of package x"),"\uff0c\u4f60\u9700\u8981\u4f7f\u7528\u5168\u5f15\u7528\u4f8b\u5982 ",(0,r.kt)("inlineCode",{parentName:"li"},"scala.collection.mutable.Map[String, String]")))))}c.isMDXComponent=!0}}]);