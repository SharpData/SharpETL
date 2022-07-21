"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[788],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>d});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var p=a.createContext({}),s=function(e){var t=a.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},c=function(e){var t=s(e.components);return a.createElement(p.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,p=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),m=s(n),d=r,f=m["".concat(p,".").concat(d)]||m[d]||u[d]||o;return n?a.createElement(f,i(i({ref:t},c),{},{components:n})):a.createElement(f,i({ref:t},c))}));function d(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=m;var l={};for(var p in t)hasOwnProperty.call(t,p)&&(l[p]=t[p]);l.originalType=e,l.mdxType="string"==typeof e?e:r,i[1]=l;for(var s=2;s<o;s++)i[s]=n[s];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},2461:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>i,default:()=>u,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var a=n(7462),r=(n(7294),n(3905));const o={},i=void 0,l={unversionedId:"batch-job-guide",id:"batch-job-guide",title:"batch-job-guide",description:"This guide provides a quick guide for commandline batch-job",source:"@site/docs/batch-job-guide.md",sourceDirName:".",slug:"/batch-job-guide",permalink:"/docs/batch-job-guide",draft:!1,editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/docs/batch-job-guide.md",tags:[],version:"current",frontMatter:{},sidebar:"docs",previous:{title:"single-job-guide",permalink:"/docs/single-job-guide"},next:{title:"transformer guide",permalink:"/docs/transformer-guide"}},p={},s=[{value:"Introduction",id:"introduction",level:2},{value:"Parameters",id:"parameters",level:2},{value:"common command params",id:"common-command-params",level:3},{value:"batch-job params",id:"batch-job-params",level:3}],c={toc:s};function u(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("p",null,"This guide provides a quick guide for commandline ",(0,r.kt)("inlineCode",{parentName:"p"},"batch-job")," "),(0,r.kt)("h2",{id:"introduction"},"Introduction"),(0,r.kt)("p",null,"The command ",(0,r.kt)("inlineCode",{parentName:"p"},"batch-job")," runs all jobs in batch each time and should be noted as one of arguments when running a job. For example, when running a sample job,and the command is as follows:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-bash"},'# run all job in batch by `spark-submit`\nspark-submit --class com.github.sharpdata.sharpetl.spark.Entrypoint spark/build/libs/spark-1.0.0-SNAPSHOT.jar batch-job -f ~/Desktop/sharp-etl-Quick-Start-Guide.xlsx --default-start-time="2021-09-30 00:00:00" --local --once\n\n# run all job locally\n./gradlew :spark:run --args="batch-job -f ~/Desktop/sharp-etl-Quick-Start-Guide.xlsx --default-start-time=\'2021-09-30 00:00:00\' --local --once"\n')),(0,r.kt)("h2",{id:"parameters"},"Parameters"),(0,r.kt)("h3",{id:"common-command-params"},"common command params"),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--local"))),(0,r.kt)("p",null,"Declare that the job is running in standalone mode. If ",(0,r.kt)("inlineCode",{parentName:"p"},"--local")," not provided, the job will try running with Hive support enabled."),(0,r.kt)("ol",{start:2},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--release-resource"))),(0,r.kt)("p",null,"The function is to automatically close spark session after job completion."),(0,r.kt)("ol",{start:3},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--skip-running"))),(0,r.kt)("p",null,"When there is a flash crash, use ",(0,r.kt)("inlineCode",{parentName:"p"},"--skip-running")," to set last job status(in running state) as failed and start a new one."),(0,r.kt)("ol",{start:4},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--default-start")," / ",(0,r.kt)("inlineCode",{parentName:"li"},"--default-start-time"))),(0,r.kt)("p",null,"Specify the default start time(eg, 20210101000000)/incremental id of this job. If the command is running for the first time, the default time would be the time set by the argument. If not, the argument would not work."),(0,r.kt)("ol",{start:5},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--once"))),(0,r.kt)("p",null,"It means that the job only run one time(for testing usage). "),(0,r.kt)("ol",{start:6},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--env"))),(0,r.kt)("p",null,"Specify the default env path: local/test/dev/qa/prod running the job."),(0,r.kt)("ol",{start:7},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--property"))),(0,r.kt)("p",null,"Using specific property file, eg ",(0,r.kt)("inlineCode",{parentName:"p"},"--property=hdfs:///user/admin/etl-conf/etl.properties")),(0,r.kt)("ol",{start:8},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--override"))),(0,r.kt)("p",null,"Overriding config in properties file, eg ",(0,r.kt)("inlineCode",{parentName:"p"},"--override=etl.workflow.path=hdfs:///user/hive/sharp-etl,a=b,c=d")),(0,r.kt)("h3",{id:"batch-job-params"},"batch-job params"),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--names"))),(0,r.kt)("p",null,"Specify the names of the job to run."),(0,r.kt)("ol",{start:2},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"-f")," / ",(0,r.kt)("inlineCode",{parentName:"li"},"--file"))),(0,r.kt)("p",null,"Specify excel file to run."),(0,r.kt)("ol",{start:3},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"--period"))),(0,r.kt)("p",null,"Specify the period of job execution."),(0,r.kt)("ol",{start:4},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("inlineCode",{parentName:"li"},"-h")," / ",(0,r.kt)("inlineCode",{parentName:"li"},"--help"))),(0,r.kt)("p",null,"Take an example of parameters and its default value is false."))}u.isMDXComponent=!0}}]);