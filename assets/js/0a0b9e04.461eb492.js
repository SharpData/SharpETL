"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[2249],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>d});var a=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function l(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function p(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?l(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):l(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function o(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},l=Object.keys(e);for(a=0;a<l.length;a++)r=l[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(a=0;a<l.length;a++)r=l[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var i=a.createContext({}),s=function(e){var t=a.useContext(i),r=t;return e&&(r="function"==typeof e?e(t):p(p({},t),e)),r},u=function(e){var t=s(e.components);return a.createElement(i.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},c=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,l=e.originalType,i=e.parentName,u=o(e,["components","mdxType","originalType","parentName"]),c=s(r),d=n,k=c["".concat(i,".").concat(d)]||c[d]||m[d]||l;return r?a.createElement(k,p(p({ref:t},u),{},{components:r})):a.createElement(k,p({ref:t},u))}));function d(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var l=r.length,p=new Array(l);p[0]=c;var o={};for(var i in t)hasOwnProperty.call(t,i)&&(o[i]=t[i]);o.originalType=e,o.mdxType="string"==typeof e?e:n,p[1]=o;for(var s=2;s<l;s++)p[s]=r[s];return a.createElement.apply(null,p)}return a.createElement.apply(null,r)}c.displayName="MDXCreateElement"},1110:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>i,contentTitle:()=>p,default:()=>m,frontMatter:()=>l,metadata:()=>o,toc:()=>s});var a=r(7462),n=(r(7294),r(3905));const l={slug:"sharp-etl-introduce-05-workflow-in-a-glance",title:"Sharp ETL\u4ecb\u7ecd(\u4e94):Workflow\u5165\u95e8",tags:["sharp etl","workflow"],date:new Date("2022-08-04T16:00:00.000Z")},p=void 0,o={permalink:"/SharpETL/blog/sharp-etl-introduce-05-workflow-in-a-glance",editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/blog/sharp-etl-introduce-05-workflow-in-a-glance.md",source:"@site/blog/sharp-etl-introduce-05-workflow-in-a-glance.md",title:"Sharp ETL\u4ecb\u7ecd(\u4e94):Workflow\u5165\u95e8",description:"\u5bfc\u8a00",date:"2022-08-04T16:00:00.000Z",formattedDate:"August 4, 2022",tags:[{label:"sharp etl",permalink:"/SharpETL/blog/tags/sharp-etl"},{label:"workflow",permalink:"/SharpETL/blog/tags/workflow"}],readingTime:3.515,hasTruncateMarker:!0,authors:[],frontMatter:{slug:"sharp-etl-introduce-05-workflow-in-a-glance",title:"Sharp ETL\u4ecb\u7ecd(\u4e94):Workflow\u5165\u95e8",tags:["sharp etl","workflow"],date:"2022-08-04T16:00:00.000Z"},nextItem:{title:"Sharp ETL\u4ecb\u7ecd(\u56db):\u65e5\u5fd7\u9a71\u52a8\u5b9e\u73b0",permalink:"/SharpETL/blog/sharp-etl-introduce-04-log-driven-implementation"}},i={authorsImageUrls:[]},s=[{value:"\u5bfc\u8a00",id:"\u5bfc\u8a00",level:2},{value:"\u53d8\u91cf",id:"\u53d8\u91cf",level:2},{value:"\u4e34\u65f6\u8868",id:"\u4e34\u65f6\u8868",level:2},{value:"\u6570\u636e\u6e90",id:"\u6570\u636e\u6e90",level:2},{value:"\u6269\u5c55",id:"\u6269\u5c55",level:2},{value:"UDF",id:"udf",level:3},{value:"Transformer",id:"transformer",level:3}],u={toc:s};function m(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h2",{id:"\u5bfc\u8a00"},"\u5bfc\u8a00"),(0,n.kt)("p",null,"\u672c\u6587\u5c06\u5feb\u901f\u8bb2\u89e3workflow\u7684\u57fa\u672c\u7528\u6cd5\uff0c\u5305\u62ec"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"\u53d8\u91cf"),(0,n.kt)("li",{parentName:"ul"},"\u4e34\u65f6\u8868"),(0,n.kt)("li",{parentName:"ul"},"\u63a7\u5236\u6d41 workflow_spec.sql"),(0,n.kt)("li",{parentName:"ul"},"step\u8bfb\u5199\u6570\u636e"),(0,n.kt)("li",{parentName:"ul"},"\u6570\u636e\u6e90"),(0,n.kt)("li",{parentName:"ul"},"\u6269\u5c55"),(0,n.kt)("li",{parentName:"ul"},"UDF"),(0,n.kt)("li",{parentName:"ul"},"Transformer"),(0,n.kt)("li",{parentName:"ul"},"Dynamic transformer")),(0,n.kt)("h2",{id:"\u53d8\u91cf"},"\u53d8\u91cf"),(0,n.kt)("p",null,"Sharp ETL\u5bf9\u53d8\u91cf\u6709\u4e30\u5bcc\u7684\u652f\u6301\uff0c\u5305\u62ec\u4efb\u52a1\u8fd0\u884c\u5185\u5efa\u7684\u53d8\u91cf\u548c\u7528\u6237\u81ea\u5b9a\u4e49\u53d8\u91cf\u3002"),(0,n.kt)("p",null,"\u5185\u7f6e\u53d8\u91cf\u5305\u62ec"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${DATA_RANGE_START}")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${DATA_RANGE_END}")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${JOB_ID}")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${JOB_NAME}")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${WORKFLOW_NAME}"))),(0,n.kt)("p",null,"\u9488\u5bf9timewindow\u4efb\u52a1\u8fd8\u5305\u62ec"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${YEAR}")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${MONTH}")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${DAY}")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${HOUR}")),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("inlineCode",{parentName:"li"},"${MINUTE}"))),(0,n.kt)("p",null,"\u7528\u6237\u53ef\u4ee5\u5728\u4efb\u610fstep\u4e2d\u65b0\u589e\u6216\u8986\u76d6\u53d8\u91cf\uff0c\u58f0\u660e\u7ed3\u675f\u540e\uff0c\u540e\u7eedstep\u5373\u53ef\u4f7f\u7528\u8be5\u53d8\u91cf\uff0c\u4f8b\u5982"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=temp\n-- target=variables\nselect from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy') as `YEAR`,\n       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'MM')   as `MONTH`,\n       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'dd')   as `DAY`,\n       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'HH')   as `HOUR`,\n       'temp_source' as `sources`,\n       'temp_target' as `target`,\n       'temp_end' as `end`\n")),(0,n.kt)("h2",{id:"\u4e34\u65f6\u8868"},"\u4e34\u65f6\u8868"),(0,n.kt)("p",null,"\u4e34\u65f6\u8868\u662fSharp ETL\u80fd\u591f\u5c06\u590d\u6742\u4efb\u52a1\u62c6\u5206\u7684\u57fa\u7840\uff0c\u5f53\u524dSpark\u7684\u5b9e\u73b0\u5c31\u662f\u4f7f\u7528\u4e86Spark\u7684\u4e34\u65f6\u8868\u3002\u4e00\u6bb5\u590d\u6742\u7684\u903b\u8f91\u53ef\u4ee5\u62c6\u5206\u4e3a\u8f93\u51fa\u5230\u591a\u4e2a\u4e34\u65f6\u8868\u6765\u7b80\u5316\u903b\u8f91\u3002\u5bf9\u4e8e\u4ecetemp\u8868\u8bfb\u6570\u636e\u7684step\u800c\u8a00\uff0csource\u53ef\u4ee5\u5ffd\u7565\u6389\uff0c\u4e0d\u5199source\u9ed8\u8ba4\u8ba4\u4e3a\u4ecetemp\u8bfb\u53d6\u6570\u636e\u3002"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- target=temp\n--  tableName=temp_table\nselect 'SUCCESS' as `RESULT`;\n\n-- step=2\n-- target=console\nselect * from temp_table;\n")),(0,n.kt)("h2",{id:"\u6570\u636e\u6e90"},"\u6570\u636e\u6e90"),(0,n.kt)("p",null,"\u6bcf\u4e2astep\u90fd\u6709source\u548ctarget\u4e24\u4e2a\u914d\u7f6e\uff0c\u5177\u4f53\u914d\u7f6e\u4f7f\u7528\u53ef\u4ee5\u53c2\u8003 ",(0,n.kt)("a",{parentName:"p",href:"/docs/datasource"},"datasource")," \u8fd9\u4e00\u8282\u6765\u4f7f\u7528\u3002\u540c\u4e00\u4e2aworkflow\u91cc\u9762datasource\u4e4b\u95f4\u53ef\u4ee5\u4efb\u610f\u7ec4\u5408\u4f7f\u7528\uff0c\u6ca1\u6709\u4e25\u683c\u9650\u5236\uff0c\u7528\u6237\u4e5f\u53ef\u4ee5\u5f88\u65b9\u4fbf\u7684\u81ea\u5b9a\u4e49\u65b0\u7684\u6570\u636e\u6e90\u3002"),(0,n.kt)("h2",{id:"\u6269\u5c55"},"\u6269\u5c55"),(0,n.kt)("p",null,"Sharp ETL\u4ece\u8bbe\u8ba1\u4e4b\u521d\u5c31\u4e00\u76f4\u8003\u8651\u8ba9\u7528\u6237\u53ef\u4ee5\u5f88\u65b9\u4fbf\u7684\u6269\u5c55\u529f\u80fd\uff0c\u65e0\u8bba\u662f\u5728step\u8bbe\u8ba1\u3001transformer\u8bbe\u8ba1\u3001UDF\u3001\u52a8\u6001\u52a0\u8f7dtransformer\u811a\u672c\u3001\u81ea\u5b9a\u4e49\u6570\u636e\u8d28\u91cf\u89c4\u5219\u3001\u81ea\u5b9a\u4e49\u6570\u636e\u8d28\u91cf\u68c0\u67e5\u811a\u672c\u90fd\u80fd\u591f\u5f88\u597d\u7684\u652f\u6301\u7528\u6237\u5b9e\u73b0\u81ea\u5b9a\u4e49\u903b\u8f91\u3002\u672a\u6765\u8fd8\u4f1a\u652f\u6301 \u5206\u652f\u5224\u65ad\u3001\u5faa\u73af\u3001\u629b\u51fa\u5f02\u5e38\u3001\u9519\u8bef\u5904\u7406\u5206\u652f \u7b49\u7b49\u63a7\u5236\u6d41\uff0c\u4f7f\u5f97Sharp ETL\u7684workflow\u66f4\u52a0\u50cf\u4e00\u4e2a\u7f16\u7a0b\u8bed\u8a00\uff0c\u8fd9\u6837\u7528\u6237\u5c31\u53ef\u4ee5\u5b8c\u5168\u4f9d\u8d56\u4e8eSQL\u6765\u5b9e\u73b0\u6240\u6709\u7684\u529f\u80fd\u3002"),(0,n.kt)("h3",{id:"udf"},"UDF"),(0,n.kt)("p",null,"\u7528\u6237\u53ef\u4ee5\u901a\u8fc7build\u81ea\u5df1\u7684jar\u5305\u6765\u5b9e\u73b0UDF\u7684\u652f\u6301\uff0c\u8fd9\u4e2ajar\u5305\u4e0d\u9700\u8981\u57fa\u4e8eSharp ETL\u6765\u5b9e\u73b0\uff0c\u751a\u81f3\u4ec5\u4ec5\u662f\u666e\u901a\u7684Scala function\u5373\u53ef\u3002"),(0,n.kt)("p",null,"\u4f8b\u5982\uff0c\u7528\u6237\u9700\u8981\u6ce8\u518c\u4e00\u4e2a\u65b0\u7684UDF\u6765\u5b9e\u73b0\u81ea\u5b9a\u4e49\u903b\u8f91\uff0c\u53ea\u9700\u8981\u7f16\u5199\u666e\u901a\u4ee3\u7801\u5373\u53ef\uff1a"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'class TestUdfObj extends Serializable {\n  def testUdf(value: String): String = {\n    s"$value-proceed-by-udf"\n  }\n}\n')),(0,n.kt)("p",null,"\u6253\u5305\u5b8c\u6210\u540e\u9700\u8981\u5c06jar\u5305\u4e0eSharp ETL\u7684jar\u5305\u4e00\u8d77\u63d0\u4ea4\uff0c\u8fd9\u6837\u5c31\u53ef\u4ee5\u5f88\u8f7b\u6613\u7684\u5f15\u7528\u81ea\u5df1\u7684UDF\u4e86\u3002"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-bash"},"spark-submit --class com.github.sharpdata.sharpetl.spark.Entrypoint spark/build/libs/spark-1.0.0-SNAPSHOT.jar /path/to/your-udf.jar ... ...\n")),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=class\n--  className=com.github.sharpdata.sharpetl.spark.end2end.TestUdfObj\n-- target=udf\n--  methodName=testUdf\n--  udfName=test_udf\n\n-- step=2\n-- source=temp\n-- target=temp\n--  tableName=udf_result\nselect test_udf('input') as `result`;\n")),(0,n.kt)("h3",{id:"transformer"},"Transformer"),(0,n.kt)("p",null,"Transformer\u7684\u76f8\u5173\u8be6\u7ec6\u4f7f\u7528\u53ef\u4ee5\u53c2\u8003 ",(0,n.kt)("a",{parentName:"p",href:"/docs/transformer-guide"},"Transformer")))}m.isMDXComponent=!0}}]);