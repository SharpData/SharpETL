"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[4023],{3905:(e,t,r)=>{r.d(t,{Zo:()=>c,kt:()=>d});var a=r(7294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function l(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?l(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):l(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function p(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},l=Object.keys(e);for(a=0;a<l.length;a++)r=l[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(a=0;a<l.length;a++)r=l[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var i=a.createContext({}),u=function(e){var t=a.useContext(i),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},c=function(e){var t=u(e.components);return a.createElement(i.Provider,{value:t},e.children)},s={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,l=e.originalType,i=e.parentName,c=p(e,["components","mdxType","originalType","parentName"]),m=u(r),d=n,k=m["".concat(i,".").concat(d)]||m[d]||s[d]||l;return r?a.createElement(k,o(o({ref:t},c),{},{components:r})):a.createElement(k,o({ref:t},c))}));function d(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var l=r.length,o=new Array(l);o[0]=m;var p={};for(var i in t)hasOwnProperty.call(t,i)&&(p[i]=t[i]);p.originalType=e,p.mdxType="string"==typeof e?e:n,o[1]=p;for(var u=2;u<l;u++)o[u]=r[u];return a.createElement.apply(null,o)}return a.createElement.apply(null,r)}m.displayName="MDXCreateElement"},9738:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>i,contentTitle:()=>o,default:()=>s,frontMatter:()=>l,metadata:()=>p,toc:()=>u});var a=r(7462),n=(r(7294),r(3905));const l={slug:"sharp-etl-introduce-02-beyond-existing-etl",title:"Sharp ETL\u4ecb\u7ecd(\u4e8c):\u8d85\u8d8a\u73b0\u6709ETL",tags:["sharp etl","data quality check"],date:new Date("2022-08-01T16:00:00.000Z")},o=void 0,p={permalink:"/SharpETL/blog/sharp-etl-introduce-02-beyond-existing-etl",editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/blog/sharp-etl-introduce-02-beyond-existing-etl.md",source:"@site/blog/sharp-etl-introduce-02-beyond-existing-etl.md",title:"Sharp ETL\u4ecb\u7ecd(\u4e8c):\u8d85\u8d8a\u73b0\u6709ETL",description:"\u5bfc\u8a00",date:"2022-08-01T16:00:00.000Z",formattedDate:"August 1, 2022",tags:[{label:"sharp etl",permalink:"/SharpETL/blog/tags/sharp-etl"},{label:"data quality check",permalink:"/SharpETL/blog/tags/data-quality-check"}],readingTime:7.145,hasTruncateMarker:!0,authors:[],frontMatter:{slug:"sharp-etl-introduce-02-beyond-existing-etl",title:"Sharp ETL\u4ecb\u7ecd(\u4e8c):\u8d85\u8d8a\u73b0\u6709ETL",tags:["sharp etl","data quality check"],date:"2022-08-01T16:00:00.000Z"},prevItem:{title:"Sharp ETL\u4ecb\u7ecd(\u4e09):\u5f53\u6570\u636e\u5de5\u7a0b\u9047\u5230\u6df7\u6c8c\u5de5\u7a0b",permalink:"/SharpETL/blog/sharp-etl-introduce-03-when-data-engineering-meets-chaos-engineering"},nextItem:{title:"Sharp ETL\u4ecb\u7ecd(\u4e00):\u4e3a\u4ec0\u4e48\u6211\u4eec\u9700\u8981Sharp ETL",permalink:"/SharpETL/blog/sharp-etl-introduce-01-why-we-need-sharp-etl"}},i={authorsImageUrls:[]},u=[{value:"\u5bfc\u8a00",id:"\u5bfc\u8a00",level:2},{value:"\u901a\u8fc7workflow\u7ec4\u7ec7\u4efb\u52a1\u903b\u8f91",id:"\u901a\u8fc7workflow\u7ec4\u7ec7\u4efb\u52a1\u903b\u8f91",level:2},{value:"workflow\u7684\u672a\u6765",id:"workflow\u7684\u672a\u6765",level:2},{value:"\u901a\u8fc7<code>Transformer</code>\u6765\u8fdb\u884c\u81ea\u5b9a\u4e49\u903b\u8f91\u6269\u5c55",id:"\u901a\u8fc7transformer\u6765\u8fdb\u884c\u81ea\u5b9a\u4e49\u903b\u8f91\u6269\u5c55",level:2},{value:"\u5de5\u7a0b\u5316\u4ee3\u7801\u751f\u6210",id:"\u5de5\u7a0b\u5316\u4ee3\u7801\u751f\u6210",level:2},{value:"\u6570\u636e\u8d28\u91cf\u95ee\u9898\u5206\u7ea7\u5206\u7c7b",id:"\u6570\u636e\u8d28\u91cf\u95ee\u9898\u5206\u7ea7\u5206\u7c7b",level:3}],c={toc:u};function s(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h2",{id:"\u5bfc\u8a00"},"\u5bfc\u8a00"),(0,n.kt)("p",null,"\u672c\u6587\u5c06\u4ece\u4ee5\u4e0b\u51e0\u4e2a\u7ef4\u5ea6\u5c55\u5f00Sharp ETL\u7684\u6570\u636e\u5de5\u7a0b\u5316\u5b9e\u8df5\uff1a"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"\u901a\u8fc7step\u7ec4\u5408\u6210\u4e3aworkflow"),(0,n.kt)("li",{parentName:"ul"},"\u652f\u6301\u901a\u8fc7\u81ea\u5b9a\u4e49\u4ee3\u7801\u903b\u8f91\u6269\u5c55"),(0,n.kt)("li",{parentName:"ul"},"\u5de5\u7a0b\u5316\u4ee3\u7801\u751f\u6210\uff0c\u56fa\u5316\u7edf\u4e00\u4e14\u6807\u51c6\u7684\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5",(0,n.kt)("ul",{parentName:"li"},(0,n.kt)("li",{parentName:"ul"},"\u4e8b\u5b9e\u8868\u548c\u7ef4\u5ea6\u8868\u7684\u5173\u8054\u68c0\u67e5"),(0,n.kt)("li",{parentName:"ul"},"\u8bb0\u5f55\u8868\u4e0e\u8868\u5173\u8054\u8fc7\u7a0b\u4e2d\u7684\u672a\u77e5\u503c\u548c\u4e0d\u9002\u7528\u503c"),(0,n.kt)("li",{parentName:"ul"},"\u6570\u636e\u8d28\u91cf\u95ee\u9898\u5206\u7ea7\u5206\u7c7b\u8bb0\u5f55"),(0,n.kt)("li",{parentName:"ul"},"... ...")))),(0,n.kt)("h2",{id:"\u901a\u8fc7workflow\u7ec4\u7ec7\u4efb\u52a1\u903b\u8f91"},"\u901a\u8fc7workflow\u7ec4\u7ec7\u4efb\u52a1\u903b\u8f91"),(0,n.kt)("p",null,"\u5728\u8f6f\u4ef6\u5de5\u7a0b\u4e2d\uff0c\u5904\u7406\u8d85\u957f\u4ee3\u7801\u7684\u65b9\u5f0f\u53ef\u80fd\u662f\u5c06\u4ee3\u7801\u903b\u8f91\u4e3a\u5c0f\u800c\u6613\u4e8e\u7406\u89e3\u7684\u5355\u5143\uff0c\u7136\u540e\u62bd\u53d6\u65b9\u6cd5\uff0c\u5e76\u7ed9\u65b9\u6cd5\u8d77\u901a\u4fd7\u6613\u61c2\u7684\u540d\u5b57\u3002\u8fd9\u4f7f\u5f97\u4ee3\u7801\u53ef\u91cd\u7528\u5e76\u53ef\u4ee5\u63d0\u9ad8\u53ef\u8bfb\u6027\u3002\u5728\u5904\u7406\u8d85\u957fSQL\u65f6\u7ecf\u5e38\u4f1a\u7528\u5230",(0,n.kt)("inlineCode",{parentName:"p"},"WITH"),"\u8bed\u6cd5\uff1a"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-sql"},"WITH query_name1 AS (\n     SELECT ...\n     )\n   , query_name2 AS (\n     SELECT ...\n       FROM query_name1\n        ...\n     )\nSELECT ...\n")),(0,n.kt)("p",null,"\u4e0d\u53ef\u5426\u8ba4\u901a\u8fc7with\u8bed\u6cd5\u91cd\u5199\u4e4b\u540e\u53ef\u8bfb\u6027\u5927\u5927\u63d0\u5347\uff0c\u4f46\u662f\u6211\u4eec\u8ba4\u4e3a\u8fd9\u4ecd\u7136\u4e0d\u591f\u3002\u901a\u8fc7Sharp ETL\u7684step\u62c6\u5206\u8fc7\u540e\u7684SQL\u53ef\u8bfb\u6027\u66f4\u597d\uff0c\u4e14debug\u66f4\u4e3a\u5bb9\u6613\u3002\n\u4e4d\u4e00\u770b\u4f3c\u4e4e\u901a\u8fc7workflow\u7ec4\u7ec7\u7684SQL\u66f4\u52a0\u957f\u6216\u8005\u590d\u6742\uff0c\u5b9e\u5219\u4e0d\u7136\uff0c\u6bcf\u4e00\u4e2astep\u90fd\u53ef\u4ee5\u6709\u540d\u79f0\u6765\u89e3\u91ca\u8fd9\u4e2astep\u7684\u4f5c\u7528\uff0c\u5176\u5b9e\u901a\u8fc7with\u8bed\u53e5\u7ec4\u7ec7\u7684SQL\u4e5f\u7ecf\u5e38\u9700\u8981\u6ce8\u91ca\u6765\u89e3\u91ca\u3002\u540c\u65f6\u8fd9\u91cc\u8fd8\u6709\u4e00\u4e2a\u9690\u85cf\u70b9\uff1a\u5982\u679csource\u662ftemp\uff0c\u5c31\u53ef\u4ee5\u4e0d\u7528\u5199source\uff0c\u4e00\u5b9a\u7a0b\u5ea6\u4e0a\u80fd\u591f\u7b80\u5316\u7406\u89e3\u3002\u540c\u65f6\u56e0\u4e3a\u65e5\u5fd7\u9a71\u52a8\u7684\u5b58\u5728\uff0c\u6bcf\u4e00\u4e2astep\u5728\u6267\u884c\u65f6\u90fd\u8bb0\u5f55\u4e86source\u548ctarget\u7684\u6761\u6570\uff0c\u8fd9\u4e2a\u76f8\u5bf9\u4e8e\u76f4\u63a5\u5199WITH\u8bed\u6cd5\u66f4\u5bb9\u6613\u8c03\u8bd5\u548c\u6392\u9519\u3002"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=define query name 1\n-- source=source type xxx\n-- target=temp\n--  tableName=query_name1\nSELECT ...\n\n-- step=define query name 2\n-- target=temp\n--  tableName=query_name2\nSELECT ...\n       FROM query_name1\n\n-- step=output\n-- target=target type xxx\n--  tableName=target table\nSELECT ...\n\n")),(0,n.kt)("h2",{id:"workflow\u7684\u672a\u6765"},"workflow\u7684\u672a\u6765"),(0,n.kt)("p",null,"\u5b9e\u9645\u4e0a\u6700\u521d\u7248\u672c\u7684workflow\u662f\u987a\u5e8f\u6267\u884c\u7684\uff0c\u4e00\u4e2astep\u63a5\u7740\u4e00\u4e2astep\u3002\u4f46\u5728\u771f\u6b63\u4f7f\u7528\u65f6\u6211\u4eec\u5f80\u5f80\u9700\u8981\u4f8b\u5982 \u5206\u652f\u5224\u65ad\u3001\u5faa\u73af\u3001\u629b\u51fa\u5f02\u5e38\u3001\u9519\u8bef\u5904\u7406\u5206\u652f \u7b49\u7b49\u529f\u80fd\uff0c\u8fd9\u4e9b\u90fd\u5728Sharp ETL\u672a\u6765\u7684\u8ba1\u5212\u4e2d\uff0c\u672a\u6765\u4f1a\u9010\u6b65\u589e\u52a0\u8fd9\u4e9b\u529f\u80fd\u3002"),(0,n.kt)("h2",{id:"\u901a\u8fc7transformer\u6765\u8fdb\u884c\u81ea\u5b9a\u4e49\u903b\u8f91\u6269\u5c55"},"\u901a\u8fc7",(0,n.kt)("a",{parentName:"h2",href:"https://github.com/SharpData/SharpETL/blob/97f303cbd1f40a29780551851f690c283bcb2061/spark/src/main/scala/com/github/sharpdata/sharpetl/spark/transformation/Transformer.scala"},(0,n.kt)("inlineCode",{parentName:"a"},"Transformer")),"\u6765\u8fdb\u884c\u81ea\u5b9a\u4e49\u903b\u8f91\u6269\u5c55"),(0,n.kt)("p",null,"\u6211\u4eec\u53ef\u4ee5\u5148\u770b\u4e00\u4e0b",(0,n.kt)("inlineCode",{parentName:"p"},"Transformer"),"\u7684API\uff0c\u5b83\u63d0\u4f9b\u4e86\u5728\u8bfb\u5199\u6570\u636e\u65f6\u63d2\u5165\u81ea\u5b9a\u4e49\u903b\u8f91\u7684\u673a\u5236\uff0c\u7528\u6237\u53ef\u4ee5\u8f7b\u6613\u7684\u901a\u8fc7\u5b9e\u73b0\u8fd9\u4e2aAPI\u6765\u63d2\u5165\u81ea\u5df1\u7684\u903b\u8f91\u3002\u751a\u81f3\u53ef\u4ee5\u901a\u8fc7\u52a8\u6001\u52a0\u8f7dscala\u811a\u672c\u6587\u4ef6\u6765\u6269\u5c55\u800c\u4e0d\u9700\u8981\u91cd\u65b0build jar\u5305\uff0c\u53ef\u4ee5\u53c2\u8003",(0,n.kt)("a",{parentName:"p",href:"/docs/transformer-guide"},"\u8fd9\u91cc"),"\u3002"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},"trait Transformer {\n\n  /**\n   * read\n   */\n  def transform(args: Map[String, String]): DataFrame = ???\n\n  /**\n   * write\n   */\n  def transform(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = ???\n}\n")),(0,n.kt)("h2",{id:"\u5de5\u7a0b\u5316\u4ee3\u7801\u751f\u6210"},"\u5de5\u7a0b\u5316\u4ee3\u7801\u751f\u6210"),(0,n.kt)("p",null,"\u5982\u679c\u662f\u5b8c\u5168\u624b\u5199SQL\u6765\u5b8c\u6210\u6240\u6709\u529f\u80fd\uff0c\u5305\u62ec\u6570\u636e\u5de5\u7a0b\u529f\u80fd\uff0c\u6574\u4e2a\u903b\u8f91\u4f1a\u975e\u5e38\u957f\u548c\u590d\u6742\u3002\u4e3a\u4e86\u964d\u4f4e\u4f7f\u7528\u95e8\u69db\uff0c\u4f7f\u5f97\u6574\u4e2a\u8fc7\u7a0b\u66f4\u52a0\u8f7b\u677e\uff0c\u6211\u4eec\u9700\u8981\u901a\u8fc7\u4e00\u4e9b\u624b\u6bb5\u5c06\u590d\u6742\u5ea6\u5c01\u88c5\u8d77\u6765\u3002\u8003\u8651\u5230\u76f8\u540c\u573a\u666f\u4e0b\u4e0d\u540c\u8868\u7684ETL\u5b9e\u73b0\u53ef\u80fd\u662f\u5341\u5206\u7c7b\u4f3c\u7684\uff0c\u6211\u4eec\u60f3\u5230\u4e86\u901a\u8fc7\u5b9a\u4e49\u6a21\u677f\u6765\u8fdb\u884c\u6570\u636e\u5efa\u6a21\uff0c\u901a\u8fc7\u6a21\u677f\u6765\u751f\u6210workflow\u8fd9\u6837\u7684\u65b9\u5f0f\u3002\u4f60\u5728 ",(0,n.kt)("a",{parentName:"p",href:"/docs/quick-start-guide#generate-sql-files-from-excel-config"},"quick start")," \u91cc\u9762\u80af\u5b9a\u4e5f\u89c1\u5230\u4e86\u901a\u8fc7\u586b\u5199excel\u6a21\u677f\u6765\u751f\u6210workflow\u7684\u65b9\u5f0f\u3002\u5b9e\u9645\u4e0a\u6211\u4eec\u8ba4\u4e3a\u9664\u4e86\u5177\u6709\u975e\u5e38\u7279\u6b8a\u903b\u8f91\u7684\u4efb\u52a1\u53ef\u4ee5\u624b\u5199\u4ee5\u5916\uff0c\u7edd\u5927\u591a\u6570\u4efb\u52a1\u90fd\u5e94\u8be5\u901a\u8fc7\u6a21\u677f\u5b9a\u4e49\uff0c\u7136\u540e\u751f\u6210\u3002\u8fd9\u6837\u6709\u51e0\u4e2a\u597d\u5904\uff1a"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"\u907f\u514d\u4e86\u624b\u5199\u4efb\u52a1\u53ef\u80fd\u9020\u6210\u7684\u6570\u636e\u5de5\u7a0b\u5316\u5b9e\u8df5\u4e0d\u4e00\u81f4\u751a\u81f3\u51fa\u9519"),(0,n.kt)("li",{parentName:"ul"},"\u5f53\u5927\u91cf\u4efb\u52a1\u9700\u8981\u4fee\u6539\u65f6\uff0c\u53ea\u9700\u8981\u4fee\u6539\u6a21\u677f\u5185\u5bb9\uff0c\u5e76\u91cd\u65b0\u751f\u6210\u5373\u53ef"),(0,n.kt)("li",{parentName:"ul"},"excel\u6a21\u677f\u4f5c\u4e3a\u5927\u5bb6\u90fd\u80fd\u7406\u89e3\u7684\u4e2d\u95f4\u4ea7\u7269\uff0c\u53ef\u4ee5\u4f5c\u4e3a\u6570\u636eBA\u548c\u6570\u636e\u5de5\u7a0b\u5e08DE\u7684\u6c9f\u901a\u6865\u6881\uff0c\u964d\u4f4e\u56e2\u961f\u95f4\u7684\u6c9f\u901a\u548c\u6469\u64e6\u6210\u672c"),(0,n.kt)("li",{parentName:"ul"},"\u56e2\u961f\u53ef\u4ee5\u4e0d\u9700\u91cd\u590d\u9020\u8f6e\u5b50\uff0c\u5728\u4e00\u4e2a\u65b0\u7684\u9879\u76ee\u53ef\u4ee5\u5feb\u901f\u8fdb\u5165\u4e1a\u52a1\u5f00\u53d1\uff0c\u51cf\u5c11\u5927\u91cf\u7684\u57fa\u7840\u8bbe\u7f6e\u6295\u5165")),(0,n.kt)("h3",{id:"\u6570\u636e\u8d28\u91cf\u95ee\u9898\u5206\u7ea7\u5206\u7c7b"},"\u6570\u636e\u8d28\u91cf\u95ee\u9898\u5206\u7ea7\u5206\u7c7b"),(0,n.kt)("p",null,"\u201c\u4e8b\u5b9e\u8868\u3001\u7ef4\u5ea6\u8868\u7684\u5173\u8054\u68c0\u67e5\u201d\u3001\u201c\u8bb0\u5f55\u8868\u4e0e\u8868\u5173\u8054\u8fc7\u7a0b\u4e2d\u7684\u672a\u77e5\u503c\u548c\u4e0d\u9002\u7528\u503c\u201d \u524d\u4e00\u7bc7\u6587\u7ae0\u5df2\u7ecf\u63d0\u8fc7\u4e86\uff0c\u8fd9\u91cc\u91cd\u70b9\u5c55\u5f00\u4e0b \u201c\u6570\u636e\u8d28\u91cf\u95ee\u9898\u5206\u7ea7\u5206\u7c7b\u201d"),(0,n.kt)("p",null,"\u9996\u5148\u770b\u4e00\u4e0b\u6211\u4eec\u7684\u6570\u636e\u8d28\u91cf\u68c0\u67e5\u914d\u7f6e\u6587\u4ef6\uff0c\u6211\u4eec\u5141\u8bb8\u901a\u8fc7\u7c7bSQL\u6216\u8005\u81ea\u5b9a\u4e49\u4ee3\u7801(User Defined Rule)\u7684\u65b9\u5f0f\u6765\u5b9a\u4e49\u6570\u636e\u8d28\u91cf\u89c4\u5219\u3002\u8fd9\u5176\u4e2d\u53ef\u4ee5\u4f7f\u7528 ",(0,n.kt)("inlineCode",{parentName:"p"},"$column")," \u6765\u5f15\u7528\u88ab\u5e94\u7528\u89c4\u5219\u7684column name\uff0c\u53ef\u4ee5\u4f7f\u7528UDF\uff0c\u4e5f\u53ef\u4ee5\u52a0\u8f7d\u4e00\u6bb5\u4ee3\u7801\u6765\u5b9e\u73b0\u6570\u636e\u8d28\u91cf\u68c0\u67e5\u7684\u89c4\u5219\u3002"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-yaml"},"- dataCheckType: power null check\n  rule: powerNullCheck($column)\n  errorType: error\n- dataCheckType: null check\n  rule: $column IS NULL\n  errorType: error\n- dataCheckType: duplicated check\n  rule: UDR.com.github.sharpdata.sharpetl.core.quality.udr.DuplicatedCheck\n  errorType: warn\n- dataCheckType: mismatch dim check\n  rule: $column = '-1'\n  errorType: warn\n")),(0,n.kt)("p",null,"\u914d\u7f6e\u6587\u4ef6\u4e2d\u5b9a\u4e49\u7684\u6570\u636e\u8d28\u91cf\u89c4\u5219\u662f\u5168\u5c40\u5171\u4eab\u7684\uff0c\u9700\u8981\u4f7f\u7528\u6570\u636e\u8d28\u91cf\u68c0\u67e5\u7684\u5730\u65b9\u53ef\u4ee5\u901a\u8fc7column.column name.qualityCheckRules=rule name\u5f15\u7528\u5df2\u6709\u7684\u89c4\u5219\uff1a"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-sql"},"-- step=1\n-- source=hive\n--  dbName=ods\n--  tableName=t_device\n--  options\n--   idColumn=order_id\n--   column.device_id.qualityCheckRules=power null check\n--   column.status.qualityCheckRules=empty check\n-- target=temp\n--  tableName=t_device__extracted\n-- writeMode=overwrite\n\nselect ....\n")),(0,n.kt)("p",null,"\u6570\u636e\u8d28\u91cf\u89c4\u5219\u540c\u65f6\u652f\u6301\u5b9a\u4e49error\u548cwarn\u7ea7\u522b\u7684\u9519\u8bef\uff0c\u901a\u5e38warn\u7ea7\u522b\u7684\u9519\u8bef\u662f\u53ef\u4ee5\u63a5\u53d7\u7684\uff0c\u800cerror\u7ea7\u522b\u7684\u9519\u8bef\u662f\u4e0d\u88ab\u8ba4\u4e3a\u53ef\u63a5\u53d7\u7684\uff0c\u5f80\u5f80\u4e0d\u4f1a\u8fdb\u5165\u6570\u636e\u4ed3\u5e93\u3002\u4f46\u662ferror\u548cwarn\u7ea7\u522b\u7684\u9519\u8bef\u90fd\u4f1a\u5206\u7ea7\u5206\u7c7b\u8bb0\u5f55\u4e0b\u6765\uff0c\u65b9\u4fbf\u540e\u7eed\u6392\u9519\u6216\u8005\u53d1\u9001\u901a\u77e5\u6765\u89e3\u51b3\u95ee\u9898\u3002"),(0,n.kt)("p",null,(0,n.kt)("em",{parentName:"p"},"\u4e0b\u4e00\u7bc7\u6587\u7ae0\u5c06\u5177\u4f53\u4ecb\u7ecd\u65e5\u5fd7\u9a71\u52a8\u7684\u903b\u8f91\u548c\u5b9e\u73b0\u3002")))}s.isMDXComponent=!0}}]);