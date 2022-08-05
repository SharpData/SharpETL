"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[950],{3905:(e,t,n)=>{n.d(t,{Zo:()=>s,kt:()=>m});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function l(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?l(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):l(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},l=Object.keys(e);for(a=0;a<l.length;a++)n=l[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(a=0;a<l.length;a++)n=l[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var p=a.createContext({}),d=function(e){var t=a.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},s=function(e){var t=d(e.components);return a.createElement(p.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},c=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,l=e.originalType,p=e.parentName,s=o(e,["components","mdxType","originalType","parentName"]),c=d(n),m=r,_=c["".concat(p,".").concat(m)]||c[m]||u[m]||l;return n?a.createElement(_,i(i({ref:t},s),{},{components:n})):a.createElement(_,i({ref:t},s))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var l=n.length,i=new Array(l);i[0]=c;var o={};for(var p in t)hasOwnProperty.call(t,p)&&(o[p]=t[p]);o.originalType=e,o.mdxType="string"==typeof e?e:r,i[1]=o;for(var d=2;d<l;d++)i[d]=n[d];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}c.displayName="MDXCreateElement"},3503:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>i,default:()=>u,frontMatter:()=>l,metadata:()=>o,toc:()=>d});var a=n(7462),r=(n(7294),n(3905));const l={slug:"\u4e3a\u4ec0\u4e48\u6211\u4eec\u9700\u8981Sharp ETL",title:"\u4e3a\u4ec0\u4e48\u6211\u4eec\u9700\u8981Sharp ETL",tags:["sharp etl"]},i=void 0,o={permalink:"/SharpETL/blog/\u4e3a\u4ec0\u4e48\u6211\u4eec\u9700\u8981Sharp ETL",editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/blog/why-we-need-sharp-etl.md",source:"@site/blog/why-we-need-sharp-etl.md",title:"\u4e3a\u4ec0\u4e48\u6211\u4eec\u9700\u8981Sharp ETL",description:"\u5bfc\u8a00",date:"2022-08-05T03:10:38.000Z",formattedDate:"August 5, 2022",tags:[{label:"sharp etl",permalink:"/SharpETL/blog/tags/sharp-etl"}],readingTime:11.995,hasTruncateMarker:!1,authors:[],frontMatter:{slug:"\u4e3a\u4ec0\u4e48\u6211\u4eec\u9700\u8981Sharp ETL",title:"\u4e3a\u4ec0\u4e48\u6211\u4eec\u9700\u8981Sharp ETL",tags:["sharp etl"]}},p={authorsImageUrls:[]},d=[{value:"\u5bfc\u8a00",id:"\u5bfc\u8a00",level:2},{value:"\u4e4b\u524d\u7684ETL\u6709\u4ec0\u4e48\u95ee\u9898",id:"\u4e4b\u524d\u7684etl\u6709\u4ec0\u4e48\u95ee\u9898",level:2},{value:"\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\u7f3a\u5931",id:"\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\u7f3a\u5931",level:3},{value:"\u7406\u60f3\u4e2d\u7684ETL\u5e94\u8be5\u4ec0\u4e48\u6837\u5b50\uff1f",id:"\u7406\u60f3\u4e2d\u7684etl\u5e94\u8be5\u4ec0\u4e48\u6837\u5b50",level:2},{value:"Sharp ETL workflow\u793a\u610f",id:"sharp-etl-workflow\u793a\u610f",level:3},{value:"One more thing!",id:"one-more-thing",level:2}],s={toc:d};function u(e){let{components:t,...n}=e;return(0,r.kt)("wrapper",(0,a.Z)({},s,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h2",{id:"\u5bfc\u8a00"},"\u5bfc\u8a00"),(0,r.kt)("p",null,"\u672c\u6587\u7ed3\u5408\u76ee\u524d\u7684\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\uff0c\u5c1d\u8bd5\u5c55\u5f00\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\u4e2dETL\u7684\u539f\u5219\uff0c\u5e76\u5bf9Sharp ETL\u505a\u7b80\u8981\u4ecb\u7ecd\u3002"),(0,r.kt)("p",null,"ETL\u6216ELT\u662f\u8fdb\u884c\u6570\u636e\u5904\u7406\u7684\u5e38\u89c1\u624b\u6bb5\u4e4b\u4e00\uff0c\u968f\u7740\u6570\u636e\u5e73\u53f0\u6e21\u8fc7\u86ee\u8352\u65f6\u4ee3\u5f00\u59cb\u7cbe\u7ec6\u5316\u6cbb\u7406\uff0c\u539f\u59cb\u7684\u7f16\u7801\u65b9\u5f0f\u548c\u65e0\u7ec4\u7ec7\u7684SQL\u811a\u672c\u5df2\u7ecf\u4e0d\u80fd\u6ee1\u8db3\u9700\u6c42\u3002"),(0,r.kt)("h2",{id:"\u4e4b\u524d\u7684etl\u6709\u4ec0\u4e48\u95ee\u9898"},"\u4e4b\u524d\u7684ETL\u6709\u4ec0\u4e48\u95ee\u9898"),(0,r.kt)("p",null,"\u5e38\u89c1\u7684ETL\u5f62\u5f0f\u6709\u51e0\u79cd\uff0c\u8fd9\u51e0\u79cd\u65b9\u5f0f\u5404\u6709\u4f18\u52a3\uff0c\u6211\u4eec\u5206\u522b\u4ece\u51e0\u4e2a\u7ef4\u5ea6\u5c55\u5f00\u4e00\u4e0b\uff1a"),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("strong",{parentName:"li"},"\u4ee3\u7801\u65b9\u5f0f"))),(0,r.kt)("p",null,"\u4ee3\u7801\u65b9\u5f0f\u662f\u975e\u5e38\u5e38\u89c1\u7684ETL\u7f16\u5199\u65b9\u5f0f\uff0c\u5e7f\u6cdb\u9002\u7528\u4e8e\u5404\u79cd\u573a\u666f\u3002"),(0,r.kt)("p",null,"pros\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"\u4ee3\u7801\u7f16\u5199\u7684ETL\u66f4\u5bb9\u6613\u968f\u610f\u6269\u5c55\u3001\u589e\u52a0\u81ea\u5b9a\u4e49\u903b\u8f91\u548c\u590d\u7528\u4ee3\u7801\u903b\u8f91\u3002")),(0,r.kt)("p",null,"cons\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"\u4f46\u662f\u4ee3\u7801\u5c31\u662f\u4ee3\u7801\uff0c\u5927\u591a\u6570\u4ee3\u7801\u90fd\u662f\u547d\u4ee4\u5f0f\u903b\u8f91\uff0c\u5e76\u4e0d\u5bb9\u6613\u7ef4\u62a4\u548c\u7406\u89e3\u3002")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("p",{parentName:"li"},"\u4ee3\u7801\u5f62\u5f0f\u7f16\u5199ETL\u4f1a\u4f7f\u5f97\u4efb\u52a1\u5f00\u53d1\u6d41\u7a0b\u66f4\u52a0\u91cd\u91cf\u7ea7\uff0c\u56e0\u4e3a\u4e00\u65e6\u4fee\u6539\u5b9e\u73b0\u903b\u8f91\uff0c\u5c31\u9700\u8981\u91cd\u65b0\u90e8\u7f72jar\u5305\u3002\u8003\u8651\u5230\u5b89\u5168\u56e0\u7d20\uff0c\u5728\u4e00\u4e9b\u4f01\u4e1a\u5185\u90e8\u7684\u591a\u79df\u6237\u5e73\u53f0\u4e0a\uff0c\u4e0a\u4f20\u65b0\u7684jar\u5305\u662f\u9700\u8981\u5ba1\u6838\u7684\uff0c\u6574\u4e2a\u6d41\u7a0b\u5c31\u4f1a\u5728\u5ba1\u6838\u8fd9\u91cc\u663e\u8457\u6162\u4e86\u4e0b\u6765\u3002\u5373\u4f7f\u4e0d\u9700\u8981\u5ba1\u6838\uff0c\u4e5f\u9700\u8981\u624b\u52a8\u90e8\u7f72\u5230\u73af\u5883\u4e0a\uff0c\u4e5f\u6709\u53ef\u80fd\u56e0\u4e3a\u589e\u52a0\u4e86\u65b0\u7684\u903b\u8f91\u5f71\u54cd\u4e86\u5176\u4ed6\u6b63\u5e38\u8fd0\u884c\u7684\u4efb\u52a1\u3002"))),(0,r.kt)("ol",{start:2},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("strong",{parentName:"li"},"\u6258\u62c9\u62fd"))),(0,r.kt)("p",null,"\u6258\u62c9\u62fd\u5728\u65b0\u5174\u4e91\u5e73\u53f0\u548c\u4f01\u4e1a\u81ea\u5efa\u5e73\u53f0\u4e0a\u975e\u5e38\u5e38\u89c1\uff0c\u51e0\u4e4e\u662f\u6240\u6709\u5e73\u53f0\u7684\u6807\u914d\u3002"),(0,r.kt)("p",null,"pros\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"DAG\u5f62\u5f0f\u4e5f\u5f88\u5bb9\u6613\u7406\u89e3\uff0c\u5f00\u53d1\u95e8\u69db\u4f4e\uff0c\u53ea\u9700\u8981\u586b\u5165\u53c2\u6570\uff0c\u8f7b\u677e\u5b9e\u73b0\u6570\u636e\u63a5\u5165\u3001\u6570\u636e\u53bb\u91cd\u7b49\u7b49\u903b\u8f91\u3002"),(0,r.kt)("li",{parentName:"ul"},"\u589e\u52a0\u3001\u4fee\u6539\u903b\u8f91\u90fd\u76f8\u5f53\u8f7b\u91cf\u7ea7\uff0c\u751a\u81f3\u5927\u591a\u6570\u5e73\u53f0\u4e5f\u5b9e\u73b0\u4e86\u7248\u672c\u7ba1\u7406\uff0c\u53ef\u4ee5\u50cf\u7ba1\u7406\u4ee3\u7801\u4e00\u6837\u7ba1\u7406\u4efb\u52a1\uff0c\u53ef\u4ee5\u8f7b\u677e\u505a\u5230\u56de\u9000\u7248\u672c\u3001\u89e3\u51b3\u51b2\u7a81\u7b49\u7c7b\u4f3cgit\u7684\u64cd\u4f5c\u3002"),(0,r.kt)("li",{parentName:"ul"},"\u90e8\u5206\u5e73\u53f0\u53ef\u4ee5\u901a\u8fc7\u5c0f\u7684\u7ec4\u4ef6\u7ec4\u7ec7\u6210\u4e3a\u66f4\u5927\u7684\u7ec4\u4ef6\u6765\u590d\u7528ETL\u7684\u903b\u8f91\u3002")),(0,r.kt)("p",null,"cons\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"\u81ea\u5b9a\u4e49\u903b\u8f91\u8f83\u56f0\u96be\uff0c\u751a\u81f3\u5728\u4e00\u4e9b\u5c01\u95ed\u6027\u8f83\u9ad8\u7684\u5e73\u53f0\u4e0a\u589e\u52a0\u65b0\u7684\u6570\u636e\u6e90\u90fd\u662f\u95ee\u9898\u3002"),(0,r.kt)("li",{parentName:"ul"},"\u754c\u9762\u64cd\u4f5c\u96be\u4ee5\u81ea\u52a8\u5316\uff0c\u5f53\u9700\u8981\u6279\u91cf\u9488\u5bf9\u82e5\u5e72\u4efb\u52a1\u8fdb\u884c\u67d0\u9879\u64cd\u4f5c\u65f6\uff0c\u56fe\u5f62\u5316\u754c\u9762\u5c31\u4e0d\u65b9\u4fbf\u4e86\u3002\u6709\u65f6\u6211\u4eec\u4f1a\u9047\u5230\u67d0\u4e2a\u4e1a\u52a1\u7cfb\u7edf\u5185\u9700\u8981\u7684ETL\u7684\u903b\u8f91\u90fd\u5f88\u7c7b\u4f3c\uff0c\u4f46\u662f\u4efb\u52a1\u6570\u91cf\u5f88\u591a\uff0c\u624b\u52a8\u6258\u62c9\u62fd\u5f88\u8d39\u4e8b\u4e5f\u5bb9\u6613\u51fa\u9519\u3002")),(0,r.kt)("ol",{start:3},(0,r.kt)("li",{parentName:"ol"},(0,r.kt)("strong",{parentName:"li"},"\u7eafSQL\u811a\u672c"))),(0,r.kt)("p",null,"pros\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"\u7ecf\u8fc7\u5408\u9002\u62c6\u5206\u7684SQL\u903b\u8f91\u66f4\u5bb9\u6613\u7406\u89e3\uff08\u5355\u6761SQL\u7684\u884c\u6570\u9700\u8981\u6709\u9650\u5236\uff09\uff0c\u58f0\u660e\u5f0f\u5b9e\u73b0\u66f4\u52a0\u8868\u610f\uff0c\u57fa\u672c\u4e0a\u4eba\u4eba\u90fd\u4f1aSQL\uff0c\u95e8\u69db\u5f88\u4f4e\u3002")),(0,r.kt)("p",null,"cons\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"\u903b\u8f91\u96be\u4ee5\u590d\u7528\uff0c\u7ecf\u5e38\u9700\u8981\u590d\u5236\u53e6\u5916\u4e00\u6bb5SQL\u7684\u903b\u8f91\u3002"),(0,r.kt)("li",{parentName:"ul"},"\u7f3a\u4e4f\u7cfb\u7edf\u6027\u7f16\u6392\uff0c\u7eafSQL\u811a\u672c\u5f80\u5f80\u6bd4\u8f83\u6563\u4e71\u3002"),(0,r.kt)("li",{parentName:"ul"},"\u6269\u5c55\u53d7\u9650\uff0c\u867d\u7136\u5404\u79cdSQL\u65b9\u8a00\u57fa\u672c\u4e0a\u90fd\u662f\u652f\u6301UDF\uff08User Defined Function\uff09\u7684\uff0c\u4f46\u662f\u6269\u5c55\u80fd\u529b\u4ecd\u7136\u6bd4\u4e0d\u4e0a\u4ee3\u7801\uff08\u6bd4\u5982\u9700\u8981\u548c\u67d0\u4e2aHTTP API\u4ea4\u4e92\uff09\u3002")),(0,r.kt)("h3",{id:"\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\u7f3a\u5931"},"\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\u7f3a\u5931"),(0,r.kt)("p",null,"\u6211\u4eec\u8ba4\u4e3a\uff0c\u5728ETL\u5f00\u53d1\u4e2d\u9700\u8981\u5305\u62ec\u8fd9\u4e9b\u5de5\u7a0b\u5b9e\u8df5\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"\u901a\u8fc7\u4e8b\u5b9e\u8868\u548c\u7ef4\u5ea6\u8868\u7684\u5173\u8054\u6765\u68c0\u67e5\u6570\u636e\u4e00\u81f4\u6027\u3001\u5b8c\u6574\u6027"),(0,r.kt)("li",{parentName:"ul"},"\u901a\u8fc7\u7279\u6b8a\u503c\u6765\u8bb0\u5f55\u8868\u4e0e\u8868\u5173\u8054\u8fc7\u7a0b\u4e2d\u7684\u672a\u77e5\u503c(\u5173\u8054\u4e0d\u4e0a)\u548c\u4e0d\u9002\u7528\u503c(\u4e0d\u5408\u7406\u503c)"),(0,r.kt)("li",{parentName:"ul"},"\u6839\u636e\u6570\u636e\u8d28\u91cf\u68c0\u67e5\u89c4\u5219\u5728\u65e5\u5fd7\u4e2d\u5206\u7ea7\u5206\u7c7b\u8bb0\u5f55\u6570\u636e\u8d28\u91cf\u95ee\u9898"),(0,r.kt)("li",{parentName:"ul"},"\u901a\u8fc7\u8bb0\u5f55\u8c03\u5ea6\u65e5\u5fd7\u6765\u4e0e\u7279\u5b9a\u8c03\u5ea6\u670d\u52a1\u89e3\u8026\u5408\uff0c\u589e\u5f3a\u8c03\u5ea6\u7684\u9c81\u68d2\u6027\uff0c\u540c\u65f6\u4e5f\u80fd\u591f\u7ed3\u6784\u5316\u7684\u8bb0\u5f55\u4efb\u52a1\u8fd0\u884c\u8fc7\u7a0b\u4e2d\u7684\u4fe1\u606f\uff0c\u65b9\u4fbf\u6392\u9519")),(0,r.kt)("p",null,"\u800c\u65e0\u8bba\u662f\u4ee3\u7801\u7f16\u5199\u3001\u6258\u62c9\u62fd\u8fd8\u662f\u7eafSQL\u811a\u672c\u90fd\u56e0\u4e3a\u8fc7\u591a\u4eba\u5de5\u64cd\u4f5c\u4f7f\u5f97\u8fd9\u4e9b\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\u96be\u4ee5\u7edf\u4e00\uff0c\u5e38\u5e38\u6709\u4ee5\u4e0b\u95ee\u9898\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"\u53ea\u505a\u5173\u8054\u800c\u5e76\u4e0d\u8bb0\u5f55\u6570\u636e\u4e00\u81f4\u6027\u3001\u5b8c\u6574\u6027\u95ee\u9898\u3002\u751a\u81f3\u7f3a\u4e4f\u6700\u57fa\u672c\u7684\u5173\u8054\u3002"),(0,r.kt)("li",{parentName:"ul"},"\u7531\u4e8e\u65f6\u95f4\u7d27\u5f20\u3001\u5b9e\u73b0\u590d\u6742\u7b49\u56e0\u7d20\uff0c\u6570\u636e\u8d28\u91cf\u68c0\u67e5\u64cd\u4f5c\u7f3a\u5931\uff0c\u5bfc\u81f4\u8fdb\u5165\u6570\u4ed3\u7684\u6570\u636e\u7f3a\u4e4f\u6700\u57fa\u672c\u7684\u8d28\u91cf\u4fdd\u8bc1\u3002"),(0,r.kt)("li",{parentName:"ul"},"\u6570\u636e\u8fd0\u7ef4\u96be\u4ee5\u81ea\u52a8\u5316\uff0c\u4efb\u52a1\u5931\u8d25\u540e\u591a\u9700\u8981\u4eba\u5de5\u4ecb\u5165\u540e\u624d\u53ef\u4ee5\u4f7f\u4efb\u52a1\u6062\u590d\u6b63\u5e38\u3002"),(0,r.kt)("li",{parentName:"ul"},"\u4efb\u52a1\u8c03\u5ea6\u8fc7\u5ea6\u4f9d\u8d56\u4e8e\u5df2\u6709\u8c03\u5ea6\u670d\u52a1\uff0c\u7f3a\u4e4f\u9632\u5446\u8bbe\u8ba1\uff0c\u96be\u4ee5\u89e3\u51b3\u91cd\u590d\u8c03\u5ea6\u7684\u95ee\u9898\uff08\u5047\u5982\u8c03\u5ea6\u670d\u52a1\u51fa\u73b0\u5f02\u5e38\uff0c\u4efb\u52a1\u88ab\u91cd\u590d\u8c03\u5ea6\u4e86\uff0c\u7ed3\u679c\u5c31\u662f\u91cd\u590d\u8ba1\u7b97\uff09"),(0,r.kt)("li",{parentName:"ul"},"\u9519\u8bef\u6392\u67e5\u8fc7\u5ea6\u4f9d\u8d56\u4e8e\u4efb\u52a1\u6267\u884c\u65e5\u5fd7\uff0c\u6392\u67e5\u9519\u8bef\u591a\u4f9d\u8d56\u4e8e\u4e0d\u505c\u7684\u63a8\u65ad\u3001\u52a0\u65e5\u5fd7\u548c\u5c1d\u8bd5\u3002\u5982\u679c\u662f\u7eafSQL\u811a\u672c\uff0c\u5219\u7ecf\u5e38\u9047\u5230\u51e0\u5343\u884c\u7684SQL\u4efb\u52a1\u5931\u8d25\uff0c\u7136\u540e\u4e0d\u77e5\u9053\u4ece\u54ea\u91cc\u5f00\u59cbdebug\u7684\u7a98\u5883\u3002")),(0,r.kt)("h2",{id:"\u7406\u60f3\u4e2d\u7684etl\u5e94\u8be5\u4ec0\u4e48\u6837\u5b50"},"\u7406\u60f3\u4e2d\u7684ETL\u5e94\u8be5\u4ec0\u4e48\u6837\u5b50\uff1f"),(0,r.kt)("p",null,"\u8fd1\u4e9b\u5e74\u6765\u6700\u706b\u70ed\u7684\u83ab\u8fc7\u4e8e\u6d41\u5904\u7406\u3001\u6d41\u5f0fETL\u8fd9\u4e9b\u6982\u5ff5\u3002\u867d\u7136\u8fd9\u7bc7\u6587\u7ae0\u4e0e\u6d41\u6216\u8005\u6279\u5904\u7406\u6ca1\u6709\u76f4\u63a5\u5173\u7cfb\uff0c\u8fd8\u662f\u8981\u6307\u51fa\uff0c\u65e0\u8bba\u662f\u6d41\u5f0f\u5904\u7406\u8fd8\u662f\u6279\u5904\u7406\uff0c\u6700\u57fa\u672c\u7684\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\u90fd\u9700\u8981\u5177\u5907\uff0c\u8fd9\u4e9b\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\u90fd\u662f\u5b9e\u9645\u5b9e\u8df5\u4e2d\u4e0d\u53ef\u6216\u7f3a\u7684\u4e00\u90e8\u5206\u3002\u4ee5\u4e0b\u8ba8\u8bba\u4e14\u629b\u5f00\u6d41\u5f0f\u6216\u6279\u5904\u7406\u4e0d\u8c08\u3002"),(0,r.kt)("p",null,"\u7406\u60f3\u4e2d\u7684ETL\u5e94\u8be5\u80fd\u7ed3\u5408\u4ee5\u4e0a\u96c6\u4e2d\u5b9e\u73b0\u65b9\u5f0f\u7684\u4f18\u70b9\uff0c\u6452\u5f03\u7f3a\u70b9\uff0c\u6211\u4eec\u53ef\u4ee5\u6765\u6784\u601d\u4e00\u4e0b\uff1a"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"\u62e5\u6709SQL\u7684\u8bed\u4e49\u5316\u8868\u8fbe\u80fd\u529b"),(0,r.kt)("li",{parentName:"ul"},"\u652f\u6301\u901a\u8fc7\u81ea\u5b9a\u4e49\u4ee3\u7801\u903b\u8f91\u6269\u5c55"),(0,r.kt)("li",{parentName:"ul"},"\u652f\u6301\u4ee3\u7801\u4f18\u5148\u3001\u7248\u672c\u7ba1\u7406"),(0,r.kt)("li",{parentName:"ul"},"\u5728\u4ee3\u7801\u4f18\u5148\u7684\u57fa\u7840\u4e0a\u80fd\u591f\u652f\u6301\u53ef\u89c6\u5316\u5de5\u5177\u6784\u5efa"),(0,r.kt)("li",{parentName:"ul"},"\u80fd\u591f\u5bf9\u8fc7\u957f\u7684SQL\u8fdb\u884c\u62c6\u89e3\uff0c\u964d\u4f4e\u7406\u89e3\u96be\u5ea6"),(0,r.kt)("li",{parentName:"ul"},"\u5185\u7f6e\u7edf\u4e00\u4e14\u6807\u51c6\u7684\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"\u4e8b\u5b9e\u8868\u548c\u7ef4\u5ea6\u8868\u7684\u5173\u8054\u68c0\u67e5"),(0,r.kt)("li",{parentName:"ul"},"\u8bb0\u5f55\u8868\u4e0e\u8868\u5173\u8054\u8fc7\u7a0b\u4e2d\u7684\u672a\u77e5\u503c\u548c\u4e0d\u9002\u7528\u503c"),(0,r.kt)("li",{parentName:"ul"},"\u6570\u636e\u8d28\u91cf\u95ee\u9898\u5206\u7ea7\u5206\u7c7b\u8bb0\u5f55"),(0,r.kt)("li",{parentName:"ul"},"\u901a\u8fc7\u8c03\u5ea6\u65e5\u5fd7\u89e3\u8026\u5408\u8c03\u5ea6\u7cfb\u7edf")))),(0,r.kt)("h3",{id:"sharp-etl-workflow\u793a\u610f"},"Sharp ETL workflow\u793a\u610f"),(0,r.kt)("p",null,"\u4e0b\u9762\u793a\u610f\u4e00\u4e0bSharp ETL\u7684\u4e00\u4e2aworkflow\uff0c\u6211\u4f1a\u5206\u522b\u5bf9\u4e8e\u8fd9\u4e9bstep\u505a\u51fa\u89e3\u91ca\u3002"),(0,r.kt)("p",null,"workflow\u4e00\u5f00\u59cb\u662f\u4e00\u4e2aheader\uff0c\u63cf\u8ff0workflow\u672c\u8eab\u7684\u5c5e\u6027\uff0c\u5305\u62ecworkflow\u540d\u79f0\u3001\u589e\u91cf\u8fd8\u662f\u5168\u91cf\u3001\u65e5\u5fd7\u9a71\u52a8\u65b9\u5f0f\u3001\u901a\u77e5\u65b9\u5f0f\u7b49\u7b49\u3002\u7136\u540e\u662f\u4e00\u7cfb\u5217step\uff0c\u6bcf\u4e2astep\u90fd\u6709\u81ea\u5df1\u7684\u8f93\u5165\u8f93\u51fa\u3002"),(0,r.kt)("p",null,"\u7b2c\u4e00\u4e2astep\u8868\u793a\u4ecehive\u4e2d\u53d6\u6570\u636e\u51fa\u6765\u5199\u5230temp\u8868\u4ee5\u4fbf\u540e\u7eed\u4f7f\u7528\uff0c\u8fd9\u4e2aSQL\u4e2d\u4e5f\u7528\u5230\u4e86Sharp ETL\u5185\u7f6e\u7684\u53d8\u91cf\u529f\u80fd\uff0c",(0,r.kt)("inlineCode",{parentName:"p"},"${YEAR}")," ",(0,r.kt)("inlineCode",{parentName:"p"},"${MONTH}")," ",(0,r.kt)("inlineCode",{parentName:"p"},"${DAY}")," \u8fd9\u4e9b\u53d8\u91cf\u90fd\u4f1a\u5728\u8fd0\u884c\u65f6\u88ab\u66ff\u6362\u4e3a\u771f\u5b9e\u7684\u503c\uff0c\u771f\u5b9e\u503c\u901a\u8fc7\u6211\u4eec\u5185\u7f6e\u7684\u8c03\u5ea6\u65e5\u5fd7\u6765\u8ba1\u7b97\u5f97\u51fa\u3002\u8fd9\u91cc\u8bf7\u6ce8\u610f\u5728source\u7684options\u4e2d\u6709\u914d\u7f6e\u6570\u636e\u8d28\u91cf\u68c0\u67e5\uff0c\u8fd9\u91cc\u53ea\u9700\u8981\u5f15\u7528\u4e0d\u540c\u7684\u6570\u636e\u8d28\u91cf\u68c0\u67e5\u89c4\u5219\uff0c\u5373\u53ef\u5bf9\u76f8\u5e94\u5b57\u6bb5\u8fdb\u884c\u6570\u636e\u8d28\u91cf\u68c0\u67e5\uff0c\u5e76\u6839\u636e\u89c4\u5219\u5b9a\u4e49\u7684\u7ea7\u522b\u8fdb\u884c\u5206\u7ea7\u5206\u7c7b\u7ba1\u7406"),(0,r.kt)("p",null,"\u7b2c\u4e8c\u4e2astep\u662f\u8fdb\u884c\u4e8b\u5b9e\u8868\u548c\u7ef4\u5ea6\u8868\u7684\u5173\u8054\uff0c\u5e76\u901a\u8fc7",(0,r.kt)("inlineCode",{parentName:"p"},"-1"),"\u6765\u8bb0\u5f55\u672a\u5173\u8054\u4e0a\u7684\u6570\u636e\u3002"),(0,r.kt)("p",null,"\u7b2c\u4e09\u4e2astep\u662f\u5bf9temp\u7684\u6570\u636e\u505a\u9884\u5904\u7406\u3002"),(0,r.kt)("p",null,"\u7b2c\u56db\u4e2astep\u662f\u5bf9hive\u6570\u636e\u66f4\u65b0\u7684\u7279\u6b8a\u5904\u7406\uff0c\u5728hive\u4e0a\u5982\u679c\u8981\u66f4\u65b0\u6570\u636e\u5c31\u9700\u8981\u628a\u6570\u636e\u5bf9\u5e94\u7684\u5206\u533a\u5168\u90e8\u66f4\u65b0\u6389\uff0c\u6240\u4ee5\u8fd9\u91cc\u662f\u5728\u8ba1\u7b97\u672c\u6b21\u6570\u636e\u6d89\u53ca\u5230\u66f4\u65b0\u54ea\u4e9b\u5206\u533a\uff0c\u4ee5\u65b9\u4fbf\u540e\u7eed\u5c06\u8fd9\u4e9b\u5206\u533a\u7684\u6570\u636e\u53d6\u51fa\u6765\u505a\u66f4\u65b0\u3002"),(0,r.kt)("p",null,"\u7b2c\u4e94\u4e2astep\u7528\u5230\u4e86\u4e0a\u4e00\u4e2astep\u62fc\u63a5\u7684\u67e5\u8be2\u6761\u4ef6\u53c2\u6570\uff0c\u5c06\u6b64\u6b21\u66f4\u65b0\u8bbe\u8ba1\u5230\u7684\u6570\u636e\u5206\u533a\u90fd\u53d6\u51fa\u6765\u3002"),(0,r.kt)("p",null,"\u7b2c\u516d\u4e2astep\u662f\u901a\u8fc7Transformer\u529f\u80fd\u8c03\u7528\u4e00\u6bb5\u4ee3\u7801\u6765\u5b9e\u73b0\u62c9\u94fe\u8868\uff0c\u5176\u5b9e\u8fd9\u6bb5\u4ee3\u7801\u4e5f\u662f\u901a\u8fc7\u62fc\u63a5SQL\u5b9e\u73b0\u7684\uff0c\u53ea\u4e0d\u8fc7\u62fc\u63a5\u8fc7\u7a0b\u8f83\u4e3a\u7075\u6d3b\uff0c\u624d\u4ee5\u4e00\u4e2atransformer\u6765\u5c01\u88c5\u3002\u5b9e\u9645\u4e0atransformer\u4e2d\u53ef\u4ee5\u6267\u884c\u4efb\u610f\u4ee3\u7801\u3002transformer\u672c\u8eab\u4e5f\u6709\u8f93\u51fa\uff0c\u8fd9\u91cc\u6211\u4eec\u5c06\u8f93\u51fa\u8986\u76d6\u5230\u5bf9\u5e94\u7684\u5206\u533a\uff0c\u5b8c\u6210\u4efb\u52a1\u3002"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"-- workflow=fact_device\n--  loadType=incremental\n--  logDrivenType=timewindow\n\n-- step=1\n-- source=hive\n--  dbName=ods\n--  tableName=t_device\n--  options\n--   idColumn=order_id\n--   column.device_id.qualityCheckRules=power null check(error)\n--   column.status.qualityCheckRules=empty check(warn)\n-- target=temp\n--  tableName=t_device__extracted\n-- writeMode=overwrite\nselect\n    `device_id` as `device_id`,\n    `manufacturer` as `manufacturer`,\n    `status` as `status`,\n    `online` as `online`,\n    `create_time` as `create_time`,\n    `update_time` as `update_time`,\n    `year` as `year`,\n    `month` as `month`,\n    `day` as `day`\nfrom `ods`.`t_device`\nwhere `year` = '${YEAR}'\n  and `month` = '${MONTH}'\n  and `day` = '${DAY}';\n\n-- step=2\n-- source=temp\n--  tableName=t_device__extracted\n-- target=temp\n--  tableName=t_device__joined\n-- writeMode=append\nselect\n    `t_device__extracted`.*,\n    case when `t_dim_user`.`user_code` is null then '-1'\n        else `t_dim_user`.`user_code`\n    end as `user_id`\nfrom `t_device__extracted`\nleft join `dim`.`t_dim_user` `t_dim_user`\n on `t_device__extracted`.`user_id` = `t_dim_user`.`user_code`\n and `t_device__extracted`.`create_time` >= `t_dim_user`.`start_time`\n and (`t_device__extracted`.`create_time` < `t_dim_user`.`end_time`\n      or `t_dim_user`.`end_time` is null);\n\n-- step=3\n-- source=temp\n--  tableName=t_device__joined\n-- target=temp\n--  tableName=t_device__target_selected\n-- writeMode=overwrite\nselect\n    `device_id`,\n    `manufacturer`,\n    `user_id`,\n    `status`,\n    `online`,\n    `create_time`,\n    `update_time`,\n    `year`,\n    `month`,\n    `day`\nfrom `t_device__joined`;\n\n-- step=4\n-- source=hive\n--  dbName=dwd\n--  tableName=t_fact_device\n-- target=variables\nselect concat('where (',\n  ifEmpty(\n    concat_ws(')\\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),\n    '1 = 1'),\n  ')') as `DWD_UPDATED_PARTITION`\nfrom (\n  select\n  dwd.*\n  from `dwd`.`t_fact_device` dwd\n  left join `t_device__target_selected` incremental_data on dwd.device_id = incremental_data.device_id\n  where incremental_data.device_id is not null\n        and dwd.is_latest = 1\n);\n\n-- step=5\n-- source=hive\n--  dbName=dwd\n--  tableName=t_fact_device\n-- target=temp\n--  tableName=t_fact_device__changed_partition_view\nselect *\nfrom `dwd`.`t_fact_device`\n${DWD_UPDATED_PARTITION};\n\n-- step=6\n-- source=transformation\n--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer\n--  methodName=transform\n--   createTimeField=create_time\n--   dwUpdateType=incremental\n--   dwViewName=t_fact_device__changed_partition_view\n--   odsViewName=t_device__target_selected\n--   partitionField=create_time\n--   partitionFormat=year/month/day\n--   primaryFields=device_id\n--   surrogateField=\n--   timeFormat=yyyy-MM-dd HH:mm:ss\n--   updateTimeField=update_time\n--  transformerType=object\n-- target=hive\n--  dbName=dwd\n--  tableName=t_fact_device\n-- writeMode=overwrite\n")),(0,r.kt)("h2",{id:"one-more-thing"},"One more thing!"),(0,r.kt)("p",null,"\u5176\u5b9e\u4e0a\u4e00\u6bb5SQL\u662f\u901a\u8fc7\u4ee3\u7801\u751f\u6210\u7684\uff0c\u5e76\u975e\u4eba\u5de5\u624b\u5199\uff01\u5728\u5b9e\u9645\u64cd\u4f5c\u4e2d\uff0c\u53ea\u9700\u8981\u586b\u5199\u6211\u4eec\u7684excel ",(0,r.kt)("a",{parentName:"p",href:"https://docs.google.com/spreadsheets/d/1Zn_Q-QUTf6us4RwdgUgBosXL09-D-TowmgwWlDskvlA"},"ods\u6a21\u677f")," \u6216 ",(0,r.kt)("a",{parentName:"p",href:"https://docs.google.com/spreadsheets/d/1CetkqBsXj_E8oZBsws9iGdaJB1QJUajnwqH4FoKhXKA"},"dwd\u6a21\u677f"),"\u5373\u53ef\u751f\u6210\u4ee3\u7801\uff0c\u53ef\u4ee5\u6210\u500d\u7684\u63d0\u9ad8\u5f00\u53d1\u6548\u7387\u3002"),(0,r.kt)("p",null,"\u540e\u7eed\u6587\u7ae0\u8fd8\u5c06\u5bf9\u5185\u7f6e\u7684\u6570\u636e\u5de5\u7a0b\u5b9e\u8df5\uff08\u5305\u62ec\u4f46\u4e0d\u9650\u4e8e\u201c\u65e5\u5fd7\u9a71\u52a8\u201d\u3001\u201c\u6570\u636e\u8d28\u91cf\u5206\u7ea7\u5206\u7c7b\u201d\u7b49\uff09\u548c Sharp ETL\u7684\u5b9e\u73b0\u7b49\u7b49\u8fdb\u884c\u4ecb\u7ecd"))}u.isMDXComponent=!0}}]);