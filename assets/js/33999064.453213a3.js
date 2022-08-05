"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[5815],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>m});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function l(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?l(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):l(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function i(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},l=Object.keys(e);for(n=0;n<l.length;n++)r=l[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(n=0;n<l.length;n++)r=l[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var p=n.createContext({}),c=function(e){var t=n.useContext(p),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},u=function(e){var t=c(e.components);return n.createElement(p.Provider,{value:t},e.children)},s={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},f=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,l=e.originalType,p=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),f=c(r),m=a,w=f["".concat(p,".").concat(m)]||f[m]||s[m]||l;return r?n.createElement(w,o(o({ref:t},u),{},{components:r})):n.createElement(w,o({ref:t},u))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var l=r.length,o=new Array(l);o[0]=f;var i={};for(var p in t)hasOwnProperty.call(t,p)&&(i[p]=t[p]);i.originalType=e,i.mdxType="string"==typeof e?e:a,o[1]=i;for(var c=2;c<l;c++)o[c]=r[c];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}f.displayName="MDXCreateElement"},6736:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>p,contentTitle:()=>o,default:()=>s,frontMatter:()=>l,metadata:()=>i,toc:()=>c});var n=r(7462),a=(r(7294),r(3905));const l={slug:"sharp-etl-introduce-04-workflow-in-a-glance",title:"Sharp ETL\u4ecb\u7ecd(\u56db):Workflow\u5165\u95e8",tags:["sharp etl","workflow"],date:new Date("2022-08-03T16:00:00.000Z")},o=void 0,i={permalink:"/SharpETL/blog/sharp-etl-introduce-04-workflow-in-a-glance",editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/blog/sharp-etl-introduce-04-workflow-in-a-glance.md",source:"@site/blog/sharp-etl-introduce-04-workflow-in-a-glance.md",title:"Sharp ETL\u4ecb\u7ecd(\u56db):Workflow\u5165\u95e8",description:"\u5bfc\u8a00",date:"2022-08-03T16:00:00.000Z",formattedDate:"August 3, 2022",tags:[{label:"sharp etl",permalink:"/SharpETL/blog/tags/sharp-etl"},{label:"workflow",permalink:"/SharpETL/blog/tags/workflow"}],readingTime:3.47,hasTruncateMarker:!0,authors:[],frontMatter:{slug:"sharp-etl-introduce-04-workflow-in-a-glance",title:"Sharp ETL\u4ecb\u7ecd(\u56db):Workflow\u5165\u95e8",tags:["sharp etl","workflow"],date:"2022-08-03T16:00:00.000Z"},nextItem:{title:"Sharp ETL\u4ecb\u7ecd(\u4e09):\u4ec0\u4e48\u662f\u65e5\u5fd7\u9a71\u52a8",permalink:"/SharpETL/blog/sharp-etl-introduce-03-what-is-log-driven"}},p={authorsImageUrls:[]},c=[{value:"\u5bfc\u8a00",id:"\u5bfc\u8a00",level:2}],u={toc:c};function s(e){let{components:t,...r}=e;return(0,a.kt)("wrapper",(0,n.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"\u5bfc\u8a00"},"\u5bfc\u8a00"),(0,a.kt)("p",null,"\u672c\u6587\u5c06\u5feb\u901f\u8bb2\u89e3workflow\u7684\u57fa\u672c\u7528\u6cd5\uff0c\u5305\u62ec"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"\u53d8\u91cf"),(0,a.kt)("li",{parentName:"ul"},"\u4e34\u65f6\u8868"),(0,a.kt)("li",{parentName:"ul"},"\u63a7\u5236\u6d41 workflow_spec.sql"),(0,a.kt)("li",{parentName:"ul"},"step\u8bfb\u5199\u6570\u636e"),(0,a.kt)("li",{parentName:"ul"},"\u6570\u636e\u6e90"),(0,a.kt)("li",{parentName:"ul"},"\u6269\u5c55"),(0,a.kt)("li",{parentName:"ul"},"UDF"),(0,a.kt)("li",{parentName:"ul"},"Transformer"),(0,a.kt)("li",{parentName:"ul"},"Dynamic transformer")))}s.isMDXComponent=!0}}]);