"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[4757],{3905:(e,t,r)=>{r.d(t,{Zo:()=>c,kt:()=>m});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function p(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var s=n.createContext({}),l=function(e){var t=n.useContext(s),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},c=function(e){var t=l(e.components);return n.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},d=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,c=p(e,["components","mdxType","originalType","parentName"]),d=l(r),m=a,f=d["".concat(s,".").concat(m)]||d[m]||u[m]||o;return r?n.createElement(f,i(i({ref:t},c),{},{components:r})):n.createElement(f,i({ref:t},c))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=d;var p={};for(var s in t)hasOwnProperty.call(t,s)&&(p[s]=t[s]);p.originalType=e,p.mdxType="string"==typeof e?e:a,i[1]=p;for(var l=2;l<o;l++)i[l]=r[l];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}d.displayName="MDXCreateElement"},5587:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>u,frontMatter:()=>o,metadata:()=>p,toc:()=>l});var n=r(7462),a=(r(7294),r(3905));const o={title:"Docker setup",sidebar_position:5,toc:!0,last_modified_at:new Date("2021-11-03T22:25:57.000Z")},i=void 0,p={unversionedId:"docker-setup",id:"docker-setup",title:"Docker setup",description:"This guide provides quick docker setup for local testing",source:"@site/docs/docker-setup.md",sourceDirName:".",slug:"/docker-setup",permalink:"/SharpETL/docs/docker-setup",draft:!1,editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/docs/docker-setup.md",tags:[],version:"current",sidebarPosition:5,frontMatter:{title:"Docker setup",sidebar_position:5,toc:!0,last_modified_at:"2021-11-03T22:25:57.000Z"},sidebar:"docs",previous:{title:"Quick Start Guide",permalink:"/SharpETL/docs/quick-start-guide"},next:{title:"Developer Setup",permalink:"/SharpETL/docs/developer-setup"}},s={},l=[{value:"Requirments",id:"requirments",level:2},{value:"Setup step by step",id:"setup-step-by-step",level:2}],c={toc:l};function u(e){let{components:t,...r}=e;return(0,a.kt)("wrapper",(0,n.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("p",null,"This guide provides quick docker setup for local testing"),(0,a.kt)("h2",{id:"requirments"},"Requirments"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Docker"),(0,a.kt)("li",{parentName:"ul"},"Docker compose")),(0,a.kt)("h2",{id:"setup-step-by-step"},"Setup step by step"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"cd docker\ndocker compose up -d # to start ETL database(mysql 5.7) & hive instance(version 2.3.7)\n")),(0,a.kt)("p",null,"To access local hive instance you need"),(0,a.kt)("p",null,"in ",(0,a.kt)("inlineCode",{parentName:"p"},"spark/build.gradle")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-diff"},'-compileOnly(project(":datasource:hive3"))\n+compileOnly(project(":datasource:hive2"))\n+implementation "org.apache.spark:spark-hive_$scalaVersion:$sparkVersion"\n')),(0,a.kt)("p",null,"in ",(0,a.kt)("inlineCode",{parentName:"p"},"datasource/hive2/build.gradle")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-diff"},"+api 'org.apache.hive:hive-metastore:2.1.0'\n")),(0,a.kt)("p",null,"add ",(0,a.kt)("inlineCode",{parentName:"p"},"hive-site.xml")," in ",(0,a.kt)("inlineCode",{parentName:"p"},"spark/src/main/resources/hive-site.xml")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-xml"},'<configuration xmlns:xi="http://www.w3.org/2001/XInclude">\n    <property>\n        <name>hive.metastore.uris</name>\n        <value>thrift://localhost:9083</value>\n    </property>\n\n    <property>\n        <name>hive.metastore.warehouse.dir</name>\n        <value>file:///Users/$(whoami)/Documents/warehouse</value>\n    </property>\n\n    <property>\n        <name>hive.metastore.warehouse.external.dir</name>\n        <value>file:///Users/$(whoami)/Documents/warehouse</value>\n    </property>\n</configuration>\n')),(0,a.kt)("p",null,"add ",(0,a.kt)("inlineCode",{parentName:"p"},"core-site.xml")," in ",(0,a.kt)("inlineCode",{parentName:"p"},"spark/src/main/resources/core-site.xml")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-xml"},'<configuration xmlns:xi="http://www.w3.org/2001/XInclude">\n    <property>\n        <name>fs.defaultFS</name>\n        <value>file:///Users/$(whoami)/Documents/warehouse</value>\n        <final>true</final>\n    </property>\n</configuration>\n')))}u.isMDXComponent=!0}}]);