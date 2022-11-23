"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[5395],{3905:(e,t,n)=>{n.d(t,{Zo:()=>s,kt:()=>m});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var p=r.createContext({}),c=function(e){var t=r.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},s=function(e){var t=c(e.components);return r.createElement(p.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},u=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,p=e.parentName,s=i(e,["components","mdxType","originalType","parentName"]),u=c(n),m=o,f=u["".concat(p,".").concat(m)]||u[m]||d[m]||a;return n?r.createElement(f,l(l({ref:t},s),{},{components:n})):r.createElement(f,l({ref:t},s))}));function m(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,l=new Array(a);l[0]=u;var i={};for(var p in t)hasOwnProperty.call(t,p)&&(i[p]=t[p]);i.originalType=e,i.mdxType="string"==typeof e?e:o,l[1]=i;for(var c=2;c<a;c++)l[c]=n[c];return r.createElement.apply(null,l)}return r.createElement.apply(null,n)}u.displayName="MDXCreateElement"},7514:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>p,contentTitle:()=>l,default:()=>d,frontMatter:()=>a,metadata:()=>i,toc:()=>c});var r=n(7462),o=(n(7294),n(3905));const a={title:"ODS config template",sidebar_position:3,toc:!0,last_modified_at:new Date("2022-11-23T21:59:57.000Z")},l=void 0,i={unversionedId:"ods-config-template",id:"ods-config-template",title:"ODS config template",description:"\u672c\u7247\u6587\u6863\u4e3b\u8981\u4ecb\u7ecdODS\u914d\u7f6e\u6a21\u677f\u7684\u53c2\u6570\u548c\u4f7f\u7528\u65b9\u5f0f\u3002",source:"@site/docs/ods-config-template.md",sourceDirName:".",slug:"/ods-config-template",permalink:"/SharpETL/docs/ods-config-template",draft:!1,editUrl:"https://github.com/SharpData/SharpETL/tree/pages/website/docs/ods-config-template.md",tags:[],version:"current",sidebarPosition:3,frontMatter:{title:"ODS config template",sidebar_position:3,toc:!0,last_modified_at:"2022-11-23T21:59:57.000Z"},sidebar:"docs",previous:{title:"Datasource",permalink:"/SharpETL/docs/datasource"},next:{title:"Excel template for source to ods",permalink:"/SharpETL/docs/excel-template-ods"}},p={},c=[{value:"\u6570\u636e\u6e90\u914d\u7f6e\uff1aods_etl_config",id:"\u6570\u636e\u6e90\u914d\u7f6eods_etl_config",level:2},{value:"\u8868\u914d\u7f6e\uff1aods_config",id:"\u8868\u914d\u7f6eods_config",level:2}],s={toc:c};function d(e){let{components:t,...n}=e;return(0,o.kt)("wrapper",(0,r.Z)({},s,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"\u672c\u7247\u6587\u6863\u4e3b\u8981\u4ecb\u7ecdODS\u914d\u7f6e\u6a21\u677f\u7684\u53c2\u6570\u548c\u4f7f\u7528\u65b9\u5f0f\u3002"),(0,o.kt)("p",null,"\u914d\u7f6e\u6a21\u677fexample\u53ef\u4ee5\u53c2\u8003quick start\u7684",(0,o.kt)("a",{parentName:"p",href:"https://docs.google.com/spreadsheets/d/1eRgSHWKDaRufvPJLp9QhcnWiVKzRegQ6PeZocvAgHEo/edit#gid=0"},"\u914d\u7f6e\u6587\u4ef6"),"\u3002"),(0,o.kt)("h2",{id:"\u6570\u636e\u6e90\u914d\u7f6eods_etl_config"},"\u6570\u636e\u6e90\u914d\u7f6e\uff1aods_etl_config"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"source_connection"),": \u914d\u7f6e\u5728application.properties\u4e2d\u7684connection"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"source_table"),": \u4ece\u54ea\u5f20\u8868\u83b7\u53d6\u6570\u636e"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"source_db"),": \u4ece\u54ea\u4e2a\u6570\u636e\u5e93\u83b7\u53d6\u6570\u636e"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"source_type"),": \u6570\u636e\u5e93\u7c7b\u578b\uff0c\u4f8b\u5982\uff1amysql"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"target_connection"),": \u76ee\u6807\u8fde\u63a5\uff0c\u914d\u7f6e\u5728application.properties\u4e2d\u7684connection\u3002\u4f8b\u5982\uff1ahive"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"target_table"),": ods\u8868\u540d"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"target_db"),": ods\u6570\u636e\u5e93\u5e93\u540d"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"target_type"),": \u76ee\u6807\u6570\u636e\u5e93\u7c7b\u578b\uff0c\u4f8b\u5982\uff1ahive"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"row_filter_expression"),": \u662f\u5426\u53ef\u7a7a\uff1a\u662f\u3002\u4f8b\u5982\uff1alocation = 'shanghai'\uff0c\u8868\u793a\u53ea\u53d6\u4e0a\u6d77\u5730\u533a\u7684\u6570\u636e\u3002\u4f1a\u4f5c\u4e3awhere\u8868\u8fbe\u5f0f\u62fc\u63a5\u5728\u67e5\u8be2\u6e90\u6570\u636e\u8868\u7684sql\u4e2d"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"load_type"),": \u589e\u91cf\u5168\u91cf\uff0c\u53ef\u9009\u503c\uff1aincremental\uff0cfull"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"log_driven_type"),": \u65e5\u5fd7\u9a71\u52a8\u7c7b\u578b\uff0c\u53ef\u9009\u503c\uff1atimewindow/upstream/kafka_offset/auto_inc_id/diff"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"upstream"),": \u4f9d\u8d56\u4e8e\u54ea\u4e00\u4e2a\u4e0a\u6e38\u4efb\u52a1\uff0c\u5bf9\u4e8eods\u4efb\u52a1\u800c\u8a00\uff0c\u4e00\u822c\u4e3a\u7a7a"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"depends_on"),": \u4f9d\u8d56\u4e8e\u54ea\u4e00\u4e2a\u4efb\u52a1\uff0c\u5bf9\u4e8eods\u4efb\u52a1\u800c\u8a00\uff0c\u4e00\u822c\u4e3a\u7a7a"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"default_start"),": timewindow\u6a21\u5f0f\u4e0b\u7684\u5f00\u59cb\u65f6\u95f4"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"partition_format"),": \u5206\u533a\u683c\u5f0f\uff0c\u53ef\u9009\u503c\uff1a\u7a7a\u5b57\u7b26\u4e32\u6216\u8005year/month/day"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"time_format"),": \u65f6\u95f4\u683c\u5f0f\uff0c\u9ed8\u8ba4\u503c\uff1aYYYY-MM-DD hh:mm:ss"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"period"),": \u591a\u5c11\u5206\u949f\u8fd0\u884c\u4e00\u6b21\u4efb\u52a1\uff0c\u5bf9\u4e8edaily\u7684\u4efb\u52a1\u5e94\u4e3a1440"),(0,o.kt)("h2",{id:"\u8868\u914d\u7f6eods_config"},"\u8868\u914d\u7f6e\uff1aods_config"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"source_table"),": \u4ece\u54ea\u5f20\u8868\u83b7\u53d6\u6570\u636e"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"source_column"),": \u6e90\u8868\u5217\u540d\u79f0"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"column_type"),": \u6e90\u8868\u5217\u7c7b\u578b"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"column_description"),": \u6e90\u8868\u5217\u63cf\u8ff0"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"is_PK"),": \u6e90\u8868\u662f\u5426\u4e3b\u952e"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"is_nullable"),": \u6e90\u8868\u662f\u5426\u53ef\u7a7a"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"incremental_column"),": \u589e\u91cf\u5217\uff0c\u4e00\u822c\u4e3a\u4e1a\u52a1\u65f6\u95f4\u5b57\u6bb5"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"target_table"),": \u76ee\u6807\u8868\u540d"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"target_column"),": \u76ee\u6807\u8868\u5217"),(0,o.kt)("p",null,(0,o.kt)("inlineCode",{parentName:"p"},"extra_column_expression"),": \u6269\u5c55\u5217\u8868\u8fbe\u5f0f\uff0c\u53ef\u4ee5\u5728\u6e90\u8868\u591a\u4e2a\u5217\u7684\u57fa\u7840\u4e0a\u505asql\u8868\u8fbe\u5f0f\u8ba1\u7b97\uff0c\u4f8b\u5982 md5(concat_ws('', user_name, .. , user_address))\uff0c\u7ed3\u679c\u4f5c\u4e3a\u65b0\u5217\u63d2\u5165\u76ee\u6807\u5217\uff0c\u8fd9\u65f6\u5bf9\u5e94\u7684\u6e90\u5217\u4e3a\u7a7a\u503c\u3002"))}d.isMDXComponent=!0}}]);