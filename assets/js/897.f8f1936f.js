"use strict";(self.webpackChunksharp_etl_site=self.webpackChunksharp_etl_site||[]).push([[897],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>d});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function c(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var i=r.createContext({}),s=function(e){var t=r.useContext(i),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},p=function(e){var t=s(e.components);return r.createElement(i.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,i=e.parentName,p=c(e,["components","mdxType","originalType","parentName"]),m=s(n),d=o,y=m["".concat(i,".").concat(d)]||m[d]||u[d]||a;return n?r.createElement(y,l(l({ref:t},p),{},{components:n})):r.createElement(y,l({ref:t},p))}));function d(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,l=new Array(a);l[0]=m;var c={};for(var i in t)hasOwnProperty.call(t,i)&&(c[i]=t[i]);c.originalType=e,c.mdxType="string"==typeof e?e:o,l[1]=c;for(var s=2;s<a;s++)l[s]=n[s];return r.createElement.apply(null,l)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},9649:(e,t,n)=>{n.d(t,{Z:()=>u});var r=n(7462),o=n(7294),a=n(6010),l=n(5999),c=n(650);const i="anchorWithStickyNavbar_mojV",s="anchorWithHideOnScrollNavbar_R0VQ";function p(e){let{as:t,id:n,...p}=e;const{navbar:{hideOnScroll:u}}=(0,c.LU)();return n?o.createElement(t,(0,r.Z)({},p,{className:(0,a.Z)("anchor",{[s]:u,[i]:!u}),id:n}),p.children,o.createElement("a",{className:"hash-link",href:"#"+n,title:(0,l.I)({id:"theme.common.headingLinkTitle",message:"Direct link to heading",description:"Title for link to heading"})},"\u200b")):o.createElement(t,p)}function u(e){let{as:t,...n}=e;return"h1"===t?o.createElement("h1",(0,r.Z)({},n,{id:void 0}),n.children):o.createElement(p,(0,r.Z)({as:t},n))}},4689:(e,t,n)=>{n.d(t,{Z:()=>S});var r=n(7462),o=n(7294),a=n(5742),l=n(9960),c=n(6010);const i={plain:{backgroundColor:"#2a2734",color:"#9a86fd"},styles:[{types:["comment","prolog","doctype","cdata","punctuation"],style:{color:"#6c6783"}},{types:["namespace"],style:{opacity:.7}},{types:["tag","operator","number"],style:{color:"#e09142"}},{types:["property","function"],style:{color:"#9a86fd"}},{types:["tag-id","selector","atrule-id"],style:{color:"#eeebff"}},{types:["attr-name"],style:{color:"#c4b9fe"}},{types:["boolean","string","entity","url","attr-value","keyword","control","directive","unit","statement","regex","atrule","placeholder","variable"],style:{color:"#ffcc99"}},{types:["deleted"],style:{textDecorationLine:"line-through"}},{types:["inserted"],style:{textDecorationLine:"underline"}},{types:["italic"],style:{fontStyle:"italic"}},{types:["important","bold"],style:{fontWeight:"bold"}},{types:["important"],style:{color:"#c4b9fe"}}]};var s={Prism:n(7410).default,theme:i};function p(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function u(){return u=Object.assign||function(e){for(var t=1;t<arguments.length;t++){var n=arguments[t];for(var r in n)Object.prototype.hasOwnProperty.call(n,r)&&(e[r]=n[r])}return e},u.apply(this,arguments)}var m=/\r\n|\r|\n/,d=function(e){0===e.length?e.push({types:["plain"],content:"\n",empty:!0}):1===e.length&&""===e[0].content&&(e[0].content="\n",e[0].empty=!0)},y=function(e,t){var n=e.length;return n>0&&e[n-1]===t?e:e.concat(t)},h=function(e,t){var n=e.plain,r=Object.create(null),o=e.styles.reduce((function(e,n){var r=n.languages,o=n.style;return r&&!r.includes(t)||n.types.forEach((function(t){var n=u({},e[t],o);e[t]=n})),e}),r);return o.root=n,o.plain=u({},n,{backgroundColor:null}),o};function g(e,t){var n={};for(var r in e)Object.prototype.hasOwnProperty.call(e,r)&&-1===t.indexOf(r)&&(n[r]=e[r]);return n}const f=function(e){function t(){for(var t=this,n=[],r=arguments.length;r--;)n[r]=arguments[r];e.apply(this,n),p(this,"getThemeDict",(function(e){if(void 0!==t.themeDict&&e.theme===t.prevTheme&&e.language===t.prevLanguage)return t.themeDict;t.prevTheme=e.theme,t.prevLanguage=e.language;var n=e.theme?h(e.theme,e.language):void 0;return t.themeDict=n})),p(this,"getLineProps",(function(e){var n=e.key,r=e.className,o=e.style,a=u({},g(e,["key","className","style","line"]),{className:"token-line",style:void 0,key:void 0}),l=t.getThemeDict(t.props);return void 0!==l&&(a.style=l.plain),void 0!==o&&(a.style=void 0!==a.style?u({},a.style,o):o),void 0!==n&&(a.key=n),r&&(a.className+=" "+r),a})),p(this,"getStyleForToken",(function(e){var n=e.types,r=e.empty,o=n.length,a=t.getThemeDict(t.props);if(void 0!==a){if(1===o&&"plain"===n[0])return r?{display:"inline-block"}:void 0;if(1===o&&!r)return a[n[0]];var l=r?{display:"inline-block"}:{},c=n.map((function(e){return a[e]}));return Object.assign.apply(Object,[l].concat(c))}})),p(this,"getTokenProps",(function(e){var n=e.key,r=e.className,o=e.style,a=e.token,l=u({},g(e,["key","className","style","token"]),{className:"token "+a.types.join(" "),children:a.content,style:t.getStyleForToken(a),key:void 0});return void 0!==o&&(l.style=void 0!==l.style?u({},l.style,o):o),void 0!==n&&(l.key=n),r&&(l.className+=" "+r),l})),p(this,"tokenize",(function(e,t,n,r){var o={code:t,grammar:n,language:r,tokens:[]};e.hooks.run("before-tokenize",o);var a=o.tokens=e.tokenize(o.code,o.grammar,o.language);return e.hooks.run("after-tokenize",o),a}))}return e&&(t.__proto__=e),t.prototype=Object.create(e&&e.prototype),t.prototype.constructor=t,t.prototype.render=function(){var e=this.props,t=e.Prism,n=e.language,r=e.code,o=e.children,a=this.getThemeDict(this.props),l=t.languages[n];return o({tokens:function(e){for(var t=[[]],n=[e],r=[0],o=[e.length],a=0,l=0,c=[],i=[c];l>-1;){for(;(a=r[l]++)<o[l];){var s=void 0,p=t[l],u=n[l][a];if("string"==typeof u?(p=l>0?p:["plain"],s=u):(p=y(p,u.type),u.alias&&(p=y(p,u.alias)),s=u.content),"string"==typeof s){var h=s.split(m),g=h.length;c.push({types:p,content:h[0]});for(var f=1;f<g;f++)d(c),i.push(c=[]),c.push({types:p,content:h[f]})}else l++,t.push(p),n.push(s),r.push(0),o.push(s.length)}l--,t.pop(),n.pop(),r.pop(),o.pop()}return d(c),i}(void 0!==l?this.tokenize(t,r,l,n):[r]),className:"prism-code language-"+n,style:void 0!==a?a.root:{},getLineProps:this.getLineProps,getTokenProps:this.getTokenProps})},t}(o.Component);var v=n(5999),b=n(650);const k="codeBlockContainer_I0IT",E="codeBlockContent_wNvx",O="codeBlockTitle_BvAR",Z="codeBlock_jd64",T="codeBlockStandalone_csWH",N="copyButton_wuS7",j="codeBlockLines_mRuA";function P(e){var t;let{children:n,className:a="",metastring:l,title:i,language:p}=e;const{prism:u}=(0,b.LU)(),[m,d]=(0,o.useState)(!1),[y,h]=(0,o.useState)(!1);(0,o.useEffect)((()=>{h(!0)}),[]);const g=(0,b.bc)(l)||i,P=(0,b.pJ)();if(o.Children.toArray(n).some((e=>(0,o.isValidElement)(e))))return o.createElement(f,(0,r.Z)({},s,{key:String(y),theme:P,code:"",language:"text"}),(e=>{let{className:t,style:r}=e;return o.createElement("pre",{tabIndex:0,className:(0,c.Z)(t,T,"thin-scrollbar",k,a,b.kM.common.codeBlock),style:r},o.createElement("code",{className:j},n))}));const C=Array.isArray(n)?n.join(""):n,x=null!=(t=null!=p?p:(0,b.Vo)(a))?t:u.defaultLanguage,{highlightLines:w,code:S}=(0,b.nZ)(C,l,x),_=()=>{!function(e,t){let{target:n=document.body}=void 0===t?{}:t;const r=document.createElement("textarea"),o=document.activeElement;r.value=e,r.setAttribute("readonly",""),r.style.contain="strict",r.style.position="absolute",r.style.left="-9999px",r.style.fontSize="12pt";const a=document.getSelection();let l=!1;a.rangeCount>0&&(l=a.getRangeAt(0)),n.append(r),r.select(),r.selectionStart=0,r.selectionEnd=e.length;let c=!1;try{c=document.execCommand("copy")}catch{}r.remove(),l&&(a.removeAllRanges(),a.addRange(l)),o&&o.focus()}(S),d(!0),setTimeout((()=>d(!1)),2e3)};return o.createElement(f,(0,r.Z)({},s,{key:String(y),theme:P,code:S,language:null!=x?x:"text"}),(e=>{let{className:t,style:n,tokens:l,getLineProps:i,getTokenProps:s}=e;return o.createElement("div",{className:(0,c.Z)(k,a,{["language-"+x]:x&&!a.includes("language-"+x)},b.kM.common.codeBlock)},g&&o.createElement("div",{style:n,className:O},g),o.createElement("div",{className:(0,c.Z)(E,x)},o.createElement("pre",{tabIndex:0,className:(0,c.Z)(t,Z,"thin-scrollbar"),style:n},o.createElement("code",{className:j},l.map(((e,t)=>{1===e.length&&"\n"===e[0].content&&(e[0].content="");const n=i({line:e,key:t});return w.includes(t)&&(n.className+=" docusaurus-highlight-code-line"),o.createElement("span",(0,r.Z)({key:t},n),e.map(((e,t)=>o.createElement("span",(0,r.Z)({key:t},s({token:e,key:t}))))),o.createElement("br",null))})))),o.createElement("button",{type:"button","aria-label":(0,v.I)({id:"theme.CodeBlock.copyButtonAriaLabel",message:"Copy code to clipboard",description:"The ARIA label for copy code blocks button"}),className:(0,c.Z)(N,"clean-btn"),onClick:_},m?o.createElement(v.Z,{id:"theme.CodeBlock.copied",description:"The copied button label on code blocks"},"Copied"):o.createElement(v.Z,{id:"theme.CodeBlock.copy",description:"The copy button label on code blocks"},"Copy"))))}))}var C=n(9649);const x="details_BAp3";function w(e){let{...t}=e;return o.createElement(b.PO,(0,r.Z)({},t,{className:(0,c.Z)("alert alert--info",x,t.className)}))}const S={head:e=>{const t=o.Children.map(e.children,(e=>function(e){var t,n;if(null!=e&&null!=(t=e.props)&&t.mdxType&&null!=e&&null!=(n=e.props)&&n.originalType){const{mdxType:t,originalType:n,...r}=e.props;return o.createElement(e.props.originalType,r)}return e}(e)));return o.createElement(a.Z,e,t)},code:e=>o.Children.toArray(e.children).every((e=>"string"==typeof e&&!e.includes("\n")))?o.createElement("code",e):o.createElement(P,e),a:e=>o.createElement(l.Z,e),pre:e=>{var t;return o.createElement(P,(0,o.isValidElement)(e.children)&&"code"===e.children.props.originalType?null==(t=e.children)?void 0:t.props:{...e})},details:e=>{const t=o.Children.toArray(e.children),n=t.find((e=>{var t;return"summary"===(null==e||null==(t=e.props)?void 0:t.mdxType)})),a=o.createElement(o.Fragment,null,t.filter((e=>e!==n)));return o.createElement(w,(0,r.Z)({},e,{summary:n}),a)},h1:e=>o.createElement(C.Z,(0,r.Z)({as:"h1"},e)),h2:e=>o.createElement(C.Z,(0,r.Z)({as:"h2"},e)),h3:e=>o.createElement(C.Z,(0,r.Z)({as:"h3"},e)),h4:e=>o.createElement(C.Z,(0,r.Z)({as:"h4"},e)),h5:e=>o.createElement(C.Z,(0,r.Z)({as:"h5"},e)),h6:e=>o.createElement(C.Z,(0,r.Z)({as:"h6"},e))}}}]);