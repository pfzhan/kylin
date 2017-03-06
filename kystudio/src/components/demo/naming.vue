<template>
<div id="naming">
    <dl v-for="rule in rules">
       <dt>{{ rule.title }}</dt>
       <dd v-for="ali in rule.list" v-html="ali"></dd>
    </dl>
    </div>
</template>
<script>
export default {
  data () {
    return {
      rules: [
        {
          'title': '文件命名规则',
          'list': [
            '1.以.vue作为后缀的文件命名采用全部小写字母，允许两种命名形式，例如：<pre>users.vue user_list.vue user_add.vue</pre>',
            '2.以.js作为后缀的文件命名采用驼峰命名形式，例如：<pre>index.js userServer.js userServerAction.js</pre>'
          ]
        },
        {
          'title': '代码命名规则',
          'list': [
            '1.全局模块，引入时申明变量首字母均大写，例如：<pre>Vue Vuex VueI18n</pre>',
            '2.自定义模块，引入时申明变量采用驼峰命名形式'
          ]
        }
      ]
    }
  }
}
</script>

<style lang="less" >
  #naming{
     dt{
     	font-size:24px;
     }
     dl{
        margin-top:20px;
     }
     dd{
     	pre{
     	  padding:10px 10px;
     	  border:dashed 1px #ccc;
     	  margin:15px 5px;
        }
     }
     
  }
</style>
