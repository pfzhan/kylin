<template>
<div class="change_lang">
  <div v-if="isLogin">
    <el-button v-if="lang=='zh-cn'" style="background: #2b2d3c;color: #fff;" @click="changeLang">English</el-button>
    <el-button v-else style="background: #2b2d3c; color: #fff;" @click="changeLang">中文</el-button>
  </div>
  <div v-else>
    <el-button v-if="lang=='zh-cn'" @click="changeLang">English</el-button>
    <el-button v-else @click="changeLang">中文</el-button>
  </div>
  
</div>
</template>
<script>
  import Vue from 'vue'
  import VueI18n from 'vue-i18n'
  import enLocale from 'element-ui/lib/locale/lang/en'
  import zhLocale from 'element-ui/lib/locale/lang/zh-CN'
  import enKylinLocale from '../../locale/en'
  import zhKylinLocale from '../../locale/zh-CN'
  Vue.use(VueI18n)
  Vue.config.lang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'zh-cn'
  enLocale.kylinLang = enKylinLocale.default
  zhLocale.kylinLang = zhKylinLocale.default
  Vue.locale('en', enLocale)
  Vue.locale('zh-cn', zhLocale)
  export default {
    name: 'changelang',
    props: ['isLogin'],
    watch: {
      lang (val) {
        Vue.config.lang = val
        Vue.http.headers.common['Accept-Language'] = val === 'zh-cn' ? 'cn' : 'en'
        localStorage.setItem('kystudio_lang', val)
      }
    },
    data () {
      return {
        lang: localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : 'zh-cn',
        options: [{label: '中文', value: 'zh-cn'}, {label: 'English', value: 'en'}]
      }
    },
    methods: {
      changeLang () {
        if (this.lang === 'en') {
          this.lang = 'zh-cn'
        } else {
          this.lang = 'en'
        }
      }
    }
  }
</script>
<style lang="less">
  @import '../../less/config.less';
  .change_lang {
    button {
      background: transparent;
      color: @word-color;
    }
    .el-button{
      border:none;
      background: @bg-top;
    }
    .el-button:hover{
      color: @fff;
    }
  }
</style>
