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
        defaultLang: 'en',
        lang: localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : this.defaultLang,
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
    },
    created () {
      let currentLang = navigator.language
      // 判断除IE外其他浏览器使用语言
      if (!currentLang) {
      // 判断IE浏览器使用语言
        currentLang = navigator.browserLanguage
      }
      if (currentLang.indexOf('zh') >= 0) {
        this.defaultLang = 'zh-cn'
      }
      Vue.config.lang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : this.defaultLang
      this.lang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : this.defaultLang
      console.log(Vue.config.lang, navigator.language, this.lang)
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
