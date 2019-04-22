<template>
  <el-button-group class="change_lang">
    <el-button size="small" @click="changeLang('en')" :class="{'active':lang=='en'}">EN</el-button>
    <el-button size="small" @click="changeLang('zh-cn')" :class="{'active':lang=='zh-cn'}">中文</el-button>
</el-button-group>
</template>
<script>
  import Vue from 'vue'
  import VueI18n from 'vue-i18n'
  import enLocale from 'kyligence-ui/lib/locale/lang/en'
  import zhLocale from 'kyligence-ui/lib/locale/lang/zh-CN'
  import enKylinLocale from '../../locale/en'
  import zhKylinLocale from '../../locale/zh-CN'
  import { getQueryString } from 'util'
  Vue.use(VueI18n)
  enLocale.kylinLang = enKylinLocale.default
  zhLocale.kylinLang = zhKylinLocale.default
  Vue.locale('en', enLocale)
  Vue.locale('zh-cn', zhLocale)
  export default {
    name: 'changelang',
    data () {
      return {
        defaultLang: 'en',
        lang: localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : this.defaultLang
      }
    },
    methods: {
      changeLang (val) {
        if (val === 'en') {
          this.lang = 'en'
          this.$store.state.system.lang = 'en'
        } else {
          this.lang = 'zh-cn'
          this.$store.state.system.lang = 'zh-cn'
        }
        Vue.config.lang = this.lang
        Vue.http.headers.common['Accept-Language'] = val === 'zh-cn' ? 'cn' : 'en'
        localStorage.setItem('kystudio_lang', val)
        document.documentElement.lang = this.lang === 'en' ? 'en-us' : this.lang
      }
    },
    created () {
      // 外链传参数改变语言环境
      var lang = getQueryString('lang')
      if (lang) {
        this.changeLang(lang)
      } else {
        let currentLang = navigator.language
        // 判断除IE外其他浏览器使用语言
        if (!currentLang) {
        // 判断IE浏览器使用语言
          currentLang = navigator.browserLanguage
        }
        if (currentLang.indexOf('zh') >= 0) {
          this.defaultLang = 'zh-cn'
        }
        const finalLang = localStorage.getItem('kystudio_lang') ? localStorage.getItem('kystudio_lang') : this.defaultLang
        this.changeLang(finalLang)
      }
    }
  }
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .change_lang{
    height: 24px;
    .el-button {
      min-width: 36px;
      border-color: @text-disabled-color;
      &:first-child {
        border-top-left-radius: 2px;
        border-bottom-left-radius: 2px;
      }
      &:last-child {
        border-top-right-radius: 2px;
        border-bottom-right-radius: 2px;
      }
      &.active {
        background: @line-border-color;
        box-shadow: inset 1px 1px 2px 0 @grey-1;
      }
    }
  }
</style>
