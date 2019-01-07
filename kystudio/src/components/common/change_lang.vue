<template>
  <el-button-group class="change_lang">
    <el-button size="mini" @click="changeLang('en')" :class="{'active':lang=='en'}">EN</el-button>
    <el-button size="mini" @click="changeLang('zh-cn')" :class="{'active':lang=='zh-cn'}">中</el-button>
</el-button-group>
</template>
<script>
  import Vue from 'vue'
  import VueI18n from 'vue-i18n'
  import enLocale from 'kyligence-ui/lib/locale/lang/en'
  import zhLocale from 'kyligence-ui/lib/locale/lang/zh-CN'
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
      changeLang (val) {
        if (val === 'en') {
          this.lang = 'en'
          this.$store.state.system.lang = 'en'
          document.documentElement.lang = 'en-us'
        } else {
          this.lang = 'zh-cn'
          this.$store.state.system.lang = 'zh-cn'
          document.documentElement.lang = 'zh-cn'
        }
      }
    },
    created () {
      // 外链传参数改变语言的逻辑
      this.$on('changeLang', this.changeLang)
      // end
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
      this.$store.state.system.lang = this.lang
      document.documentElement.lang = this.lang === 'en' ? 'en-us' : this.lang
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
