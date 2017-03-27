<template>
<el-button v-if="lang!=='en'" @click="changeLang">EN</el-button>
<el-button v-else @click="changeLang">中</el-button>
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
  console.log(enLocale)
  Vue.locale('en', enLocale)
  Vue.locale('zh-cn', zhLocale)
  export default {
    name: 'changelang',
    watch: {
      lang (val) {
        Vue.config.lang = val
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
<style scoped="">
 .el-button{
  border:none;
 }
</style>
