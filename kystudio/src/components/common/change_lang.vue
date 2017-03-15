<template>
<el-button v-if="lang=='en'" @click="changeLang">E</el-button>
<el-button v-else @click="changeLang">中</el-button>
</template>
<script>
  import router from '../../router'
  export default {
    name: 'changelang',
    watch: {
      lang (val) {
        if (localStorage.getItem('kystudio_lang') !== this.lang) {
          router.go('/')
        }
        localStorage.setItem('kystudio_lang', this.lang)
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

</style>
