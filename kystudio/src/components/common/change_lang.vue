<template>
   <el-select  placeholder="" v-model="lang">
    <el-option
      v-for="item in options"
      :label="item.label"
      :value="item.value"
      >
    </el-option>
  </el-select>
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
    }
  }
</script>
<style scoped="">

</style>
