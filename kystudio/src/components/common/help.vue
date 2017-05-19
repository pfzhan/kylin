<template>
<div class="help_box">
<el-dropdown @command="handleCommand">
  <span class="el-dropdown-link">
    Help <icon name="angle-down"></icon>
  </span>
  <el-dropdown-menu slot="dropdown">
    <el-dropdown-item command="kapmanual">KAP Manual</el-dropdown-item>
    <el-dropdown-item command="kybotservice">KyBot Service</el-dropdown-item>
    <el-dropdown-item command="aboutkap">About KAP</el-dropdown-item>
    <el-dropdown-item command="aboutus">About Us</el-dropdown-item>
  </el-dropdown-menu>
</el-dropdown>


<a :href="url" target="_blank"></a>
<el-dialog v-model="aboutKapVisible" title="关于KAP">
  <about_kap :about="serverAbout">
  </about_kap>
</el-dialog>
</div>
</template>
<script>
  import { mapActions } from 'vuex'

  import aboutKap from '../common/about_kap.vue'

  export default {
    name: 'help',
    data () {
      return {
        aboutKapVisible: false,
        url: ''
      }
    },
    methods: {
      ...mapActions({
        getAboutKap: 'GET_ABOUTKAP'
      }),
      handleCommand (val) {
        var _this = this
        if (val === 'kapmanual') {
          this.url = 'http://manual.kyligence.io/'
          this.$nextTick(function () {
            _this.$el.getElementsByTagName('a')[0].click()
          })
        } else if (val === 'kybotservice') {
          this.url = 'https://kybot.io/#/home'
          this.$nextTick(function () {
            _this.$el.getElementsByTagName('a')[0].click()
          })
        } else if (val === 'aboutkap') {
          // 发请求 kap/system/license
          this.getAboutKap().then((result) => {
          }, (resp) => {
            console.log(resp)
          })
          this.aboutKapVisible = true
        } else if (val === 'aboutus') {
          this.url = 'http://kyligence.io/'
          this.$nextTick(function () {
            _this.$el.getElementsByTagName('a')[0].click()
          })
        }
      }
    },
    computed: {
      serverAbout () {
        return this.$store.state.system.serverAboutKap
      }
    },
    components: {
      'about_kap': aboutKap
    },
    locales: {
      'en': {},
      'zh-cn': {}
    }
  }
</script>
<style lang="less">
.help_box{
  .el-dropdown{
	cursor: pointer;
	svg{
	  vertical-align: middle;
	}
  }	
  .el-dialog__header {height:70px;line-height:70px;padding:0 20px;text-align:left;}
  .el-dialog__title {color:#red;font-size:14px;}
  .el-dialog__body {padding:0 50px;}
}

</style>
