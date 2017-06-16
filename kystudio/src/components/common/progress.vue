<template>  
  <div class="kap_progress" :class="progressClass">
     <el-progress :percentage="percent"  :show-text="showText"></el-progress><icon v-if="icon&&!showText" :name="icon"></icon>
  </div>
</template>
<script>
export default {
  name: 'total',
  props: ['percent', 'status'],
  methods: {
  },
  computed: {
    icon () {
      if (this.status === 'PENDING') {
        return ''
      } else if (this.status === 'RUNNING') {
        return ''
      } else if (this.status === 'FINISHED') {
        return 'check-circle'
      } else if (this.status === 'ERROR') {
        return 'times-circle'
      } else if (this.status === 'DISCARDED') {
        return ''
      } else if (this.status === 'STOPPED') {
        return 'pause-circle'
      }
    },
    progressClass () {
      if (this.status === 'PENDING') {
        return 'running'
      } else if (this.status === 'RUNNING') {
        return 'running'
      } else if (this.status === 'FINISHED') {
        return 'success'
      } else if (this.status === 'ERROR') {
        return 'error'
      } else if (this.status === 'DISCARDED') {
        return 'lose'
      } else if (this.status === 'STOPPED') {
        return 'running'
      }
    },
    showText () {
      if (this.status === 'PENDING') {
        return true
      } else if (this.status === 'RUNNING') {
        return true
      } else if (this.status === 'FINISHED') {
        return false
      } else if (this.status === 'ERROR') {
        return false
      } else if (this.status === 'DISCARDED') {
        return false
      } else if (this.status === 'STOPPED') {
        return false
      }
    }
  },
  created () {
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
 .kap_progress{
    .el-progress-bar {
      margin-right: -34px;
      padding-right:30px;
    }
    &.error{
      .fa-icon{
        color:red;
      }
      .el-progress-bar__inner{
        background-color:red;
      }
    }
    &.running{
      .el-progress-bar {
        padding-right:30px;
       }
       .el-progress__text{
        margin-left: 0;
       }
      .fa-icon{
        color:@base-color;
      }
      .el-progress-bar__inner{
        background-color:@base-color;
      }
    }
    &.success {
      .fa-icon{
        color:#13ce66;
      }
      .el-progress-bar__inner{
        background-color:#13ce66;
      }
    }
    &.lose{
      .fa-icon{
        color:#000;
      }
      .el-progress-bar__inner{
        background-color:#000;
      }  
    }
    position: relative;
    height: 40px;
    padding-right: 10px;
    padding-top: 16px;
    .fa-icon {
      position: absolute;
      right:20px;
      top:10px;
    }
 }
</style>
