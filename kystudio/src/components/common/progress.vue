<template>  
  <div class="kap_progress" :class="progressClass">
     <el-progress :percentage="percent" :text-inside="true" :stroke-width="14"></el-progress>
     <i v-if="icon" :class="icon"></i>
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
        return 'el-icon-ksd-good_health'
      } else if (this.status === 'ERROR') {
        return 'el-icon-ksd-error_01'
      } else if (this.status === 'DISCARDED') {
        return 'el-icon-ksd-error_01'
      } else if (this.status === 'STOPPED') {
        return 'el-icon-warning'
      }
    },
    progressClass () {
      if (this.status === 'PENDING') {
        return 'pending'
      } else if (this.status === 'RUNNING') {
        return 'running'
      } else if (this.status === 'FINISHED') {
        return 'success'
      } else if (this.status === 'ERROR') {
        return 'error'
      } else if (this.status === 'DISCARDED') {
        return 'lose'
      } else if (this.status === 'STOPPED') {
        return 'pause'
      }
    }
  },
  created () {
  }
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
  .kap_progress{
    position: relative;
    .el-progress {
      display: inline-block;
      width: 90%;
    }
    i {
      position: absolute;
      right: 0;
      top: 5px;
      cursor: default !important;
    }
    &.success {
      .el-progress-bar__inner {
        background-color: @color-success;
      }
      i {
        color: @color-success;
      }
    }
    &.error {
      .el-progress-bar__inner {
        background-color: @color-danger;
      }
      i {
        color: @color-danger;
      }
    }
    &.lose {
      .el-progress-bar__inner {
        background-color: @color-info;
      }
      i {
        color: @color-info;
      }
    }
    &.pause {
      i {
        color: @color-primary;
      }
    }
  }
</style>
