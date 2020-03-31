<template>
  <div class="user-no-authority">
    <div class="content">
      <template v-if="!tipType">
        <i class="el-icon-ksd-lock"></i>
        <p class="text"><span>{{$t('noAuthorityText', {time: jumpTimer})}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a></p>
      </template>
      <template v-else>
        <i class="el-icon-ksd-sad"></i>
        <p class="text"><span>{{$t('noAuthorityText1', {time: jumpTimer})}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a></p>
      </template>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
// import { mapActions, mapGetters } from 'vuex'

@Component({
  beforeRouteEnter (to, from, next) {
    let type = ''
    if (to.query && Object.keys(to.query).length && 'resouce' in to.query) {
      type = to.query.resouce
    }
    next(vm => {
      vm.tipType = type
    })
  },
  locales: {
    'en': {
      noAuthorityText: 'Sorry, you do not have permission to access this page, After {time} seconds, the system will jump to ',
      noAuthorityText1: 'Recommendation mode is not supported in the current project, please open the mode in Setting and try again. After {time} seconds, the system will automatically jump to ',
      dashboard: 'the dashboard page'
    },
    'zh-cn': {
      noAuthorityText: '抱歉，您无权访问该页面，{time} 秒后系统将跳转到',
      noAuthorityText1: '当前项目暂不支持模型推荐及优化，请在打开智能推荐开关后进行尝试。{time} 秒后系统将跳转到',
      dashboard: '仪表盘页面'
    }
  }
})
export default class NoAuthority extends Vue {
  jumpTimer = 5
  tipType = ''
  timer = null

  // 跳转至dashboard页面
  jumpToDashboard () {
    clearInterval(this.timer)
    this.$router.push('/dashboard')
  }

  mounted () {
    // 5秒后自动跳转至dashboard页面
    this.timer = setInterval(() => {
      this.jumpTimer -= 1
      if (this.jumpTimer === 0) {
        this.jumpToDashboard()
        clearInterval(this.timer)
      }
    }, 1000)
  }
}
</script>
<style lang="less" scoped>
  @import "../../assets/styles/variables.less";
  .user-no-authority {
    width: 100%;
    height: 100%;
    text-align: center;
    position: relative;
    .content {
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      .el-icon-ksd-lock {
        font-size: 50px;
        color: @text-placeholder-color;
      }
      .el-icon-ksd-sad {
        font-size: 50px;
      }
      .text {
        margin-top: 17px;
        font-size: 12px;
        color: @text-title-color;
      }
      .jump-address {
        color: @base-color;
      }
    }
  }
</style>
