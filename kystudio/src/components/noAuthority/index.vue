<template>
  <div class="user-no-authority">
    <div class="content">
      <template v-if="tipType === 'isNoAuthority'">
        <i class="el-icon-ksd-lock"></i>
        <template v-if="$lang === 'en'">
          <p class="text"><span>{{$t('noAuthorityText')}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a><span>{{$t('noAuthorityText1', {time: jumpTimer})}}</span></p>
        </template>
        <template v-else>
          <p class="text"><span>{{$t('noAuthorityText', {time: jumpTimer})}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a></p>
        </template>
      </template>
      <template v-else-if="tipType === 'isNotSemiAuto'">
        <i class="el-icon-ksd-sad"></i>
        <template v-if="$lang === 'en'">
          <p class="text"><span>{{$t('noModalAuthorityText1')}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a><span>{{$t('noModalAuthorityText2', {time: jumpTimer})}}</span></p>
        </template>
        <template v-else>
          <p class="text"><span>{{$t('noModalAuthorityText1', {time: jumpTimer})}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a></p>
        </template>
      </template>
      <template v-else>
        <i class="el-icon-ksd-sad"></i>
        <p class="text">
          <template v-if="$lang === 'en'">
            <span>{{$t('is404Tip')}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a><span>{{$t('is404Tip1', {time: jumpTimer})}}</span>
          </template>
          <template v-else>
            <span>{{$t('is404Tip', {time: jumpTimer})}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a>
          </template>
        </p>
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
      noAuthorityText: 'Sorry, you don`t have permission to access this page. Will automatically redirect to ',
      noAuthorityText1: ' in {time} seconds.',
      noModalAuthorityText1: 'Recommendation mode is not supported in the current project. Please turn on the mode in Setting and try again. Will automatically redirect to ',
      noModalAuthorityText2: ' in {time} seconds.',
      is404Tip: 'Sorry, the page doesn‘t exist. Will automatically redirect to ',
      is404Tip1: ' in {time} seconds.',
      dashboard: 'Dashboard'
    },
    'zh-cn': {
      noAuthorityText: '抱歉，您无权访问该页面。{time} 秒后系统将跳转到',
      noModalAuthorityText1: '当前项目未开启模型推荐及优化。请在设置中开启智能推荐后再试。{time} 秒后系统将跳转到',
      is404Tip: '抱歉，您访问的页面不存在。{time} 秒后将自动跳转至',
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
        clearInterval(this.timer)
        if (this.$route.name !== 'noAuthority' && this.$route.name !== '404') return
        this.jumpToDashboard()
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
