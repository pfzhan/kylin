<template>
   <div class="ksd-canary" v-if="$store.state.system.canaryReport && Object.keys($store.state.system.canaryReport).length">
    <i :class="poolStatus" class="el-icon-ksd-project_status"></i>
   </div>
</template>
<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
// for newten
// import { handleSuccess, handleError } from 'util/business'
export default {
  name: 'canary',
  props: ['perPageSize', 'totalSize', 'curPage', 'totalSum'],
  data () {
    return {
      ST: null,
      resLock: false,
      canaryReportData: null,
      lastCheckTime: 0,
      newTime: 0
    }
  },
  methods: {
    ...mapActions({
      getCanaryReport: 'GET_CANARY_REPORT'
    }),
    loadCanaryReport (cb) {
      if (this.resLock || this.$route.path === 'access/login') {
        return
      }
      this.resLock = true
      // for newten
      // this.getCanaryReport({
      //   projectName: this.currentSelectedProject,
      //   local: false
      // }).then((res) => {
      //   this.resLock = false
      //   handleSuccess(res, (data) => {
      //     this.canaryReportData = data
      //     cb && cb()
      //   })
      // }, (res) => {
      //   this.resLock = false
      //   handleError(res)
      // })
    },
    timerLoadCanary (stTime) {
      clearTimeout(this.ST)
      var timer = /\d+/.test(stTime) ? stTime : this.stInterval * 60000
      timer = timer > 8000 ? timer : 8000 // 加 8秒防止后台突然时间更新失败导致前端死循环
      this.ST = setTimeout(() => {
        this.loadCanaryReport(() => {
          this.triggerLoadCanary()
        })
      }, timer)
    },
    changeShow () {
      this.triggerLoadCanary()
    },
    triggerLoadCanary () {
      this.newTime = new Date().getTime()
      if (!this.lastCheckTime || this.lastCheckTime && this.updateTime > this.stInterval * 60000) {
        this.timerLoadCanary(0)
        return
      }
      this.timerLoadCanary(this.stInterval * 60000 - this.updateTime)
    },
    documentVisibeTrigger () {
      if (document.visibilityState === 'visible') {
        this.triggerLoadCanary()
      }
    }
  },
  mounted () {
    this.loadCanaryReport(() => {
      this.timerLoadCanary()
    })
    document.addEventListener('visibilitychange', this.documentVisibeTrigger)
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    poolStatus () {
      var countError = 0
      var countCrash = 0
      for (var nodeName in this.canaryReportData) {
        var cur = this.canaryReportData[nodeName]
        if (cur) {
          cur.forEach((list) => {
            if (list.status === 'ERROR') {
              countCrash++
            }
            if (list.status === 'WARNING') {
              countError++
            }
            if (list.status === 'CRASH') {
              countCrash++
            }
            if (this.lastCheckTime < list.lastCheckTime) {
              this.lastCheckTime = list.lastCheckTime
            }
          })
        }
      }
      if (countCrash > 0) {
        return 'crash'
      } else if (countError > 0) {
        return 'error'
      } else {
        return 'good'
      }
    },
    stInterval () {
      return +this.$store.state.system.canaryReloadTimer || 15
    },
    updateTime () {
      return this.newTime - this.lastCheckTime
    }
  },
  watch: {
    '$store.state.system.lang' (val) {
      Vue.http.headers.common['Accept-Language'] = val === 'zh-cn' ? 'cn' : 'en'
      this.loadCanaryReport()
    }
  },
  destroyed () {
    clearTimeout(this.ST)
    document.removeEventListener('visibilitychange', this.documentVisibeTrigger)
  },
  locales: {
    'en': {
    },
    'zh-cn': {
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .ksd-canary{
    >.good{
      color: @normal-color-1;
    }
    >.error{
      color: @error-color-1;
    }
    >.warning{
      color: @warning-color-1;
    }
  }
</style>
