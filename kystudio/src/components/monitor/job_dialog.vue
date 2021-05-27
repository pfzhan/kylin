<template>
  <div>
    <el-alert show-icon :show-background="false" type="info" class="download-tips">
      <span slot="title">{{$t('outputTips')}}<el-button size="mini" nobg-text @click="downloadLogs">{{$t('download')}}</el-button><span v-if="$store.state.config.platform === 'iframe'">{{$t('or')}}<el-button type="primary" size="mini" text @click="goSystemLog">{{$t('gotoSystemLog')}}</el-button></span>{{$t('end')}}</span>
      <!-- 只在 KC 中使用 -->
    </el-alert>
    <!-- <el-alert show-icon :show-background="false" type="info" class="download-tips" v-else>
      <span slot="title">{{$t('outputTipsKC')}}</span>
    </el-alert> -->
    <el-input
    type="textarea"
    :rows="14"
    :readonly="true"
    v-model="stepDetail">
    </el-input>
    <form name="download" class="downloadLogs" :action="actionUrl" target="_blank" method="get">
      <input type="hidden" name="project" :value="targetProject"/>
    </form>
  </div>
</template>

<script>
import { apiUrl } from '../../config'
import { mapActions } from 'vuex'
import { postCloudUrlMessage } from '../../util/business'
export default {
  name: 'jobDialog',
  props: ['stepDetail', 'stepId', 'jobId', 'targetProject'],
  locales: {
    'en': {
      outputTips: 'The output log shows the first and last 100 lines by default. To view all the output, please click to ',
      download: 'download the log file',
      end: '.',
      outputTipsKC: 'The output log shows the first and last 100 lines by default. To view all the output, please download diagnosis package on Admin-Troubleshooting page.',
      gotoSystemLog: 'view all logs',
      or: ' or '
    },
    'zh-cn': {
      outputTips: '输出日志默认展示首尾各100行内容，如需查看所有内容，请点击',
      download: '下载全部日志',
      end: '。',
      outputTipsKC: '输出日志默认展示首尾各 100 行内容，如需查看所有内容，请进入管理 - 诊断运维页面下载诊断包。',
      gotoSystemLog: '查看全部日志',
      or: '或'
    }
  },
  data () {
    return {
      hasClickDownloadLogBtn: false,
      cloudService: localStorage.getItem('cloud_service') ? localStorage.getItem('cloud_service') : 'AWSChinaCloud'
    }
  },
  computed: {
    actionUrl () {
      return apiUrl + 'jobs/' + this.jobId + '/steps/' + this.stepId + '/log'
    }
  },
  methods: {
    ...mapActions({
      downloadLog: 'DOWNLOAD_LOGS'
    }),
    goSystemLog () {
      postCloudUrlMessage(this.$route, { name: 'systemLogs', query: { id: this.jobId, project: this.targetProject } })
    },
    downloadLogs () {
      let params = {
        jobId: this.jobId,
        stepId: this.stepId,
        project: this.targetProject
      }
      if (this.hasClickDownloadLogBtn) {
        return false
      }
      if (this.$store.state.config.platform === 'iframe') {
        try {
          this.hasClickDownloadLogBtn = true
          this.downloadLog(params).then(res => {
            this.hasClickDownloadLogBtn = false
            if (res && res.status === 200 && res.body) {
              let fileName = this.targetProject + '_' + this.stepId + '.log'
              let data = res.body
              const blob = new Blob([data], {type: 'application/json;charset=utf-8'})
              if (window.navigator.msSaveOrOpenBlob) {
                navigator.msSaveBlob(blob, fileName)
              } else {
                var link = document.createElement('a')
                link.href = window.URL.createObjectURL(blob)
                link.download = fileName
                link.click()
                window.URL.revokeObjectURL(link.href)
              }
            }
          })
        } catch (error) {
          this.hasClickDownloadLogBtn = false
        }
      } else {
        this.hasClickDownloadLogBtn = false
        this.$nextTick(() => {
          this.$el.querySelectorAll('.downloadLogs').length && this.$el.querySelectorAll('.downloadLogs')[0].submit()
        })
      }
    }
  }
}
</script>
<style lang="less">
.download-tips {
  padding-top: 0 !important;
  .el-alert__title {
    font-size: 14px;
  }
  .el-button {
    padding: 0;
    font-size: 14px;
    span {
      vertical-align: bottom;
    }
  }
  .el-alert__closebtn{
    top:3px;
  }
}
</style>
