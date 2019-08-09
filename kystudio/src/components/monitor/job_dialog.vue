<template>
  <div>
    <el-alert show-icon :show-background="false" type="info" class="download-tips">
      <span slot="title">{{$t('outputTips')}}<el-button type="primary" size="mini" text @click="downloadLogs">{{$t('download')}}</el-button>{{$t('end')}}</span>
    </el-alert>
    <el-input
    type="textarea"
    :rows="14"
    :readonly="true"
    v-model="stepDetail">
    </el-input>
    <form name="download" class="downloadLogs" :action="actionUrl" target="_blank" method="get">
      <input type="hidden" name="project" :value="currentSelectedProject"/>
    </form>
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import { apiUrl } from '../../config'
export default {
  name: 'jobDialog',
  props: ['stepDetail', 'stepId', 'jobId'],
  locales: {
    'en': {
      outputTips: 'The output log shows the first and last 100 lines by default. To view all the output, please click to ',
      download: 'download the log file.',
      end: '.'
    },
    'zh-cn': {
      outputTips: '输出日志默认展示首尾各100行内容，如需查看所有内容，请点击',
      download: '下载全部日志',
      end: '。'
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    actionUrl () {
      return apiUrl + 'jobs/' + this.jobId + '/steps/' + this.stepId + '/log'
    }
  },
  methods: {
    downloadLogs () {
      this.$nextTick(() => {
        this.$el.querySelectorAll('.downloadLogs')[0].submit()
      })
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
  }
}
</style>
