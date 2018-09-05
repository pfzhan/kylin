<template>
    <div>
      <el-form> 
       <el-table class='table_margin formTable'
          :data='brokers'
          style='width: 100%' border>
          <el-table-column
            show-overflow-tooltip
            :label="$t('host')"
            prop='host'
            width='300'
            header-align='center'
            align='center'>
            <template slot-scope='scope'>
                <el-input v-model='scope.row.host' size="small" v-if='currentCheck === scope.$index'></el-input>
                <span v-else>{{scope.row.host}}</span>
            </template>
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('port')"
            prop='port'
            header-align='center'
            align='center'>
            <template slot-scope='scope'>
              <el-input v-model='scope.row.port' size="small" v-if='currentCheck === scope.$index'></el-input>
              <span v-else >{{scope.row.port}}</span>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('action')"
            header-align='center'
            align='center'
            width='120'>
            <template slot-scope='scope'>
              <el-button size='mini' icon='el-icon-check' @click='checkBroker(scope.$index, scope.row)' v-if='currentCheck === scope.$index'></el-button>
              <el-button size='mini' icon='el-icon-ksd-table_edit' @click='editBroker(scope.$index)'  v-else></el-button>
              <el-button size='mini' icon='el-icon-delete' @click='removeBroker(scope.$index)'></el-button>
            </template>
          </el-table-column>
        </el-table>
        <el-row class='row_padding'>
          <el-col :span='24'>
            <el-button  icon='el-icon-plus' @click='addBroker' type="primary" plain>Broker
            </el-button>
             <el-button  icon='el-icon-search' :loading="loading" type="primary" v-if="showGetTopicBtn && topicBtnVisible" @click='loadTopicInfo'>{{$t('clusterInfo')}}
            </el-button>
          </el-col>
        </el-row>
       </el-form>
    </div>
</template>
<script>
export default {
  name: 'editcluster',
  props: ['showGetTopicBtn', 'loading', 'hisbrokers'],
  data () {
    return {
      brokers: this.hisbrokers || [],
      currentCheck: -1,
      rules: {
        name: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' },
        {validator: this.checkName, trigger: 'blur'}
        ],
        parserName: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' }
        ],
        timestampField: [
        { required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change' }
        ]
      }
    }
  },
  methods: {
    loadTopicInfo () {
      this.$emit('loadTopicInfo')
    },
    initData () {
      this.brokers.splice(0, this.brokers.length)
      this.brokers = this.hisbrokers
      this.currentCheck = -1
    },
    addBroker: function () {
      if (this.currentCheck < 0) {
        this.brokers.push({host: '', port: ''})
        this.currentCheck = this.brokers.length - 1
      }
    },
    hasBrokenBroker (data) {
      data.host = data.host && data.host.replace(/^\s|\s$/g, '') || ''
      data.port = data.port && data.port.replace(/^\s|\s$/g, '') || ''
      var portIsInt = /^\d+$/.test(data.port)
      if (!data.host || !data.port || !portIsInt) {
        return true
      }
      return false
    },
    hasBrokenBrokers () {
      for (var i = 0; i < this.brokers.length; i++) {
        if (this.hasBrokenBroker(this.brokers[i])) {
          return true
        }
      }
      return false
    },
    checkBroker: function (index, data) {
      if (this.hasBrokenBroker(data)) {
        this.$message({message: this.$t('invalidInput'), type: 'error'})
        return
      }
      this.currentCheck = -1
      this.$emit('getAllClusters', this.brokers)
    },
    editBroker: function (index) {
      if (this.currentCheck !== -1) {
        return
      }
      this.currentCheck = index
    },
    removeBroker: function (index) {
      if (this.currentCheck === index) {
        this.currentCheck = -1
      }
      if (this.currentCheck > index) {
        this.currentCheck--
      }
      this.brokers.splice(index, 1)
      this.$emit('getAllClusters', this.brokers)
    }
  },
  computed: {
    topicBtnVisible () {
      return this.brokers.length > 0 && this.currentCheck === -1 && !this.hasBrokenBrokers()
    }
  },
  created () {
  },
  locales: {
    'en': {host: 'Host', port: 'Port', action: 'Action', cluster: 'Cluster', clusterInfo: 'Get Cluster Info', invalidInput: 'Invalid input'},
    'zh-cn': {host: '主机', port: '端口号', action: '操作', cluster: '集群', clusterInfo: '获取该集群信息', invalidInput: '无效输入'}
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .table_margin {
    margin-top: 10px;
    margin-bottom: 10px;
  }
  .row_padding {
   padding-top: 5px;
   padding-bottom: 5px;
  }
</style>
