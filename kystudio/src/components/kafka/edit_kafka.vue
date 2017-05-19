<template>
    <el-form :model='kafkaMeta' label-position='right' :rules='rules'  label-width=' 120px' ref='kafkaForm'>
        <span style='line-height: 36px;'>{{$t('cluster')}}</span>
        <el-table class='table_margin'
          :data='kafkaMeta.clusters[0].brokers' 
          style='width: 100%'>
          <el-table-column
            label='ID'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <el-input v-model='scope.row.id' v-if='currentCheck === scope.$index'></el-input>
              <span v-else>{{scope.row.id}}</span>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('host')"
            prop='host'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <el-input v-model='scope.row.host' v-if='currentCheck === scope.$index'></el-input>
              <span v-else>{{scope.row.host}}</span>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('port')"
            prop='port'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <el-input v-model='scope.row.port' v-if='currentCheck === scope.$index'></el-input>   
              <span v-else >{{scope.row.host}}</span>         
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('action')"
            header-align='center'
            align='center'
            width='110'>
            <template scope='scope'>
              <el-button size='mini' icon='check' @click='checkBroker(scope.$index)' v-if='currentCheck === scope.$index'></el-button>
              <el-button size='mini' icon='edit' @click='editBroker(scope.$index)'  v-else></el-button >
              <el-button size='mini' icon='delete' @click='removeBroker(scope.$index)'></el-button>
            </template>
          </el-table-column>
        </el-table>
        <el-row class='row_padding'>
          <el-col :span='24'>   
            <el-button size='mini' icon='plus' @click='addBroker'>Add Broker
            </el-button>
          </el-col>
        </el-row>
     <!--    <el-row class='row_padding'>
          <el-col :span='24'>      
            <el-button size='mini' icon='loading' @click='getClusterInfo'>{{$t('clusterInfo')}}
            </el-button>
          </el-col>
        </el-row>   -->
        <el-row class='row_padding'>
          <el-col :span='24'>     
            Topic: {{kafkaMeta.topic}} 
          </el-col>
        </el-row>         
        <el-card >
          <div slot="header">
            <span >{{$t('parserSetting')}}</span>
          </div>
          <div>
            <el-form-item :label="$t('parserName')" prop="parserName">
                <el-input v-model="kafkaMeta.parserName"></el-input>      
            </el-form-item>
            <el-form-item :label="$t('parserProperties')" prop="parserProperties">
                <el-input v-model="kafkaMeta.parserProperties" placeholder="configA=1;configB=2"></el-input>      
            </el-form-item>
          </div>
      </el-card>        
    </el-form>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess } from '../../util/business'
export default {
  name: 'createKafka',
  props: ['tableName'],
  data () {
    return {
      rules: {
        parserName: [
        { required: true, message: '', trigger: 'change' }
        ],
        parserProperties: [
        { required: true, message: '', trigger: 'change' }
        ]
      },
      currentCheck: 0,
      kafkaMeta: {
        clusters: [{}]
      },
      streamingConfig: null
    }
  },
  methods: {
    ...mapActions({
      getKafkaTableDetail: 'GET_KAFKA_CONFIG',
      getStreamingConfig: 'LOAD_STREAMING_CONFIG'
    }),
    addBroker: function () {
      // if (this.currentCheck === -1) {
      this.kafkaMeta.clusters[0].brokers.push({id: '', host: '', port: ''})
      this.currentCheck = this.kafkaMeta.clusters[0].brokers.length - 1
      // }
    },
    checkBroker: function (index) {
      this.currentCheck = -1
    },
    editBroker: function (index) {
      this.currentCheck = index
    },
    removeBroker: function (index) {
      if (this.currentCheck > index) {
        this.currentCheck --
      }
      this.kafkaMeta.clusters[0].brokers.splice(index, 1)
    }
  },
  created () {
    this.$on('kafkaEditFormValid', (t) => {
      this.$refs['kafkaForm'].validate((valid) => {
        if (valid) {
          this.$emit('validEditSuccess', {
            kafkaMeta: this.kafkaMeta,
            streamingMeta: this.streamingConfig
          })
        }
      })
    })
    this.getKafkaTableDetail(this.tableName).then((res) => {
      handleSuccess(res, (data) => {
        this.kafkaMeta = data && data[0]
      })
    })
    this.getStreamingConfig(this.tableName).then((res) => {
      handleSuccess(res, (data) => {
        this.streamingConfig = data && data[0]
      })
    })
  },
  locales: {
    'en': {host: 'Host', port: 'Port', action: 'Action', cluster: 'Cluster', clusterInfo: 'Get Cluster Info', timestamp: 'timestamp', derivedTimeDimension: 'Derived Time Dimension', parserSetting: 'Parser Setting', parserName: 'Parser Name', parserTimestampField: 'Parser Timestamp Field', parserProperties: 'ParserProperties'},
    'zh-cn': {host: '主机', port: '端口号', action: '操作', cluster: '集群', clusterInfo: '获取该集群信息', timestamp: 'timestamp', derivedTimeDimension: '推导的时间维度', parserSetting: '解析器设置', parserName: '解析器名称', parserTimestampField: '时间戳字段名称', parserProperties: '解析器属性'}
  }
}
</script>
<style scoped=''>
 .table_margin {
   margin-top: 10px;
   margin-bottom: 10px;
 }
  .row_padding {
  padding-top: 5px;
  padding-bottom: 5px;
 }
 .el-card .el-card__body {
  padding: 0px;
 }
</style>
