<template>
  <div>
    <el-form :model='kafkaMeta' label-position='right'   label-width=' 120px' ref='kafkaForm'>
        <span style='line-height: 36px;'>{{$t('cluster')}}</span>
        <el-table class='table_margin'
          :data='kafkaMeta.clusters[0].brokers' 
          style='width: 100%'>
          <el-table-column
            label='ID'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <span>{{scope.row.id}}</span>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('host')"
            prop='host'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <span>{{scope.row.host}}</span>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('port')"
            prop='port'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <span>{{scope.row.host}}</span>         
            </template>
          </el-table-column>
        </el-table>
               
        <el-card >
          <div slot="header">
            <span >{{$t('parserSetting')}}</span>
          </div>
          <div>
            <el-form-item :label="$t('parserName')" prop="parserName">
                <el-input v-model="kafkaMeta.parserName" disabled></el-input>      
            </el-form-item>
            <el-form-item :label="$t('parserProperties')" prop="parserProperties">
                <el-input v-model="kafkaMeta.parserProperties" disabled placeholder="configA=1;configB=2"></el-input>      
            </el-form-item>
          </div>
      </el-card>        
    </el-form>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess } from '../../util/business'
export default {
  name: 'createKafka',
  prop: ['tableName'],
  data () {
    return {
      kafkaMeta: {
        clusters: []
      }
    }
  },
  methods: {
    ...mapActions({
      getKafkaTableDetail: 'GET_KAFKA_CONFIG'
    })
  },
  created () {
    this.getKafkaTableDetail(this.tableName).then((res) => {
      handleSuccess(res, (data) => {
        console.log(data, 'sssss')
        this.kafkaMeta = data && data[0]
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
