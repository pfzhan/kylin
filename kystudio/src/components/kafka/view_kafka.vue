<template>
  <div>
    <el-form  label-position='right'   label-width=' 120px' ref='kafkaForm'>
        <span style='line-height: 36px;'>{{$t('cluster')}}</span>
        <el-table class='table_margin'
          :data='kafkaMeta.clusters&&kafkaMeta.clusters[0].brokers'
          style='width: 100%'>
          <el-table-column
            show-overflow-tooltip
            label='ID'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <span>{{scope.row.id}}</span>
            </template>
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('host')"
            prop='host'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <span>{{scope.row.host}}</span>
            </template>
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('port')"
            prop='port'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <span>{{scope.row.port}}</span>
            </template>
          </el-table-column>
        </el-table>

        <el-card >
          <div slot="header">
            <span >{{$t('parserSetting')}}</span>
          </div>
          <div style="padding:10px;">
            <el-form-item :label="$t('parserName')" prop="parserName">
                <el-input v-model="kafkaMeta.parserName" disabled></el-input>
            </el-form-item>
            <el-form-item :label="$t('timestampField')" prop="timestampField">
                <el-input v-model="kafkaMeta.timestampField" disabled></el-input>
            </el-form-item>
            <el-form-item :label="$t('parserProperties')" prop="parserProperties">
                <el-input v-model="kafkaMeta.parserProperties" disabled ></el-input>
            </el-form-item>
          </div>
      </el-card>
    </el-form>
    </div>
</template>
<script>
// import { mapActions } from 'vuex'
// import { handleSuccess } from '../../util/business'
export default {
  name: 'viewkafka',
  props: ['streamingData'],
  data () {
    return {
      // kafkaMeta: {
      //   clusters: []
      // }
    }
  },
  methods: {
  },
  created () {
    // this.kafkaMeta.clusters = this.streamingData && this.streamingData[0] || null
  },
  computed: {
    kafkaMeta () {
      return this.streamingData
    }
  },
  locales: {
    'en': {host: 'Host', port: 'Port', action: 'Action', cluster: 'Cluster', clusterInfo: 'Get Cluster Info', timestamp: 'timestamp', derivedTimeDimension: 'Derived Time Dimension', parserSetting: 'Parser Setting', parserName: 'Parser Name', timestampField: 'Timestamp Field', parserProperties: 'Optional Properties'},
    'zh-cn': {host: '主机', port: '端口号', action: '操作', cluster: '集群', clusterInfo: '获取该集群信息', timestamp: 'timestamp', derivedTimeDimension: '推导的时间维度', parserSetting: '解析器设置', parserName: '解析器名称', timestampField: '时间戳字段名称', parserProperties: '解析器属性'}
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
 .el-card{
  border:none;
 }
 .el-card .el-card__body {
  padding: 0px;
 }
</style>
