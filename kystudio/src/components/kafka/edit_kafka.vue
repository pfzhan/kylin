<template>
    <el-form :model='kafkaMeta' label-position='top' :rules='rules'  label-width='120px' ref='kafkaForm'>
        <span style='line-height: 36px;'>{{$t('kylinLang.dataSource.kafkaCluster')}}</span>
        <edit-cluster ref="clustercompoment" @getAllClusters="getClusters" :hisbrokers="kafkaMeta.clusters[0].brokers"></edit-cluster>
        
        <p class="ksd-mb-14 ksd-mt-20">{{$t('kylinLang.dataSource.topic')}} {{kafkaMeta.topic}}</p>
        <!-- <el-card >
          <div slot="header"> -->
          <h4 class="ksd-mt-40">{{$t('parserSetting')}}</h4>
          <!-- </div> -->
          <div style="padding:10px;">
            <el-form-item :label="$t('parserName')" prop="parserName" label-width='140px'>
                <el-input v-model="kafkaMeta.parserName"></el-input>
            </el-form-item>
            <el-form-item :label="$t('timestampField')" prop="timestampField" label-width='140px'>
                <el-input v-model="kafkaMeta.timestampField"></el-input>
            </el-form-item>
            <el-form-item :label="$t('parserProperties')" prop="parserProperties" label-width='140px'>
                <el-input v-model="kafkaMeta.parserProperties"></el-input>
            </el-form-item>
          </div>
      </el-card>
    </el-form>
</template>
<script>
import editCluster from './edit_cluster.vue'
import { objectClone } from 'util/index'
export default {
  name: 'editKafka',
  props: ['tableName', 'streamingData', 'streamingConfig', 'show'],
  watch: {
    'show' (v) {
      if (v) {
        this.$refs.clustercompoment.initData()
      }
    }
  },
  data () {
    return {
      rules: {
        parserName: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' }
        ],
        timestampField: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' }
        ]
      },
      currentCheck: -1
    }
  },
  components: {
    'edit-cluster': editCluster
  },
  methods: {
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
    },
    getClusters (data) {
      this.kafkaMeta.clusters[0].brokers = data
    }
  },
  computed: {
    kafkaMeta () {
      return objectClone(this.streamingData)
    }
  },
  created () {
    this.$on('kafkaEditFormValid', (t) => {
      this.$refs['kafkaForm'].validate((valid) => {
        if (!this.kafkaMeta.clusters[0].brokers.length) {
          this.$message({message: this.$t('noKafkaCluster'), type: 'error'})
          return
        }
        if (valid) {
          this.$emit('validEditSuccess', {
            kafkaMeta: this.kafkaMeta,
            streamingMeta: this.streamingConfig
          })
        }
      })
    })
    // this.getKafkaTableDetail(this.tableName).then((res) => {
    //   handleSuccess(res, (data) => {
    //     this.kafkaMeta = data && data[0]
    //   })
    // })
    // this.getStreamingConfig(this.tableName).then((res) => {
    //   handleSuccess(res, (data) => {
    //     this.streamingConfig = data && data[0]
    //   })
    // })
  },
  locales: {
    'en': {host: 'Host', port: 'Port', action: 'Action', cluster: 'Cluster', clusterInfo: 'Get Cluster Info', timestamp: 'timestamp', derivedTimeDimension: 'Derived Time Dimension', parserSetting: 'Parser Setting', parserName: 'Parser Name', timestampField: 'Timestamp Field', parserProperties: 'Optional Properties', noKafkaCluster: 'No Kafka cluster info'},
    'zh-cn': {host: '主机', port: '端口号', action: '操作', cluster: '集群', clusterInfo: '获取该集群信息', timestamp: 'timestamp', derivedTimeDimension: '推导的时间维度', parserSetting: '解析器设置', parserName: '解析器名称', timestampField: '时间戳字段名称', parserProperties: '解析器属性', noKafkaCluster: 'kafka集群信息不能为空'}
  }
}
</script>
<style scoped=''>
/* .table_margin {
   margin-top: 10px;
   margin-bottom: 10px;
 }
  .row_padding {
  padding-top: 5px;
  padding-bottom: 5px;
 }
 .el-card{
   border:none;
   padding:10px;
}
 .el-card .el-card__body {
  padding: 0px;
 }*/
</style>
