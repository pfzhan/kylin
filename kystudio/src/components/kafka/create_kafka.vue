<template>
  <div id="create-kafka">
    <el-form :model='kafkaMeta' label-position='right' :rules='rules'  label-width='180px' ref='kafkaForm'>
        <span style='line-height: 36px;'>{{$t('cluster')}}</span>
        <el-table class='table_margin formTable'
          :data='kafkaMeta.clusters[0].brokers'
          style='width: 100%'>
          <el-table-column
            show-overflow-tooltip
            label='ID'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <el-input v-model='scope.row.id' v-if='currentCheck === scope.$index'></el-input>
              <span v-else>{{scope.row.id}}</span>
            </template>
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
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
            show-overflow-tooltip
            :label="$t('port')"
            prop='port'
            header-align='center'
            align='center'>
            <template scope='scope'>
              <el-input v-model='scope.row.port' v-if='currentCheck === scope.$index'></el-input>
              <span v-else >{{scope.row.port}}</span>
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
        <el-row class='row_padding'>
          <el-col :span='24'>
            <el-button  icon='search' :loading="loading" type="primary" v-if="topicBtnDisabled" @click='getClusterInfo'>{{$t('clusterInfo')}}
            </el-button>
          </el-col>
        </el-row>
        <el-row class='row_padding ' style="height:200px;overflow:hidden" v-show="showTopicBox">
          <el-col :span='10' >
            <el-tree :data="treeData" :props="treeProps" class='textarea_height'
             @node-click="getTopicInfo">
            </el-tree>
          </el-col>
          <el-col :span='14'>
            <editor v-model="sourceSchema" ref="jsonDataBox" lang="json" theme="chrome" width="100%" height="200" useWrapMode="true"></editor>
          </el-col>
          <div class='convertBtn' @click='streamingOnChange();loadColumnZH();'>      <p>Convert</p>
            <i class='el-icon-arrow-down' aria-hidden='true'></i>
          </div>
        </el-row>
        <el-card v-show="showConvertBox" style="border-color:#20222e" >
          <el-row slot='header'>
            <el-col :span='6'>
              <p style="height: 36px;line-height: 36px">{{$t('tableName')}}:</p>
            </el-col>
            <el-col :span='8'>
              <el-select v-model="database" style="width:100%">
                <el-option v-for="(item, index) in databaseOption" :key="index"
                :label="item"
                :value="item">
                </el-option>
              </el-select>

            </el-col>
            <el-col :span='1' class="ksd-center ksd-lineheight-40"><b>／</b></el-col>
            <el-col :span='9' prop="name">
              <el-input  v-model="kafkaMeta.name" ></el-input>
            </el-col>
          </el-row>
          <div>
          <div style="height:400px;overflow:auto;">
          <el-table
          :data='columnList'
          style='width: 100%' class="formTable">
            <el-table-column
            label="ID"
            width="55">
              <template scope="scope">
                <el-checkbox v-model="scope.row.checked" true-label="Y" false-label="N"></el-checkbox>
              </template>
            </el-table-column>
            <el-table-column
            :label="$t('column')"
            property="name">
            </el-table-column>
            <el-table-column
            :label="$t('columnType')">
              <template scope="scope">
                <el-select v-model="scope.row.type" @change="loadColumnZH()">
                  <el-option
                    v-for="(item, index) in dataTypes"
                    :key="index"
                    :label="item"
                    :value="item"
                    >
                  </el-option>
                </el-select>
              </template>
            </el-table-column>
            <el-table-column
            :label="$t('comment')">
              <template scope="scope">
                <el-tag v-if="scope.row.type=='timestamp' && scope.row.fromSource=='Y'">{{$t('timestamp')}}</el-tag>
                <el-tag v-if="scope.row.fromSource=='N'" >{{$t('derivedTimeDimension')}}</el-tag>
              </template>
            </el-table-column>
          </el-table>
          </div>
          </div>
        </el-card>
        <el-card v-show="showConvertBox" style="border:none">
          <div slot="header">
            <span >{{$t('parserSetting')}}</span>
          </div>
          <div>
            <el-form-item :label="$t('parserName')" prop="parserName">
                <el-input v-model="kafkaMeta.parserName"></el-input>
            </el-form-item>
            <el-form-item :label="$t('timestampField')" prop="timestampField">
              <el-select v-model="kafkaMeta.timestampField">
                <el-option v-for="(item, index) in streamingCfg.columnOptions"
                :key="index"
                :label="item"
                :value="item">
                </el-option>
              </el-select>
            </el-form-item>
            <el-form-item :label="$t('parserProperties')" prop="parserProperties">
                <el-input v-model="kafkaMeta.parserProperties" placeholder="tsColName=createdAt;tsParser=org.apache.kylin.source.kafka.DateTimeParser;tsPattern=MMM dd,yyyy hh:mm:ss aa"></el-input>
            </el-form-item>
          </div>
      </el-card>
    </el-form>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleError, handleSuccess } from '../../util/business'
export default {
  name: 'createKafka',
  props: ['show'],
  watch: {
    'show' (v) {
      if (!v) {
        this.loading = false
        this.initKafkaDialog()
        this.treeData = []
        this.sourceSchema = ''
        this.columnList = []
        this.showConvertBox = false
        this.showTopicBox = false
      }
    }
  },
  data () {
    return {
      rules: {
        name: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' }
        ],
        parserName: [
        { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' }
        ],
        timestampField: [
        { required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change' }
        ]
      },
      streamingMeta: {name: '', type: 'kafka'},
      currentCheck: -1,
      sourceSchema: '',
      showTopicBox: false,
      showConvertBox: false,
      database: 'DEFAULT',
      kafkaMeta: {
        name: '',
        topic: '',
        timeout: '60000',
        bufferSize: '65536',
        parserName: 'org.apache.kylin.source.kafka.TimedJsonStreamParser',
        margin: '300000',
        clusters: [{
          brokers: []
        }],
        parserProperties: '',
        timestampField: ''
      },
      loading: false,
      columnList: [],
      streamingCfg: {
        columnOptions: [],
        timestampField: ''
      },
      timestampColumnExist: false,
      streamingAutoGenerateMeasure: [
        {name: 'year_start', type: 'date'},
        {name: 'quarter_start', type: 'date'},
        {name: 'month_start', type: 'date'},
        {name: 'week_start', type: 'date'},
        {name: 'day_start', type: 'date'},
        {name: 'hour_start', type: 'timestamp'},
        {name: 'minute_start', type: 'timestamp'}
      ],
      dataTypes: ['tinyint', 'smallint', 'int', 'bigint', 'float', 'double', 'decimal', 'timestamp', 'date', 'string', 'varchar(256)', 'char', 'boolean', 'binary'],
      // databaseOption: ['DEFAULT'],
      treeProps: {
        children: 'children',
        label: 'label'
      },
      treeData: [],
      sampleData: null
    }
  },
  methods: {
    ...mapActions({
      clusterInfo: 'GET_CLUSTER_INFO',
      topicInfo: 'GET_TOPIC_INFO'
    }),
    initKafkaDialog () {
      this.kafkaMeta = {
        name: '',
        topic: '',
        timeout: '60000',
        bufferSize: '65536',
        parserName: 'org.apache.kylin.source.kafka.TimedJsonStreamParser',
        margin: '300000',
        clusters: [{
          brokers: []
        }],
        parserProperties: ''
      }
    },
    addBroker: function () {
      if (this.currentCheck <= 0) {
        this.kafkaMeta.clusters[0].brokers.push({id: '', host: '', port: ''})
        this.currentCheck = this.kafkaMeta.clusters[0].brokers.length - 1
      }
    },
    checkBroker: function (index) {
      this.currentCheck = -1
    },
    editBroker: function (index) {
      this.currentCheck = index
    },
    removeBroker: function (index) {
      if (this.currentCheck > index) {
        this.currentCheck--
      }
      this.kafkaMeta.clusters[0].brokers.splice(index, 1)
    },
    getClusterInfo: function () {
      var editor = this.$refs.jsonDataBox.editor
      editor.setValue(' ')
      this.sourceSchema = ''
      this.treeData = []
      this.loading = true
      this.clusterInfo({
        project: localStorage.getItem('selected_project'),
        kafkaConfig: JSON.stringify(this.kafkaMeta),
        streamingConfig: JSON.stringify(this.streamingMeta)
      }).then((res) => {
        handleSuccess(res, (result) => {
          this.showTopicBox = true
          var data = result
          for (let key of Object.keys(data)) {
            let treeNode = {label: key, children: []}
            data[key].forEach(function (cluster) {
              treeNode.children.push({label: cluster})
            })
            this.treeData.push(treeNode)
          }
        })
        this.loading = false
      }).catch((res) => {
        handleError(res)
        this.loading = false
      })
    },
    getTopicInfo: function (node, nodeDesc) {
      if (node.children) {
        return
      } else {
        this.kafkaMeta.topic = node.label
        // this.kafkaMeta.name = nodeDesc.parent.data.label
        let topic = {
          cluster: nodeDesc.parent.data.label,
          name: node.label,
          kafka: {
            project: localStorage.getItem('selected_project'),
            kafkaConfig: JSON.stringify(this.kafkaMeta),
            streamingConfig: JSON.stringify(this.streamingMeta)
          }
        }
        this.topicInfo(topic).then((res) => {
          handleSuccess(res, (data) => {
            this.sampleData = data
            this.sourceSchema = JSON.stringify(data[0])
          })
        }).catch((res) => {
          handleError(res)
        })
      }
    },
    convertJson: function (jsonData) {
      this.showConvertBox = true
      try {
        var parseResult = JSON.parse(jsonData)
        parseResult = JSON.parse(parseResult)
      } catch (error) {
        return
      }
      let columnList = []
      function changeObjTree (obj, base) {
        base = base ? base + '_' : ''
        for (let i in obj) {
          if (Object.prototype.toString.call(obj[i]) === '[object Object]') {
            changeObjTree(obj[i], base + i)
            continue
          }
          columnList.push(createNewObj(base + i, obj[i]))
        }
      }

      function checkValType (val, key) {
        var defaultType
        if (typeof val === 'number') {
          if (/id/i.test(key) && val.toString().indexOf('.') === -1) {
            defaultType = 'int'
          } else if (val <= 2147483647) {
            if (val.toString().indexOf('.') !== -1) {
              defaultType = 'decimal'
            } else {
              defaultType = 'int'
            }
          } else {
            defaultType = 'timestamp'
          }
        } else if (typeof val === 'string') {
          if (!isNaN((new Date(val)).getFullYear()) && typeof ((new Date(val)).getFullYear()) === 'number') {
            defaultType = 'date'
          } else {
            defaultType = 'varchar(256)'
          }
        } else if (Object.prototype.toString.call(val) === '[object Array]') {
          defaultType = 'varchar(256)'
        } else if (typeof val === 'boolean') {
          defaultType = 'boolean'
        }
        return defaultType
      }

      function createNewObj (key, val) {
        var obj = {}
        obj.name = key
        obj.type = checkValType(val, key)
        obj.value = val
        obj.fromSource = 'Y'
        obj.checked = 'Y'
        if (Object.prototype.toString.call(val) === '[object Array]') {
          obj.checked = 'N'
        }
        return obj
      }

      changeObjTree(parseResult)
      for (var i = 0; i < this.streamingAutoGenerateMeasure.length; i++) {
        var defaultCheck = 'Y'
        columnList.push({
          'name': this.streamingAutoGenerateMeasure[i].name,
          'checked': defaultCheck,
          'type': this.streamingAutoGenerateMeasure[i].type,
          'value': null,
          'fromSource': 'N'
        })
      }
      return columnList
    },
    streamingOnChange: function () {
      this.columnList = this.convertJson(this.sourceSchema)
    },
    loadColumnZH: function () {
      let _this = this
      _this.streamingCfg.columnOptions = []
      _this.columnList.forEach(function (column, $index) {
        if (column.checked === 'Y' && column.fromSource === 'Y' && column.type === 'timestamp') {
          _this.streamingCfg.columnOptions.push(column.name)
          _this.timestampColumnExist = true
        }
      })
      if (_this.streamingCfg.columnOptions.length >= 1) {
        _this.kafkaMeta.timestampField = _this.streamingCfg.columnOptions[0]
        // _this.kafkaMeta.parserProperties = 'tsColName=' + _this.streamingCfg.parseTsColumn
      } else {
        _this.kafkaMeta.timestampField = ''
        // _this.kafkaMeta.parserProperties = ''
      }
    }
  },
  computed: {
    topicBtnDisabled () {
      return this.kafkaMeta.clusters[0].brokers.length > 0
    },
    databaseOption () {
      var arr = ['DEFAULT']
      var datasource = this.$store.state.datasource.dataSource[localStorage.getItem('selected_project')]
      if (datasource) {
        datasource.forEach((d) => {
          if (arr.indexOf(d.database) < 0) {
            arr.push(d.database)
          }
        })
      }
      return arr
    }
  },
  created () {
    this.$on('kafkaFormValid', (t) => {
      this.$refs['kafkaForm'].validate((valid) => {
        if (valid && /^\w+$/.test(this.kafkaMeta.name)) {
          // this.kafkaMeta.timestampField = this.streamingCfg.timestampField
          if (!/^\w+$/.test(this.kafkaMeta.name)) {
            this.$message('Streaming Table ' + this.$t('kylinLang.common.nameFormatValidTip'))
            return
          }
          this.$emit('validSuccess', {
            database: this.database,
            tableName: this.kafkaMeta.name,
            columnList: this.columnList,
            kafkaMeta: this.kafkaMeta,
            streamingMeta: this.streamingMeta,
            sampleData: this.sampleData
          })
        }
      })
    })
  },
  locales: {
    'en': {host: 'Host', port: 'Port', action: 'Action', cluster: 'Cluster', clusterInfo: 'Get Cluster Info', tableName: 'TABLE NAME', column: 'Column', columnType: 'Column Type', comment: 'Comment', timestamp: 'timestamp', derivedTimeDimension: 'Derived Time Dimension', parserSetting: 'Parser Setting', parserName: 'Parser Name', timestampField: 'Timestamp Field', parserProperties: 'Optional Properties'},
    'zh-cn': {host: '主机', port: '端口号', action: '操作', cluster: '集群', clusterInfo: '获取该集群信息', tableName: '表名', column: '列', columnType: '列类型', comment: '注释', timestamp: 'timestamp', derivedTimeDimension: '推导的时间维度', parserSetting: '解析器设置', parserName: '解析器名称', timestampField: '时间戳字段名称', parserProperties: '解析器属性'}
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';

#create-kafka{
  .table_margin {
   margin-top: 10px;
   margin-bottom: 10px;
  }
  .row_padding {
  padding-top: 5px;
  padding-bottom: 5px;
 }
 .textarea_height {
  height: 200px;
  width: 100%;
  overflow-y: auto;
 }
 .textarea_percent {
  height: 100%;
  width: 100%
 }
 .convertBtn {
  position: absolute;
  bottom: 0;
  background-color: #434b70;
  color: #fff;
  left: 65%;
  margin-left: 10px;
  padding: 5px;
  border-radius: 4px 4px 0 0;
  cursor: pointer;
  height: 30px;
  line-height: 30px;
 }
.convertBtn i {
   position: relative;
   left: 18px;
}
.el-card .el-card__body {
  padding: 0px;
}
.el-button--mini{
  background: transparent;
  color: @fff;
  border: 1px solid @base-color;
}
  .el-input{
    padding: 0;
    color:#fff;
  }
  .el-table__row{
    background: @input-bg;
  }
  .el-table__row:hover td{
    background: @input-bg!important;
  }
  .el-input{
    background: @input-bg;
  }
  .el-input__inner{
    border-color: #7881aa;
    color:#fff;
  }
}
</style>
