<template>
    <el-form :model='kafkaMeta' label-position='right' :rules='rules'  label-width='180px' ref='kafkaForm'>
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
        <el-row class='row_padding'>
          <el-col :span='24'>
            <el-button size='mini' icon='loading' @click='getClusterInfo'>{{$t('clusterInfo')}}
            </el-button>
          </el-col>
        </el-row>
        <el-row class='row_padding ' >
          <el-col :span='10' v-loading.body='loading'>
            <el-tree :data="treeData" :props="treeProps" class='textarea_height'
             @node-click="getTopicInfo">
            </el-tree>
          </el-col>
          <el-col :span='14'>
            <editor v-model="sourceSchema" lang="json" theme="chrome" width="100%" height="600" useWrapMode="true"></editor>
          </el-col>
          <div class='convertBtn' @click='streamingOnChange();loadColumnZH();'>      <p>Convert</p>
            <i class='el-icon-arrow-down' aria-hidden='true'></i>
          </div>
        </el-row>
        <el-card>
          <el-row slot='header'>
            <el-col :span='6'>
             {{$t('tableName')}}:
            </el-col>
            <el-col :span='9'>
              <el-select v-model="database">
                <el-option v-for="(item, index) in databaseOption" :key="index"
                :label="item"
                :value="item">
                </el-option>
              </el-select>
              <b>/</b>
            </el-col>
            <el-col :span='9'>
              <el-input  v-model="kafkaMeta.name"></el-input>
            </el-col>
          </el-row>
          <el-table
          :data='columnList'
          style='width: 100%'>
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
                <el-select v-model="scope.row.type" >
                  <el-option
                    v-for="(item, index) in dataTypes"
                    :key="index"
                    :label="item"
                    :value="item"
                    @change="loadColumnZH()">
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
        </el-card>
        <el-card >
          <div slot="header">
            <span >{{$t('parserSetting')}}</span>
          </div>
          <div>
            <el-form-item :label="$t('parserName')" prop="parserName">
                <el-input v-model="kafkaMeta.parserName"></el-input>
            </el-form-item>
            <el-form-item :label="$t('parserTimestampField')">
              <el-select v-model="streamingCfg.parseTsColumn">
                <el-option v-for="(item, index) in streamingCfg.columnOptions"
                :key="index"
                :label="item"
                :value="item">
                </el-option>
              </el-select>
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
import editor from 'vue2-ace-editor'
import 'brace/mode/javascript'
import 'brace/mode/less'
import 'brace/mode/json'
import 'brace/theme/chrome'
export default {
  name: 'createKafka',
  data () {
    return {
      rules: {
        name: [
        { required: true, message: '', trigger: 'change' }
        ],
        parserName: [
        { required: true, message: '', trigger: 'change' }
        ],
        parserProperties: [
        { required: true, message: '', trigger: 'change' }
        ]
      },
      streamingMeta: {name: '', type: 'kafka'},
      currentCheck: -1,
      sourceSchema: '',
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
        parserProperties: ''
      },
      loading: false,
      columnList: [],
      streamingCfg: {
        columnOptions: [],
        parseTsColumn: ''
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
      databaseOption: ['DEFAULT'],
      treeProps: {
        children: 'children',
        label: 'label'
      },
      treeData: []
    }
  },
  components: {
    editor
  },
  methods: {
    ...mapActions({
      clusterInfo: 'GET_CLUSTER_INFO',
      topicInfo: 'GET_TOPIC_INFO'
    }),
    addBroker: function () {
      if (this.currentCheck === -1) {
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
        this.currentCheck --
      }
      this.kafkaMeta.clusters[0].brokers.splice(index, 1)
    },
    getClusterInfo: function () {
      let _this = this
      _this.treeData = []
      this.loading = true
      this.clusterInfo({
        project: 'a',
        kafkaConfig: this.kafkaMeta,
        streamingConfig: this.streamingMeta
      }).then((result) => {
        for (let key of Object.keys(result.body)) {
          let treeNode = {label: key, children: []}
          result.body[key].forEach(function (cluster) {
            treeNode.children.push({label: cluster})
          })
          _this.treeData.push(treeNode)
        }
        this.loading = false
      }).catch((result) => {
        this.loading = false
      })
    },
    getTopicInfo: function (node, nodeDesc) {
      let _this = this
      if (node.children) {
        return
      } else {
        let topic = {
          cluster: nodeDesc.parent.data.label,
          name: node.label,
          kafka: {
            project: 'a',
            kafkaConfig: this.kafkaMeta,
            streamingConfig: this.streamingMeta
          }
        }
        _this.topicInfo(topic).then((result) => {
          if (result && result.body.length > 0) {
            _this.sourceSchema = result.body[0]
          }
        }).catch((result) => {
          console.log(result)
        })
      }
    },
    convertJson: function (jsonData) {
      try {
        var parseResult = JSON.parse(jsonData)
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
      _this.columnList.forEach(function (column, $index) {
        if (column.checked === 'Y' && column.fromSource === 'Y' && column.type === 'timestamp') {
          _this.streamingCfg.columnOptions.push(column.name)
          _this.timestampColumnExist = true
        }
      })
      if (_this.streamingCfg.columnOptions.length >= 1) {
        _this.streamingCfg.parseTsColumn = _this.streamingCfg.columnOptions[0]
        _this.kafkaMeta.parserProperties = 'tsColName=' + _this.streamingCfg.parseTsColumn
      } else {
        _this.streamingCfg.parseTsColumn = []
        _this.kafkaMeta.parserProperties = ''
      }
    }
  },
  created () {
    let _this = this
    this.$on('kafkaFormValid', (t) => {
      _this.$refs['kafkaForm'].validate((valid) => {
        if (valid) {
          _this.$emit('validSuccess', {
            database: _this.database,
            tableName: _this.tableName,
            columnList: _this.columnList,
            kafkaMeta: _this.kafkaMeta,
            streamingMeta: _this.streamingMeta})
        }
      })
    })
  },
  locales: {
    'en': {host: 'Host', port: 'Port', action: 'Action', cluster: 'Cluster', clusterInfo: 'Get Cluster Info', tableName: 'TABLE NAME', column: 'Column', columnType: 'Column Type', comment: 'Comment', timestamp: 'timestamp', derivedTimeDimension: 'Derived Time Dimension', parserSetting: 'Parser Setting', parserName: 'Parser Name', parserTimestampField: 'Parser Timestamp Field', parserProperties: 'ParserProperties'},
    'zh-cn': {host: '主机', port: '端口号', action: '操作', cluster: '集群', clusterInfo: '获取该集群信息', tableName: '表名', column: '列', columnType: '列类型', comment: '注释', timestamp: 'timestamp', derivedTimeDimension: '推导的时间维度', parserSetting: '解析器设置', parserName: '解析器名称', parserTimestampField: '时间戳字段名称', parserProperties: '解析器属性'}
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
 .textarea_height {
  height: 600px;
  width: 100%
 }
 .textarea_percent {
  height: 100%;
  width: 100%
 }
 .convertBtn {
  position: absolute;
  bottom: 0;
  background-color: #ccc;
  left: 65%;
  margin-left: 10px;
  padding: 5px;
  color: #000;
  border-radius: 4px 4px 0 0;
  cursor: pointer;
  height: 35px;
  box-shadow: 2px 2px 2px #eee;
 }
.convertBtn i {
   position: relative;
   left: 18px; 
}
.el-card .el-card__body {
  padding: 0px;
}
</style>
