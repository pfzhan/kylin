<template>
	<div class="datasource">
      <div class="tree_list">
        <el-radio-group v-model="currentLoadType" class="ksd-mt-30 ksd-ml-30" v-if="isAdmin">
		    <el-radio-button label="Hive" @click.native="openLoadHiveListDialog"><icon name="download" scale="0.8"></icon><span> Hive</span></el-radio-button>
		    <el-radio-button label="Kfka" @click.native="openKafkaDialog"><icon name="download" scale="0.8"></icon><span> Kafka</span></el-radio-button>
		  </el-radio-group>

      <tree :treedata="hiveAssets"  :expandall='true'  maxLabelLen="20" :indent="4" :showfilter="false" :allowdrag="false" @nodeclick="clickTable"></tree>
      </div>
      <div class="table_content" >
       <img class="null_pic" src="../../assets/img/notabledata.png" v-show="!tableData"/>
       <div class="ksd-fright ksd-mt-20" style="position:relative;z-index:1" v-show="tableData">
       <kap-icon-button v-if="tableData.source_type === 0" icon="refresh" type="primary" :useload="true" @click.native="reloadTableDialogVisible" ref="reloadBtn">{{$t('reload')}}</kap-icon-button>
          <!-- <el-button type="info" icon="eyedropper">Sampling</el-button> -->
          <kap-icon-button icon="eyedropper" v-if="tableData.source_type === 0" type="info" :useload="true" @click.native="collectSampleDialogOpen" ref="sampleBtn">{{$t('sampling')}}</kap-icon-button>
          <kap-icon-button icon="eyedropper" v-if="tableData.source_type === 1" type="info" :useload="true" @click.native="collectKafkaSampleDialogOpen" ref="kafkaSampleBtn">{{$t('sampling')}}(Streaming)</kap-icon-button>
<!--           <el-button type="danger" @click.native="unloadTable" icon="delete2">Unload</el-button> -->
           <kap-icon-button v-if="isAdmin" icon="trash" type="danger" :useload="true" @click.native="unloadTable" ref="unloadBtn">{{$t('unload')}}</kap-icon-button>
          </div>
      	<el-tabs v-model="activeName" class="ksd-mt-20 clear" v-show="tableData">
		    <el-tab-pane :label="$t('kylinLang.dataSource.columns')" name="first">
	    	  <el-table
			    :data="tableData.columns"
			    border
			    style="width: 100%">
			    <el-table-column
			      prop="id"
			      label="ID"
			      width="66">
			    </el-table-column>
			    <el-table-column
			      prop="name"
			      :label="$t('kylinLang.dataSource.columnName')"
			     >
			    </el-table-column>
			    <el-table-column
			      width="120"
			      prop="datatype"
			      :label="$t('kylinLang.dataSource.dataType')">
			    </el-table-column>
			    <el-table-column
			      width="160"
			      :label="$t('kylinLang.dataSource.cardinality')">
             <template scope="scope">
              {{ tableData.cardinality[scope.row.name] }}
            </template>
			    </el-table-column>
			    <el-table-column
			      prop="comment"
			      :label="$t('kylinLang.dataSource.comment')">
			    </el-table-column>
			  </el-table>
		    </el-tab-pane>
		    <el-tab-pane :label="$t('kylinLang.dataSource.extendInfo')" name="second">
		    	<table class="extendinfo_table" v-if="extendData.data_source_properties" >
		    		<tr>
		    			<th>TABLE NAME:</th>
		    			<td>{{tableData.name}}</td>
		    		</tr>
                    <tr>
		    			<th>HIVE DATABASE:</th>
		    			<td>{{tableData.database}}</td>
		    		</tr>
		    		<tr>
		    			<th>TABLE TYPE:</th>
		    			<td>{{tableData.table_type}}</td>
		    		</tr>
		    		<tr>
		    			<th>SNAPSHOT TIME:</th>
		    			<td>{{tableData.database}}</td>
		    		</tr>
		    		<tr>
		    			<th>LOCATION:</th>
		    			<td>{{extendData.data_source_properties.location}}</td>
		    		</tr>
		    		<tr>
		    			<th>INPUT FORMAT:</th>
		    			<td>{{extendData.data_source_properties.hive_inputFormat}}</td>
		    		</tr>
		    		<tr>
		    			<th>OUT FORMAT:</th>
		    			<td>{{extendData.data_source_properties.hive_outputFormat}}</td>
		    		</tr>
		    		<tr>
		    			<th>OWNER:</th>
		    			<td>{{extendData.data_source_properties.owner}}</td>
		    		</tr>
		    		<tr>
		    			<th>TOTAL FILE NUMBER:</th>
		    			<td>{{extendData.data_source_properties.total_file_number}}</td>
		    		</tr>

		    		<tr>
		    			<th>TOTAL FILE SIZE:</th>
		    			<td>{{extendData.data_source_properties.total_file_size}}</td>
		    		</tr>
		    		<tr>
		    			<th>PARTITIONED:</th>
		    			<td>{{extendData.data_source_properties.partition_column!==''}}</td>
		    		</tr>
		    		<tr>
		    			<th>PARTITION COLUMNS:</th>
		    			<td>{{extendData.data_source_properties.partition_column}}</td>
		    		</tr>
		    	</table>
		    </el-tab-pane>
		    <el-tab-pane :label="$t('kylinLang.dataSource.statistics')" name="third" v-if="tableData.source_type === 0">
		    	 <el-table
			    :data="statistics"
			    border
			    style="width:100%"
			    >
			    <el-table-column
			      type="index"
			      label="ID"
			      width="58">
			    </el-table-column>
			    <el-table-column
			      prop="column"
			      :label="$t('kylinLang.dataSource.columns')"
			     >
			    </el-table-column>
			    <el-table-column
			      width="120"
			      prop="cardinality"
			      :label="$t('kylinLang.dataSource.cardinality')">
			    </el-table-column>
			    <el-table-column
			      width="100"
			      prop="max_value"
			      :label="$t('kylinLang.dataSource.maximum')">
			    </el-table-column>
			    <el-table-column
			      prop="min_value"
            width="100"
			      :label="$t('kylinLang.dataSource.minimal')">
			    </el-table-column>
			    <el-table-column
			      prop="max_length_value"
			      :label="$t('kylinLang.dataSource.maxLengthVal')">
			    </el-table-column>
			    <el-table-column
			      prop="min_length_value"
			      :label="$t('kylinLang.dataSource.minLengthVal')">
			    </el-table-column>
			    <el-table-column
			      width="120"
			      prop="null_count"
			      :label="$t('kylinLang.dataSource.nullCount')">
			    </el-table-column>
			  </el-table>
		    </el-tab-pane>
		    <el-tab-pane :label="$t('kylinLang.dataSource.sampleData')" name="fourth">
		      <el-table
			    :data="sampleData.slice(1)"
			    border
			    style="width: 100%">
			    <el-table-column v-for="(val,index) in sampleData[0]" :key="index"
			      :prop="''+index"
			      :label="sampleData[0][index]">
			    </el-table-column>
			  </el-table>
		    </el-tab-pane>
          <el-tab-pane label="Streaming Cluster" name="fifth" v-if="tableData.source_type === 1">
            <el-button type="primary" icon="edit" @click="editKafkaFormVisible=true" class="ksd-fright">{{$t('kylinLang.common.edit')}}</el-button>
            <view_kafka  ref="addkafkaForm" v-on:validSuccess="kafkaValidSuccess" :streamingData="currentStreamingTableData"  :tableName="currentStreamingTable" ></view_kafka>
          </el-tab-pane>
        </el-tabs>
      </div>

      <el-dialog size="small" :title="$t('loadhiveTables')" v-model="load_hive_dalog_visible" class="load_hive_dialog">
        <el-input v-model="filterVal" :placeholder="$t('filterInputTips')"></el-input>
        <el-row :gutter="20">
		  <el-col :span="8"><div class="grid-content bg-purple">
		  	 <div class="dialog_tree_box">
           <tree  @lazyload="loadChildNode" :multiple="true"  @nodeclick="clickHiveTable" :lazy="true" :treedata="hiveData" maxlevel="3" ref="subtree"  :showfilter="false" :allowdrag="false" ></tree>
          </div>
		  </div></el-col>
		  <el-col :span="16"><div class="grid-content bg-purple">
		  	<div class="tree_check_content ksd-mt-20">
		 	  <arealabel :labels="selectTables" @refreshData="refreshHiveData" changeable="unchange" :selectedlabels="selectTablesNames" :placeholder="$t('selectLeftHiveTip')" @removeTag="removeSelectedHive"  :datamap="{label: 'label', value: 'value'}"></arealabel>
        <div class="ksd-mt-20">
          <!-- <el-checkbox v-model="openCollectRange">Table Sampling</el-checkbox> -->
          <!-- <span class="demonstration">Sample percentage</span> -->
          <!-- <el-slider :min="0" show-stops :step="20" @change="changeBarVal" v-model="tableStaticsRange" :max="100" :format-tooltip="formatTooltip" :disabled = '!openCollectRange'></el-slider> -->
          <slider @changeBar="changeBar" :label="$t('sampling')" :show="load_hive_dalog_visible"></slider>
        </div>
		    </div>
		  </div></el-col>
		</el-row>
		  <div slot="footer" class="dialog-footer">
		    <el-button @click="load_hive_dalog_visible = false">{{$t('kylinLang.common.cancel')}}</el-button>
		    <el-button type="primary" @click="loadHiveList" :loading="loadHiveLoad">{{$t('kylinLang.common.sync')}}</el-button>
		  </div>
	    </el-dialog>

     <!-- reload table dialog -->
     <el-dialog :title="$t('setScanRange')" v-model="scanRatioDialogVisible" @close="cancelReloadTable">
        <el-row :gutter="20">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content ksd-mt-20">
              <div class="ksd-mt-20">
                <!-- <el-checkbox v-model="openCollectRange">Table Sampling</el-checkbox> -->
               <!--   <el-slider v-model="tableStaticsRange" :min="0"  show-stops :step="20" :max="100" :format-tooltip="formatTooltip" :disabled = '!openCollectRange'></el-slider> -->
                 <slider @changeBar="changeBar" :label="$t('sampling')"  :show="scanRatioDialogVisible"></slider>
              </div>
              </div>
            </div>
          </el-col>
        </el-row>
        <div slot="footer" class="dialog-footer">
          <el-button @click="cancelReloadTable">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="reloadTable">{{$t('kylinLang.common.submit')}}</el-button>
        </div>
      </el-dialog>
      <!-- 单个采样dialog -->
      <el-dialog :title="$t('setScanRange')" v-model="scanSampleRatioDialogVisible" >
        <el-row :gutter="20">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content ksd-mt-20">
              <div class="ksd-mt-20">
                 <slider label="Table Sampling" @changeBar="changeBar" :show="scanSampleRatioDialogVisible"></slider>
              </div>
              </div>
            </div>
          </el-col>
        </el-row>
        <div slot="footer" class="dialog-footer">
          <el-button @click="cancelLoadSample">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="loadSample">{{$t('kylinLang.common.submit')}}</el-button>
        </div>
      </el-dialog>

     <el-dialog title="Load Kafka Topic" v-model="kafkaFormVisible" top="10%" size="small">
        <create_kafka  ref="kafkaForm" v-on:validSuccess="kafkaValidSuccess"></create_kafka>
        <span slot="footer" class="dialog-footer">
          <el-button @click="kafkaFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="checkKafkaForm">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>


     <el-dialog title="Load Kafka Topic" v-model="editKafkaFormVisible" top="10%" size="small">
        <edit_kafka  ref="kafkaFormEdit"  v-on:validEditSuccess="kafkaEditValidSuccess" :streamingData="currentStreamingTableData" :streamingConfig="currentStreamingConfig"  :tableName="currentStreamingTable" ></edit_kafka>
        <span slot="footer" class="dialog-footer">
          <el-button @click="editKafkaFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="checkKafkaFormEdit">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>
      <el-dialog
        :title="$t('kylinLang.common.tip')"
        v-model="loadResultVisible"
        >
         <el-alert v-for=" su in loadResult.success" :key="su"
            :title="$t('kylinLang.common.success')+currentAction+'['+su+']'"
            type="success"
            :closable="false"
            class="ksd-mt-10"
            show-icon>
          </el-alert>
            <el-alert v-for=" fa in loadResult.fail" :key="fa"
            :title="$t('kylinLang.common.fail')+currentAction+'['+fa+']'"
            type="error"
            :closable="false"
            show-icon>
          </el-alert>
        <span slot="footer" class="dialog-footer">
          <el-button type="primary" @click="loadResultVisible = false">{{$t('kylinLang.common.close')}}</el-button>
        </span>
      </el-dialog>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, hasRole, kapWarn } from '../../util/business'
import { changeDataAxis } from '../../util/index'
import createKafka from '../kafka/create_kafka'
import editKafka from '../kafka/edit_kafka'
import viewKafka from '../kafka/view_kafka'
import arealabel from 'components/common/area_label'
export default {
  data () {
    return {
      test: ['add'],
      subMenu: 'Model',
      hiveAssets: [],
      loadResultVisible: false,
      scanRatioDialogVisible: false,
      scanSampleRatioDialogVisible: false,
      tableStaticsRange: 100,
      openCollectRange: false,
      loadHiveLoad: false,
      defaultProps: {
        children: 'children',
        label: 'label',
        data: 'data'
      },
      currentAction: this.$t('load'),
      tableData: '',
      extendData: {},
      statistics: [],
      sampleData: [],
      selectTables: [],
      selectTablesNames: [],
      project: localStorage.getItem('selected_project'),
      activeName: 'first',
      currentLoadType: '',
      load_hive_dalog_visible: false,
      kafkaFormVisible: false,
      editKafkaFormVisible: false,
      hiveData: [],
      filterVal: '',
      currentStreamingTable: '',
      currentStreamingTableData: '',
      currentStreamingConfig: '',
      loadResult: {
        success: [],
        fail: []
      }
    }
  },
  components: {
    arealabel,
    'create_kafka': createKafka,
    'edit_kafka': editKafka,
    'view_kafka': viewKafka
  },
  created () {
    if (this.project) {
      this.loadHiveTree()
    }
  },
  methods: {
    ...mapActions({
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      loadTableExt: 'LOAD_DATASOURCE_EXT',
      loadDatabase: 'LOAD_HIVEBASIC_DATABASE',
      loadTablesByDatabse: 'LOAD_HIVE_TABLES',
      loadHiveInProject: 'LOAD_HIVE_IN_PROJECT',
      unloadHiveInProject: 'UN_LOAD_HIVE_IN_PROJECT',
      saveSampleData: 'SAVE_SAMPLE_DATA',
      saveKafka: 'SAVE_KAFKA',
      updateKafka: 'UPDATE_KAFKA',
      collectSampleData: 'COLLECT_SAMPLE_DATA',
      getTableJob: 'GET_TABLE_JOB',
      collectKafkaSampleData: 'LOAD_KAFKA_SAMPLEDATA',
      getKafkaTableDetail: 'GET_KAFKA_CONFIG',
      getStreamingConfig: 'LOAD_STREAMING_CONFIG'
    }),
    changeBar (val) {
      this.tableStaticsRange = val
      this.openCollectRange = !!val
    },
    refreshHiveData (val) {
      this.selectTablesNames = val
    },
    // 初始化加载hive列表的弹窗
    openLoadHiveListDialog () {
      if (!this.project) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        return
      }
      this.load_hive_dalog_visible = true
      this.$refs.subtree.cancelCheckedAll()
      this.selectTables = []
      this.selectTablesNames = []
    },
    openKafkaDialog () {
      if (!this.project) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        return
      }
      this.kafkaFormVisible = true
    },
    collectKafkaSampleDialogOpen () {
      var tableName = this.tableData.database + '.' + this.tableData.name
      this.collectKafkaSampleData(tableName).then((res) => {
        this.$message('采样成功！')
        this.$refs.kafkaSampleBtn = false
      }, (res) => {
        this.$refs.kafkaSampleBtn = false
      })
    },
    // loadKafkaData () {
    //   var tableName = this.tableData.database + '.' + this.tableData.name
    //   this.getKafkaTableDetail(tableName).then((res) => {
    //     handleSuccess(res, (data) => {
    //       this.currentStreamingData = Object.assign(this.currentStreamingData, data)
    //     })
    //   })
    // },
    removeSelectedHive (val) {
      this.$refs.subtree.cancelNodeChecked(val)
      this.selectTablesNames.splice(this.selectTablesNames.indexOf(val), 1)
      this.selectTables = this.selectTables.filter((t) => {
        return t.id === val
      })
    },
    // 加载Hive列表
    loadHivesAction (tableNamesArr, btn) {
      this.loadHiveInProject({
        project: this.project,
        tables: tableNamesArr.join(','),
        data: {
          ratio: (this.tableStaticsRange / 100).toFixed(2),
          tables: tableNamesArr,
          project: this.project,
          needProfile: this.openCollectRange
        }
      }).then((response) => {
        btn.loading = false
        handleSuccess(response, (data) => {
          this.$set(this.loadResult, 'success', data['result.loaded'])
          this.$set(this.loadResult, 'fail', data['result.unloaded'])
        })
        this.load_hive_dalog_visible = false
        this.scanSampleRatioDialogVisible = false
        this.scanRatioDialogVisible = false
        this.loadResultVisible = true
        this.selectTables = []
        this.selectTablesNames = []
        this.$refs.subtree.cancelCheckedAll()
      }, (res) => {
        btn.loading = false
        handleError(res)
      })
    },
    // reload table meta data
    reloadTableDialogVisible () {
      this.tableStaticsRange = 100
      this.openCollectRange = 100
      this.scanRatioDialogVisible = false
      this.currentAction = '加载'
      var tableName = this.tableData.database + '.' + this.tableData.name
      this.checkTableHasJob(tableName, () => {
        // this.scanRatioDialogVisible = true
        this.loadHivesAction([tableName], this.$refs.reloadBtn)
      }, () => {
        this.scanRatioDialogVisible = true
      })
    },
    cancelReloadTable () {
      this.scanRatioDialogVisible = false
      this.$refs.reloadBtn.loading = false
    },
    cancelLoadSample () {
      this.scanSampleRatioDialogVisible = false
      this.$refs.sampleBtn.loading = false
    },
    // 检查Table是否有正在运行的JOB
    checkTableHasJob (tableName, cb, hasNotCb) {
      this.getTableJob(tableName).then((res) => {
        handleSuccess(res, (data) => {
          if (data && (data.job_status === 'FINISHED' || data.progress === '100') || !data) {
            hasNotCb()
          } else {
            cb()
          }
        })
      }, (res) => {
        hasNotCb()
      })
    },
    // 单个采样弹窗初始化
    collectSampleDialogOpen () {
      this.tableStaticsRange = 100
      this.openCollectRange = 100
      this.checkTableHasJob(this.tableData.database + '.' + this.tableData.name, () => {
        kapWarn(this.$t('hasCollectJob'))
        this.$refs.sampleBtn.loading = false
      }, () => {
        this.scanSampleRatioDialogVisible = true
      })
    },
    // 单个table的采样
    loadSample () {
      this.collectSampleData({
        project: this.project,
        tableName: this.tableData.database + '.' + this.tableData.name,
        data: {
          ratio: (this.tableStaticsRange / 100).toFixed(2)
        }
      }).then((res) => {
        handleSuccess(res, () => {
          this.$refs.sampleBtn.loading = false
          this.scanSampleRatioDialogVisible = false
          this.$message(this.$t('loadTableJobBeginTips'))
        })
      }, (res) => {
        handleError(res)
        this.$refs.sampleBtn.loading = false
        this.scanSampleRatioDialogVisible = false
      })
    },
    // 重新加载table
    reloadTable () {
      var tableName = this.tableData.database + '.' + this.tableData.name
      this.loadHivesAction([tableName], this.$refs.reloadBtn)
    },
    // 卸载Table
    unloadTable () {
      this.currentAction = this.$t('unload')
      this.unloadHiveInProject({
        project: this.project,
        tables: this.tableData.database + '.' + this.tableData.name
      }).then((response) => {
        handleSuccess(response, (data) => {
          this.$set(this.loadResult, 'success', data['result.unload.success'])
          this.$set(this.loadResult, 'fail', data['result.unload.fail'])
        })
        this.load_hive_dalog_visible = false
        this.loadResultVisible = true
        this.loadHiveTree()
        this.$refs['unloadBtn'].loading = false
        this.tableData = ''
      }, (res) => {
        handleError(res)
        this.$refs['unloadBtn'].loading = false
      })
    },
    loadHiveTree () {
      this.loadDataSourceByProject(this.project).then((response) => {
        handleSuccess(response, (data, code) => {
          var datasourceData = data
          var datasourceTreeData = {
            id: '1',
            label: 'Tables',
            children: []
          }
          var databaseData = {}
          for (var i = 0; i < datasourceData.length; i++) {
            databaseData[datasourceData[i].database] = databaseData[datasourceData[i].database] || []
            databaseData[datasourceData[i].database].push(datasourceData[i])
          }
          for (var s in databaseData) {
            var obj = {}
            obj.id = s
            obj.label = s
            obj.children = []

            for (var f = 0; f < databaseData[s].length; f++) {
              var childObj = {}
              childObj.id = s + '$' + databaseData[s][f].name
              childObj.data = databaseData[s][f].name
              childObj.label = databaseData[s][f].name
              childObj.tags = databaseData[s][f].source_type === 0 ? null : ['S']
              // childObj.checked = true
              obj.children.push(childObj)
            }
            datasourceTreeData.children.push(obj)
          }
          this.hiveAssets = [datasourceTreeData]
        })
      }, (res) => {
        handleError(res)
      })
    },
    clickHiveTable (data) {
      if (data.id && data.id.indexOf('.') > 0) {
        var newArr = this.selectTables.filter(function (item) {
          return item.value === data.id
        })
        if (!newArr || newArr.length <= 0) {
          this.selectTables.push({
            label: data.id,
            value: data.id
          })
          this.selectTablesNames.push(data.id)
        }
      } else {
        this.loadTablesByDatabse(data.label).then((res) => {
          handleSuccess(res, (result, code, status, msg) => {
            var len = result && result.length || 0
            data.children = []
            for (var k = 0; k < len; k++) {
              data.children.push({
                id: data.label + '.' + result[k],
                label: result[k],
                children: []
              })
            }
          })
        }, (res) => {
          handleError(res)
        })
      }
    },
    loadChildNode (node, resolve) {
      if (node.level === 0) {
        return resolve([{label: 'Hive Tables'}])
      } else if (node.level === 1) {
        this.loadDatabase().then((res) => {
          handleSuccess(res, (data) => {
            var datasourceTreeData = []
            for (var i = 0; i < data.length; i++) {
              datasourceTreeData.push({id: data[i], label: data[i], children: []})
            }
            resolve(datasourceTreeData)
          })
        }, (res) => {
          handleError(res)
        })
      } else if (node.level === 2) {
        var subData = []
        this.loadTablesByDatabse(node.label).then((res) => {
          handleSuccess(res, (data) => {
            var len = data && data.length || 0
            for (var k = 0; k < len; k++) {
              subData.push({
                id: node.label + '.' + data[k],
                label: data[k]
              })
            }
            resolve(subData)
          })
        }, (res) => {
          handleError(res)
        })
      } else {
        resolve([])
      }
    },
    clickTable (leaf) {
      var databaseInfo = leaf.id.split('$')
      if (databaseInfo.length === 2) {
        var database = databaseInfo[0]
        var tableName = databaseInfo[1]
        this.showTableDetail(database, tableName)
      }
    },
    showTableDetail (databaseName, table) {
      var _this = this
      var database = databaseName
      var tableName = table
      var tableData = this.$store.state.datasource.dataSource[this.project]
      for (var k = 0; k < tableData.length; k++) {
        if (tableData[k].database === database && tableData[k].name === tableName) {
          this.tableData = tableData[k]
          break
        }
      }
      this.activeName = 'first'
      this.loadTableExt(database + '.' + tableName).then((res) => {
        handleSuccess(res, (data) => {
          _this.extendData = data
          for (var s = 0, len = _this.extendData.columns_stats && _this.extendData.columns_stats.length || 0; s < len; s++) {
            _this.extendData.columns_stats[s].column = this.tableData.columns[s].name
          }
          _this.statistics = _this.extendData.columns_stats
          var sampleData = changeDataAxis(_this.extendData.sample_rows)
          var basicColumn = [[]]
          for (var i = 0; i < sampleData.length; i++) {
            for (var m = 0; m < sampleData[i].length; m++) {
              basicColumn[0].push(_this.tableData.columns[m].name)
            }
            break
          }
          this.sampleData = basicColumn.concat(sampleData)
          if (_this.tableData.source_type === 1) {
            this.currentStreamingTable = data.table_name
            this.getKafkaTableDetail(this.currentStreamingTable).then((res) => {
              handleSuccess(res, (data) => {
                this.currentStreamingTableData = data[0] || null
              })
            })
            this.getStreamingConfig(this.currentStreamingTable).then((res) => {
              handleSuccess(res, (data) => {
                this.currentStreamingConfig = data && data[0]
              })
            })
            // console.log(this.currentStreamingTable, 9900000)
            return
          }
        })
      }, (res) => {
        handleError(res)
      })
    },
    loadHiveList () {
      this.currentAction = this.$t('load')
      if (this.selectTablesNames.length > 0) {
        this.loadHiveLoad = true
        this.loadHiveInProject({
          project: this.project,
          tables: this.selectTablesNames.join(','),
          data: {
            ratio: (this.tableStaticsRange / 100).toFixed(2),
            tables: this.selectTablesNames,
            project: this.project,
            needProfile: this.openCollectRange
          }
        }).then((response) => {
          handleSuccess(response, (data) => {
            this.$set(this.loadResult, 'success', data['result.loaded'])
            this.$set(this.loadResult, 'fail', data['result.unloaded'])
          })
          this.loadHiveTree()
          this.load_hive_dalog_visible = false
          this.loadResultVisible = true
          this.selectTables = []
          this.selectTablesNames = []
          this.loadHiveLoad = false
        }, (res) => {
          this.loadHiveLoad = false
          handleError(res)
          this.load_hive_dalog_visible = false
        })
      }
    },
    checkKafkaForm: function () {
      this.$refs['kafkaForm'].$emit('kafkaFormValid')
    },
    checkKafkaFormEdit: function () {
      this.$refs['kafkaFormEdit'].$emit('kafkaEditFormValid')
    },
    kafkaValidSuccess: function (data) {
      let columns = []
      data.columnList.forEach(function (column, $index) {
        if (column.checked === 'Y') {
          let columnInstance = {
            id: ++$index,
            name: column.name,
            datatype: column.type
          }
          columns.push(columnInstance)
        }
      })
      let tableData = {
        name: data.kafkaMeta.name,
        source_type: 1,
        columns: columns,
        database: data.database || 'Default'
      }
      data.streamingMeta.name = data.kafkaMeta.name
      this.saveSampleData({ tableName: data.database + '.' + data.tableName, sampleData: data.sampleData })
      this.saveKafka({
        kafkaConfig: JSON.stringify(data.kafkaMeta),
        streamingConfig: JSON.stringify(data.streamingMeta),
        project: this.project,
        tableData: JSON.stringify(tableData)
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t('saveSuccessful')
          })
          this.kafkaFormVisible = false
          this.loadHiveTree()
          // this.showTableDetail(data.database, data.tableName)
        })
      }, (res) => {
        handleError(res)
      })
    },
    kafkaEditValidSuccess: function (data) {
      this.updateKafka({
        kafkaConfig: JSON.stringify(data.kafkaMeta),
        streamingConfig: JSON.stringify(data.streamingMeta),
        project: this.project,
        tableData: ''
      }).then((res) => {
        // handleSuccess(res, (data) => {
        this.$message(this.$t('kylinLang.common.updateSuccess'))
        this.editKafkaFormVisible = false
        this.loadHiveTree()
        this.showTableDetail(data.kafkaMeta.name.split('.')[0], data.kafkaMeta.name.split('.')[1])
        this.activeName = 'fifth'
        // })
      }, (res) => {
        this.editKafkaFormVisible = false
        this.showTableDetail(data.kafkaMeta.name.split('.')[0], data.kafkaMeta.name.split('.')[1])
        handleError(res)
      })
    }
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  watch: {
    'filterVal' (val) {
      this.$refs.subtree.$emit('filter', val)
    },
    'openCollectRange' (val) {
      if (val) {
        this.tableStaticsRange = 100
      }
    }
  },
  mounted () {
  },
  locales: {
    'en': {'load': 'Load', 'reload': 'Reload', 'sampling': 'Sampling', 'unload': 'Unload', 'loadhiveTables': 'Load Hive Table Metadata', 'selectLeftHiveTip': 'Please select tables from the left hive table tree', 'setScanRange': 'Scan Range Setting', 'filterInputTips': 'Please input the hive table name to filter', 'loadTableJobBeginTips': 'Collect job start running!You can go to Monitor page to watch the progress!', 'hasCollectJob': 'There has been a running collect job!You can go to Monitor page to watch the progress!'},
    'zh-cn': {'load': '加载', 'reload': '重载', 'sampling': '采样', 'unload': '卸载', 'loadhiveTables': '加载Hive表元数据', 'selectLeftHiveTip': '请在左侧选择要加载的table', 'setScanRange': '设置扫描范围', 'filterInputTips': '请输入hive表名进行过滤', 'loadTableJobBeginTips': '采集开始，您可以到Monitor页面查看采样进度！', 'hasCollectJob': '已有一个收集作业正在进行中，您可以去Monitor页面查看进度!'}
  }
}
</script>
<style lang="less" >
@import '../../less/config.less';
.modeltab{
    &>.el-tabs{
      &>.el-tabs__header{
        margin: 0;
      }
      .el-tabs__content{
        overflow-y: auto;
      }
    }
  }
  .sub_menu{
    .el-tabs__content{
      overflow-y: auto;
    }
    &>.el-tabs{
      &>.el-tabs__header{
        margin: 0;
      }
    }
  }
.datasource{
  .null_pic{
    position: absolute;
    left: 50%;
    top: 200px;
    margin-left: -50px;

  }
  .normalTable:before{
      // content:"N";
      // background-color:yellow;
      // color:red;
      // font-weight:bold;
   }
   .streamingTable:before {
      content:"S";
      background-color:blue;
      color:#fff;
      display: inline-block;
      width: 16px;
      height: 16px;
      line-height: 16px;
      text-align: center;
      position: absolute;
      left: -20px;
      top:4px;
      border-radius: 8px;
   }
	.sub_menu{
		.el-tabs__content{
			overflow-y: auto;
		}
    &>.el-tabs{
      &>.el-tabs__header{
        margin: 0;
      }
    }
	}
	.tree_list {
		height: 1000px;
		display: inline-block;
		position: relative;
		top:-15px;
    background: @grey-color;
    label .el-radio-button__inner{
      color: @fff;
      border-color: @base-color;
    }
    label:nth-child(1) .el-radio-button__inner{
      background: @base-color;
    }
    label:nth-child(2) .el-radio-button__inner{
      background: @grey-color;
    }
    .ksd-ml-30{
      margin-left: 45px!important;
      margin-top: 40px!important;
    }
	}

	.table_content{
    background: @tableBC!important;
		padding: 20px;
		position: absolute;
		left: 250px;
		top:-16px;
		right: 0;
		min-height: 1000px;
		border-left: solid 1px #d1dbe5;
    background-color: #fff;
		.el-tabs__content{
			overflow-y: auto;
		}
	}
	.extendinfo_table {
		width: 100%;
		tr{
			border-top:solid 1px #ccc;
			height: 40px;
		}
		tr:nth-child(odd) {
			background-color: #fff;
		}
		th{
			padding: 10px;
			text-align: right;
			width: 200px;
			font-weight: normal;
			font-size: 14px;
		}
		td{
			padding: 10px;
			font-size: 12px;
		}
	}
	.dialog_tree_box{
		.tree_box{
			top:10px;
			height: 400px;
			overflow: auto;
			.el-tree {
			  background-color: #fff;
        width: auto;
			}
		}
		.tree_check_content{
			min-height: 500px;
			top:10px;
		}
	}
  
}
</style>
