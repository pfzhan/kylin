<template>
	<div class="datasource" id="datasource">
    <div class="tree_list">
      <el-radio-group v-model="currentLoadType" class="ksd-mt-30 ksd-ml-30" v-if="isAdmin || hasProjectAdminPermission(project)">
		    <el-radio-button label="Hive" @click.native="openLoadHiveListDialog"><icon name="download" scale="0.8"></icon><span> Hive</span></el-radio-button>
		    <el-radio-button label="Kfka" @click.native="openKafkaDialog"><icon name="download" scale="0.8"></icon><span> Kafka</span></el-radio-button>
		  </el-radio-group>

      <tree :treedata="hiveAssets" :empty-text="$t('dialogHiveTreeNoData')" :expandall='true'  maxLabelLen="20" :indent="2" :showfilter="false" :allowdrag="false" @nodeclick="clickTable" maxlevel="3"></tree>
      </div>
      <div class="table_content" >
       <img class="null_pic" src="../../assets/img/no_table.png" v-show="!tableData"/>
       <div class="extendInfo" v-show="tableData">
         <p><span :title="extendData.table_name" style="font-size:16px;color:#218fea"> {{extendData.table_name|omit(50, '...')}}</span></p>
       </div>
       <div class="rightBtns" style="position:absolute;right:0;z-index:1;top:30px;right:16px;" v-show="tableData">
         <kap-icon-button v-if="tableData.source_type === 0 && (isAdmin || hasProjectAdminPermission(project))" icon="refresh" type="blue" :useload="true" @click.native="reloadTableDialogVisible" ref="reloadBtn">{{$t('reload')}}</kap-icon-button>
         <kap-icon-button v-if="isAdmin || hasProjectAdminPermission(project)" icon="trash" type="blue" :useload="true" @click.native="unloadTable" ref="unloadBtn">{{$t('unload')}}</kap-icon-button>
            <!-- <el-button type="info" icon="eyedropper">Sampling</el-button> -->
            <kap-icon-button icon="eyedropper" class="sampling" v-if="tableData.source_type === 0" type="info" :useload="true" @click.native="collectSampleDialogOpen" ref="sampleBtn">{{$t('samplingBtn')}}</kap-icon-button>
            <kap-icon-button icon="eyedropper" class="sampling" v-if="tableData.source_type === 1" type="info" :useload="true" @click.native="collectKafkaSampleDialogOpen" ref="kafkaSampleBtn">{{$t('samplingBtn')}}(Streaming)</kap-icon-button>
  <!--           <el-button type="danger" @click.native="unloadTable" icon="delete2">Unload</el-button> -->
            <p style="font-size:12px;margin-top:10px;text-align:right;padding-right:4px;" v-if="extendData.last_modified">{{$t('kylinLang.dataSource.lastModified')}} {{extendData.last_modified}}</p>
        </div>
       <el-tabs v-model="activeName" class="ksd-mt-40 clear" v-show="tableData" id="datasource-table">
		    <el-tab-pane :label="$t('kylinLang.dataSource.columns')" name="first">
          <el-input id="data-source-search" style="width:200px;" class="ksd-mb-10"
            :placeholder="$t('kylinLang.common.pleaseFilter')"
            icon="search"
            v-model="filterColumn"
            @change="filterColumnChange">
          </el-input>
  	    	  <el-table
  			    :data="tableColumnsByFilter"
  			    border
  			    style="width: 100%">
  			    <el-table-column
  			      prop="id"
  			      label="ID"
              sortable
              :sort-method="idSorted"
  			      width="80">
  			    </el-table-column>
  			    <el-table-column
  			      prop="name"
              sortable
  			      :label="$t('kylinLang.dataSource.columnName')"
  			     >
  			    </el-table-column>
  			    <el-table-column
  			      width="120"
  			      prop="datatype"
              sortable
  			      :label="$t('kylinLang.dataSource.dataType')">
  			    </el-table-column>
  			    <el-table-column
  			      width="160"
              sortable
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
		    <el-tab-pane :label="$t('kylinLang.dataSource.extendInfo')" name="second" >
		    	<table class="extendinfo_table" v-if="extendData.data_source_properties">
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
		    	<el-table :data="statistics" border style="width:100%" tooltip-effect="dark">
  			    <el-table-column
  			      type="index"
  			      label="ID"
  			      width="58">
  			    </el-table-column>
  			    <el-table-column
  			      prop="column"
              show-overflow-tooltip
  			      :label="$t('kylinLang.dataSource.columns')"
  			     >
  			    </el-table-column>
  			    <el-table-column
  			      width="120"
              show-overflow-tooltip
  			      prop="cardinality"
  			      :label="$t('kylinLang.dataSource.cardinality')">
  			    </el-table-column>
  			    <el-table-column
  			      width="100"
              show-overflow-tooltip
  			      prop="max_value"
  			      :label="$t('kylinLang.dataSource.maximum')">
  			    </el-table-column>
  			    <el-table-column
  			      prop="min_value"
              show-overflow-tooltip
              width="100"
  			      :label="$t('kylinLang.dataSource.minimal')">
  			    </el-table-column>
  			    <el-table-column
            show-overflow-tooltip
  			      prop="max_length_value"
  			      :label="$t('kylinLang.dataSource.maxLengthVal')">
  			    </el-table-column>
  			    <el-table-column
            show-overflow-tooltip
  			      prop="min_length_value"
  			      :label="$t('kylinLang.dataSource.minLengthVal')">
  			    </el-table-column>
  			    <el-table-column
            show-overflow-tooltip
  			      width="120"
  			      prop="null_count"
  			      :label="$t('kylinLang.dataSource.nullCount')">
  			    </el-table-column>
  			  </el-table>
          <p style="font-size:12px;" class="ksd-mt-10">{{$t('kylinLang.dataSource.totalRow')}} {{extendData.total_rows}}</p>
		    </el-tab-pane>
		    <el-tab-pane :label="$t('kylinLang.dataSource.sampleData')" name="fourth">
		      <el-table :data="sampleData.slice(1)" tooltip-effect="dark" border style="width: 100%">
  			    <el-table-column v-for="(val,index) in sampleData[0]" :key="index"
  			      :prop="''+index"
              show-overflow-tooltip
              :width="15* (sampleData[0][index] && sampleData[0][index].length || 0) + 20"
  			      :label="sampleData[0][index]">
  			    </el-table-column>
			    </el-table>
		    </el-tab-pane>
        <el-tab-pane label="Streaming Cluster" name="fifth" v-if="tableData.source_type === 1">
            <el-button type="primary" icon="edit" @click="editKafkaFormVisible=true" class="ksd-fright">{{$t('kylinLang.common.edit')}}</el-button>
            <view_kafka  ref="addkafkaForm" v-on:validSuccess="kafkaValidSuccess" :streamingData="currentStreamingTableData"  :tableName="currentStreamingTable" ></view_kafka>
        </el-tab-pane>
        <el-tab-pane :label="$t('access')" name="sixth">
           <access :tableData="tableData" v-if="activeName==='sixth'"></access>
        </el-tab-pane>
      </el-tabs>
      </div>

      <el-dialog size="small" :title="$t('loadhiveTables')" v-model="load_hive_dalog_visible" class="load_hive_dialog" :close-on-press-escape="false" :close-on-click-modal="false">
        <!-- <el-input v-model="filterVal" :placeholder="$t('filterInputTips')"></el-input> -->
        <el-row :gutter="20">
		  <el-col :span="8"><div class="grid-content bg-purple">
		  	 <div class="dialog_tree_box">
           <!--<tree :indent="2"
                 :multiple="true"
                 :treedata="hiveData"
                 :emptyText="dialogEmptyText"
                 maxLabelLen="24"
                 maxlevel="3"
                 ref="subtree"
                 :showfilter="true"
                 :allowdrag="false"
                 @nodeclick="clickHiveTable"></tree>-->
           <tree :indent="2"
           @lazyload="loadChildNode"
           :multiple="true"
           @nodeclick="clickHiveTable"
           :lazy="true"
           :treedata="hiveData"
           :emptyText="dialogEmptyText"
           maxlevel="3"
           ref="subtree"
           :maxLabelLen="24"
           :showfilter="false"
           :allowdrag="false" ></tree>
          </div>
		  </div></el-col>
		  <el-col :span="16"><div class="grid-content bg-purple">
		  	<div class="tree_check_content ksd-mt-20">
		 	  <arealabel :validateRegex="/^\w+\.\w+$/" @validateFail="selectedHiveValidateFail" @refreshData="refreshHiveData"  :selectedlabels="selectTablesNames" :allowcreate='true' placeholder=" " @removeTag="removeSelectedHive"  :datamap="{label: 'label', value: 'value'}"></arealabel>
        <div class="ksd-mt-10 ksd-extend-tips" v-html="$t('loadHiveTip')"></div>
        <div class="ksd-mt-20">
          <!-- <el-checkbox v-model="openCollectRange">Table Sampling</el-checkbox> -->
          <!-- <span class="demonstration">Sample percentage</span> -->
          <!-- <el-slider :min="0" show-stops :step="20" @change="changeBarVal" v-model="tableStaticsRange" :max="100" :format-tooltip="formatTooltip" :disabled = '!openCollectRange'></el-slider> -->
          <slider @changeBar="changeBar" :show="load_hive_dalog_visible">
            <span slot="label">{{$t('sampling')}}
              <common-tip placement="right" :content="$t('kylinLang.dataSource.collectStatice')" >
                 <icon name="question-circle" class="ksd-question-circle"></icon>
              </common-tip>
           </span>
    <!--        <span slot="sliderLabel">{{$t('kylinLang.dataSource.samplingPercentage')}} <common-tip placement="right" :content="$t('kylinLang.model.samplingPercentageTips')" >
                 <icon name="question-circle-o"></icon>
              </common-tip></span> -->
          </slider>
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
     <el-dialog :title="$t('reload')" v-model="scanRatioDialogVisible" @close="cancelReloadTable" :close-on-press-escape="false" :close-on-click-modal="false">
        <el-row :gutter="20">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content ksd-mt-20">
              <div class="ksd-mt-20">
                <!-- <el-checkbox v-model="openCollectRange">Table Sampling</el-checkbox> -->
               <!--   <el-slider v-model="tableStaticsRange" :min="0"  show-stops :step="20" :max="100" :format-tooltip="formatTooltip" :disabled = '!openCollectRange'></el-slider> -->
                 <slider @changeBar="changeBar" :show="scanRatioDialogVisible">
                   <span slot="label">{{$t('sampling')}}
                    <common-tip :content="$t('kylinLang.dataSource.collectStatice')" >
                       <icon name="question-circle" class="ksd-question-circle"></icon>
                    </common-tip>
                 </span>
          <!--         <span slot="sliderLabel">{{$t('kylinLang.dataSource.samplingPercentage')}} <common-tip placement="right" :content="$t('kylinLang.model.samplingPercentageTips')" >
                 <icon name="question-circle-o"></icon>
              </common-tip></span> -->
                 </slider>
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
      <el-dialog :title="$t('setScanRange')" size="tiny" v-model="scanSampleRatioDialogVisible" @close="cancelLoadSample" :close-on-press-escape="false" :close-on-click-modal="false">
        <span slot="title">{{$t('setScanRange')}} <common-tip placement="right" :content="$t('kylinLang.dataSource.collectStatice')" >
                 <icon name="question-circle" class="ksd-question-circle"></icon></common-tip></span>
        <el-row :gutter="20">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content ksd-mt-20">
              <div class="ksd-mt-20">
                 <slider  @changeBar="changeBar" :show="scanSampleRatioDialogVisible" :hideCheckbox="true" :range="100">
                 </slider>
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

     <el-dialog title="Load Kafka Topic" v-model="kafkaFormVisible" top="10%" size="small" :close-on-press-escape="false" :close-on-click-modal="false">
        <create_kafka  ref="kafkaForm" v-on:validSuccess="kafkaValidSuccess" :show="kafkaFormVisible"></create_kafka>
        <span slot="footer" class="dialog-footer">
          <el-button @click="kafkaFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="checkKafkaForm">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>


     <el-dialog title="Load Kafka Topic" v-model="editKafkaFormVisible" top="10%" size="small":close-on-press-escape="false" :close-on-click-modal="false">
        <edit_kafka  ref="kafkaFormEdit"  v-on:validEditSuccess="kafkaEditValidSuccess" :streamingData="currentStreamingTableData" :streamingConfig="currentStreamingConfig"  :tableName="currentStreamingTable" ></edit_kafka>
        <span slot="footer" class="dialog-footer">
          <el-button @click="editKafkaFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="checkKafkaFormEdit">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>
      <el-dialog  :title="$t('kylinLang.common.tip')"
        v-model="loadResultVisible" :close-on-press-escape="false" :close-on-click-modal="false">
         <el-alert v-for=" su in loadResult.success" :key="su"
            :title="currentAction + ' ' + $t('kylinLang.common.success') + ' ! ' + '['+su+']'"
            type="success"
            :closable="false"
            class="ksd-mt-10"
            show-icon>
          </el-alert>
            <el-alert v-for=" fa in loadResult.fail" :key="fa"
            :title="currentAction + ' ' + $t('kylinLang.common.fail') + ' ! ' + '['+fa+']'"
            type="error"
            :closable="false"
            class="ksd-mt-10"
            show-icon>
          </el-alert>
           <el-alert v-for=" fa in loadResult.running" :key="fa"
            :title=" currentAction + ' ' + $t('kylinLang.common.fail') + ' ! ' + $t('kylinLang.common.running') + '['+fa+']'"
            type="error"
            :closable="false"
            class="ksd-mt-10"
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
import { handleSuccess, handleError, hasRole, kapWarn, transToGmtTime, hasPermission } from '../../util/business'
import { permissions } from '../../config'
import { changeDataAxis, isFireFox, objectArraySort } from '../../util/index'
import createKafka from '../kafka/create_kafka'
import editKafka from '../kafka/edit_kafka'
import viewKafka from '../kafka/view_kafka'
import arealabel from 'components/common/area_label'
import access from './access'
// import Scrollbar from 'smooth-scrollbar'
export default {
  data () {
    return {
      test: ['add'],
      subMenu: 'Model',
      filterColumn: '',
      tableColumnsByFilter: [],
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
      treePerPage: 50,
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
      // pageLoadHiveAllData: [],
      // dialogTreeHiveDataIsReady: false,
      dialogEmptyText: this.$t('dialogHiveTreeLoading'),
      filterVal: '',
      currentStreamingTable: '',
      currentStreamingTableData: '',
      currentStreamingConfig: '',
      loadResult: {
        success: [],
        fail: [],
        running: []
      }
    }
  },
  components: {
    arealabel,
    'create_kafka': createKafka,
    'edit_kafka': editKafka,
    'view_kafka': viewKafka,
    'access': access
  },
  created () {
    if (this.project) {
      this.loadHiveTree()
      // 页面上默默加载全部数据
      // this.loadAllHiveTreeData()
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
    hasPermissionOfProject (project) {
      var projectList = this.$store.state.project.allProject
      var len = projectList && projectList.length || 0
      var projectId = ''
      for (var s = 0; s < len; s++) {
        if (projectList[s].name === project) {
          projectId = projectList[s].uuid
        }
      }
      return hasPermission(this, projectId, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask, permissions.OPERATION.mask)
    },
    filterColumnChange (filterVal) {
      if (filterVal) {
        this.tableColumnsByFilter = this.tableData.columns.filter((col) => {
          return col.name.toUpperCase().indexOf(filterVal.toUpperCase()) >= 0
        })
      } else {
        this.tableColumnsByFilter = this.tableData.columns
      }
    },
    idSorted (a, b) {
      return +a.id > +b.id
    },
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
      // 直接赋值，会有监控报错，不知道是不是tree的问题，加延迟，类似拉取得是请求
      /* window.setTimeout(() => {
        this.hiveData = this.pageLoadHiveAllData
      }) */
      this.load_hive_dalog_visible = true
      this.$refs.subtree && this.$refs.subtree.cancelCheckedAll()
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
      this.collectKafkaSampleData({tableName: tableName, project: this.project}).then((res) => {
        this.$message(this.$t('kylinLang.common.submitSuccess'))
        this.$refs.kafkaSampleBtn.loading = false
      }, (res) => {
        this.$refs.kafkaSampleBtn.loading = false
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
    selectedHiveValidateFail () {
      this.$message(this.$t('selectedHiveValidateFailText'))
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
          this.$set(this.loadResult, 'running', data['result.running'])
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
      this.currentAction = this.$t('load')
      var tableName = this.tableData.database + '.' + this.tableData.name
      this.checkTableHasJob(tableName, () => {
        // this.scanRatioDialogVisible = true
        this.$refs.reloadBtn.loading = false
        this.$message(this.$t('kylinLang.dataSource.dataSourceHasJob'))
        return
        // this.loadHivesAction([tableName], this.$refs.reloadBtn)
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
      this.getTableJob({tableName: tableName, project: this.project}).then((res) => {
        handleSuccess(res, (data) => {
          if (data && (data.job_status === 'FINISHED' || data.progress === '100' || data.job_status === 'DISCARDED') || !data) {
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
          this.$set(this.loadResult, 'running', data['result.running'])
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
      this.loadDataSourceByProject({project: this.project, isExt: true}).then((response) => {
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
            obj.children = objectArraySort(obj.children, true, 'label')
            datasourceTreeData.children.push(obj)
          }
          datasourceTreeData.children = objectArraySort(datasourceTreeData.children, true, 'label')
          this.hiveAssets = datasourceData.length === 0 ? [] : [datasourceTreeData]
          this.$nextTick(() => {
            // 可视区高 - 66（header的高）- 48（面包屑的占位）- 33（tab高）- 33（子tab高）- 79（列表上方那几块的高）
            let iTop = this.$el.querySelectorAll('.tree_box')[0].offsetTop
            let iHeight = window.innerHeight - 66 - 48 - 33 - 33 - iTop
            this.$el.querySelector('.filter-tree').style.height = iHeight + 'px'
          })
        })
      }, (res) => {
        handleError(res)
      })
    },
    /* loadAllHiveTreeData: function () {
      this.dialogTreeHiveDataIsReady = false
      this.loadDatabase().then((res) => {
        handleSuccess(res, (data) => {
          var targetData = [
            {
              label: 'Hive Tables',
              children: []
            }
          ]
          for (var i = 0; i < data.length; i++) {
            var item = data[i]
            var obj = {
              id: item.databaseName,
              label: item.databaseName,
              children: []
            }

            if (item.tableNames.length > 0) {
              var arr = item.tableNames.map(function (tableitem) {
                return {id: item.databaseName + '.' + tableitem, label: tableitem, children: []}
              })
              obj.children = arr
            }
            targetData[0].children.push(obj)
          }
          this.pageLoadHiveAllData = targetData
          this.dialogTreeHiveDataIsReady = true
          this.dialogEmptyText = this.pageLoadHiveAllData.length > 0 ? '' : this.$t('dialogHiveTreeNoData')
        })
      }, (res) => {
        this.dialogTreeHiveDataIsReady = true
        handleError(res)
      })
    }, */
    clickHiveTable (data, vnode) {
      if (data.id && data.id.indexOf('.') > 0 && !data.isMore) {
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
      }
      var node = data
      if (node.index) {
        // 加载更多
        vnode.store.remove(vnode.data)
        var addData = node.fullData.slice(node.index, node.index + this.treePerPage)
        for (var k = 0; k < addData.length; k++) {
          node.parentStore.append({
            id: node.parentLabel + '.' + addData[k],
            label: addData[k]
          }, node.parentNode)
        }
        if (node.index + this.treePerPage < node.fullData.length) {
          node.parentStore.append({
            id: node.parentLabel + '...',
            label: '。。。',
            parentLabel: node.parentLabel,
            fullData: node.fullData,
            children: [],
            index: node.index + this.treePerPage,
            isMore: true
          }, node.parentNode)
        }
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
              datasourceTreeData.push({id: data[i], label: data[i], children: [], fullData: data})
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
            var pagerLen = len > this.treePerPage ? this.treePerPage : len
            for (var k = 0; k < pagerLen; k++) {
              subData.push({
                id: node.label + '.' + data[k],
                label: data[k]
              })
            }
            if (pagerLen < len) {
              subData.push({
                id: node.label + '...',
                label: '。。。',
                children: [],
                parentNode: node,
                parentLabel: node.label,
                fullData: data,
                index: this.treePerPage,
                isMore: true
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
          this.$store.state.datasource.currentShowTableData = tableData[k]
          break
        }
      }
      this.activeName = 'first'
      this.loadTableExt({tableName: database + '.' + tableName, project: this.project}).then((res) => {
        handleSuccess(res, (data) => {
          _this.extendData = data
          for (var s = 0, len = _this.extendData.columns_stats && _this.extendData.columns_stats.length || 0; s < len; s++) {
            _this.extendData.columns_stats[s].column = this.tableData.columns[s].name
          }
          _this.extendData.last_modified = _this.extendData.last_modified ? transToGmtTime(_this.extendData.last_modified, _this) : null
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
            this.getKafkaTableDetail({tableName: this.currentStreamingTable, project: this.project}).then((res) => {
              handleSuccess(res, (data) => {
                this.currentStreamingTableData = data[0] || null
              })
            })
            this.getStreamingConfig({tableName: this.currentStreamingTable, project: this.project}).then((res) => {
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
      this.tableColumnsByFilter = this.tableData.columns
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
            this.$set(this.loadResult, 'running', data['result.running'])
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
      this.saveKafka({
        kafkaConfig: JSON.stringify(data.kafkaMeta),
        streamingConfig: JSON.stringify(data.streamingMeta),
        project: this.project,
        tableData: JSON.stringify(tableData)
      }).then((res) => {
        handleSuccess(res, () => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.saveSuccess')
          })
          this.saveSampleData({ tableName: data.database + '.' + data.tableName, sampleData: data.sampleData, project: this.project })
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
    },
    getProjectIdByName (pname) {
      var projectList = this.$store.state.project.allProject
      var len = projectList && projectList.length || 0
      var projectId = ''
      for (var s = 0; s < len; s++) {
        if (projectList[s].name === pname) {
          projectId = projectList[s].uuid
        }
      }
      return projectId
    },
    hasSomeProjectPermission () {
      return hasPermission(this, this.getProjectIdByName(localStorage.getItem('selected_project')), permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    },
    hasProjectAdminPermission () {
      return hasPermission(this, this.getProjectIdByName(localStorage.getItem('selected_project')), permissions.ADMINISTRATION.mask)
    }
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  watch: {
    /* 'dialogTreeHiveDataIsReady' (value) {
      if (value !== undefined) {
        if (this.load_hive_dalog_visible === true) {
          this.dialogEmptyText = this.pageLoadHiveAllData.length > 0 ? '' : this.$t('dialogHiveTreeNoData')
          this.hiveData = this.pageLoadHiveAllData
        }
      }
    }, */
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
    if (isFireFox()) {
      // Scrollbar.init(document.querySelector('.sub_menu .el-tabs__content'))
    }
  },
  locales: {
    'en': {'dialogHiveTreeNoData': 'No data', 'dialogHiveTreeLoading': 'loading', 'load': 'Load', 'reload': 'Reload', 'samplingBtn': 'Sampling', 'sampling': 'Table Sampling', 'unload': 'Unload', 'loadhiveTables': 'Load Hive Table Metadata', 'selectLeftHiveTip': 'Please select tables from the left hive table tree', 'setScanRange': 'Table Sampling', 'filterInputTips': 'Please input the hive table name to filter', 'loadTableJobBeginTips': 'Collect job start running!You can go to Monitor page to watch the progress!', 'hasCollectJob': 'There has been a running collect job!You can go to Monitor page to watch the progress!', 'loadHiveTip': '<p style="font-weight: bold">HOW TO SYNC TABLE\'S METADATA</p><p class="ksd-mt-10"><span style="font-weight: bold">Select tables from hive tree: </span>click tables in the left hive tree and the maximum is 1,000 tables.</p><p><span style="font-weight: bold">Enter table name as \'database.table\': </span>if you don\'t need to take a look at tables, just enter table name as \'database.table\'; use comma to separate multiple tables\' name; use ENTER to close entering. The maximum is 1000 tables.</p>', 'access': 'Access', 'selectedHiveValidateFailText': 'Please enter table name as \'database.table\'.'},
    'zh-cn': {'dialogHiveTreeNoData': '暂无数据', 'dialogHiveTreeLoading': '加载中', 'load': '加载', 'reload': '重载', 'samplingBtn': '采样', 'sampling': '收集表信息', 'unload': '卸载', 'loadhiveTables': '加载Hive表元数据', 'selectLeftHiveTip': '请在左侧选择要加载的table', 'setScanRange': '表采样', 'filterInputTips': '请输入hive表名进行过滤', 'loadTableJobBeginTips': '采集开始，您可以到Monitor页面查看采样进度！', 'hasCollectJob': '已有一个收集作业正在进行中，您可以去Monitor页面查看进度!', 'loadHiveTip': '<p style="font-weight: bold">加载表元数据的方式</p><p class="ksd-mt-10">选择表：可以在左侧的Hive tree选择需要加载的表。每次最多可加载1000张表。</p><p>输入表：可以在右侧的加载框中直接输入‘database.table’，按回车键结束输入。输入多张表名时，请使用逗号分隔，每次最多可加载1000张表。</p>', 'access': '权限', 'selectedHiveValidateFailText': '请输入完整表名\'database.table\'。'}
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
  *{
    font-size: 12px;
  }
  .el-radio-group{
    font-size: 0;
  }
  .extendInfo {
    float: left;
    margin-top: 4px;
    p{
      font-size: 12px;
    }
  }
  .grid-content .el-select__tags {
    left: 0;
  }
  .el-button.sampling{
    // border-color: @popper-bg;
    background: transparent!important;
  }
  .el-button--danger{
    border-color: #4cb050;
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
		display: inline-block;
		position: relative;
		top:-8px;
    background: @grey-color;
    // label .el-radio-button__inner{
    //   color: @fff;
    //   border-color: @base-color;
    // }
    // label:nth-child(1) .el-radio-button__inner{
    //   background: @base-color;
    // }
    // label:nth-child(2) .el-radio-button__inner{
    //   background: @grey-color;
    // }
    .el-radio-button{
      .el-radio-button__inner{
        border-color: #7881aa;
      }
      .el-radio-button{
        &.is-active{
          .el-radio-button__inner{
            border: 1px solid @base-color;
          }
        }
      }
    }
    .ksd-ml-30{
      margin-left: 45px!important;
      margin-top: 30px!important;
      margin-bottom: 12px!important;
    }
	}

	.table_content{
    background: @tableBC!important;
		padding: 20px;
		position: absolute;
		left: 250px;
		top:-16px;
		right: 0;
		// min-height: 1000px;
		border-left: solid 1px #d1dbe5;
    background-color: #fff;
		.el-tabs__content{
			overflow-y: auto;
		}
	}
	.extendinfo_table {
    color:#d4d7e3;
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
			font-size: 12px;
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
  .el-button--info{
    border-color: green;
  }
  .el-button--info:hover{
    border-color: #4cb050;
  }
}
#datasource-table{
  .el-tabs__nav-scroll{
    margin-left: 0!important;
  }
}
.null_pic{
  width: 150px;
}
#data-source-search{
  .el-input__inner{
    border-color: #7881aa;
  }
}
.load_hive_dialog {
  .el-dialog__body{
    padding-top: 0;
    padding-left: 0;
  }
  .el-select{
    max-height: 232px;
  }
  .el-select__tags {
    max-height: 232px;
    max-width: 100%!important;
    overflow-y: auto;
    overflow-x: hidden;

  }
}
#datasource{
  .el-dialog--small{
    width: 50%;
  }
}
#datasource .rightBtns .el-button{
 padding:6px 15px;
}
</style>
