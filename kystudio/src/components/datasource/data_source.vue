<template>
	<div class="datasource" id="datasource">
    <div class="ksd_left_bar">
      <div v-if="isAdmin || hasProjectAdminPermission()" class="btn-group">
         <el-button @click.native="openLoadDataSourceDialog" type="primary" size="medium" style="width: 130px;" icon="el-icon-ksd-load">{{$t('kylinLang.common.dataSource')}}
         </el-button>
      </div>
      <div class="table-type" v-if="selectedProjectDatasource">
        <span v-if="selectedProjectDatasource == 0">Hive</span>
        <span v-if="selectedProjectDatasource == 8 || selectedProjectDatasource == 16">RDBMS</span>
        <span v-if="selectedProjectDatasource == 1">Kafka</span>
      </div>
      <div class="tree-list">
        <tree :treedata="hiveAssets" :empty-text="$t('dialogHiveTreeNoData')" :expandall='true'  maxLabelLen="20" :showfilter="false" :allowdrag="false" @nodeclick="clickTable" maxlevel="3"></tree>
      </div>
    </div>
      <div class="table_content" >
        <div class="ksd-null-pic-text" v-if="!tableData">
          <img  src="../../assets/img/no_data.png" />
          <p>{{$t('kylinLang.common.noData')}}</p>
        </div>
       <div class="extendInfo" v-if="tableData">
         <p class="table-title" :title="extendData.table_name">{{extendData.table_name|omit(50, '...')}}</p>
         <p class="table-modify-time" v-if="extendData.last_modified">{{$t('kylinLang.dataSource.lastModified')}} {{extendData.last_modified}}</p>
       </div>
       <div class="rightBtns" style="position:absolute;right:0;z-index:1;top:30px;right:16px;" v-show="tableData">
          <div>

            <kap-icon-button class="ksd-fright" v-if="isAdmin || hasProjectAdminPermission()" :useload="true" @click.native="unloadTable" ref="unloadBtn" plain>{{$t('unload')}}</kap-icon-button>
            <kap-icon-button class="sampling ksd-fright" v-if="tableData.source_type === 1" type="primary" :useload="true" @click.native="collectKafkaSampleDialogOpen" ref="kafkaSampleBtn" plain>{{$t('samplingBtn')}}</kap-icon-button>
            <kap-icon-button class="ksd-fright" v-if="tableData.source_type === 0|| tableData.source_type === 8 || tableData.source_type === 16" type="primary" :useload="true" @click.native="collectSampleDialogOpen" ref="sampleBtn" plain>{{$t('samplingBtn')}}</kap-icon-button>
            <kap-icon-button class="ksd-fright" v-if="(tableData.source_type === 0 || tableData.source_type === 8 || tableData.source_type === 16)&& (isAdmin || hasProjectAdminPermission())" type="primary" :useload="true" @click.native="reloadTableDialogVisible" ref="reloadBtn" plain>{{$t('reload')}}</kap-icon-button>

          </div>

      </div>
      <div class="clear"></div>
       <el-tabs v-model="activeName" class="ksd-mt-24 clear" v-if="tableData" id="datasource-table">
		    <el-tab-pane :label="$t('kylinLang.dataSource.columns')" name="first">
          <el-input id="data-source-search" style="width:200px;" size="medium" class="ksd-mb-10 ksd-fright"
            :placeholder="$t('kylinLang.common.pleaseFilter')"
            prefix-icon="el-icon-search"
            v-model="filterColumn"
            @input="filterColumnChange">
          </el-input>
  	    	  <el-table
  			    :data="tableColumnsFilterCC"
  			    border
            v-if="activeName==='first'"
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
  			      width="170"
  			      prop="datatype"
              sortable
  			      :label="$t('kylinLang.dataSource.dataType')">
  			    </el-table-column>
  			    <el-table-column
  			      width="160"
              sortable
              prop="cardinality"
  			      :label="$t('kylinLang.dataSource.cardinality')">
  			    </el-table-column>
  			    <el-table-column
  			      prop="comment"
  			      :label="$t('kylinLang.dataSource.comment')">
  			    </el-table-column>
  			  </el-table>
		    </el-tab-pane>
		    <el-tab-pane :label="$t('kylinLang.dataSource.extendInfo')" name="second" >
		    	<table class="ksd-table" v-if="extendData.data_source_properties">
		    		<tr>
					<th>TABLE NAME</th>
		    			<td>{{tableData.name}}</td>
		    		</tr>
                    <tr>
					<th>HIVE DATABASE</th>
		    			<td>{{tableData.database}}</td>
		    		</tr>
		    		<tr>
					<th>TABLE TYPE</th>
		    			<td>{{tableData.table_type}}</td>
		    		</tr>
		    		<tr>
					<th>SNAPSHOT TIME</th>
		    			<td>{{tableData.database}}</td>
		    		</tr>
		    		<tr>
					<th>LOCATION</th>
		    			<td>{{extendData.data_source_properties.location}}</td>
		    		</tr>
		    		<tr>
					<th>INPUT FORMAT</th>
		    			<td>{{extendData.data_source_properties.hive_inputFormat}}</td>
		    		</tr>
		    		<tr>
					<th>OUT FORMAT</th>
		    			<td>{{extendData.data_source_properties.hive_outputFormat}}</td>
		    		</tr>
		    		<tr>
					<th>OWNER</th>
		    			<td>{{extendData.data_source_properties.owner}}</td>
		    		</tr>
		    		<tr>
					<th>TOTAL FILE NUMBER</th>
		    			<td>{{extendData.data_source_properties.total_file_number}}</td>
		    		</tr>

		    		<tr>
					<th>TOTAL FILE SIZE</th>
		    			<td>{{extendData.data_source_properties.total_file_size}}</td>
		    		</tr>
		    		<tr>
					<th>PARTITIONED</th>
		    			<td>{{extendData.data_source_properties.partition_column!==''}}</td>
		    		</tr>
		    		<tr>
					<th>PARTITION COLUMNS</th>
		    			<td>{{extendData.data_source_properties.partition_column}}</td>
		    		</tr>
		    	</table>
		    </el-tab-pane>
		    <el-tab-pane :label="$t('kylinLang.dataSource.statistics')" name="third" v-if="tableData.source_type === 0|| tableData.source_type === 8 || tableData.source_type === 16">
          <p class="ksd-mt-4 ksd-mb-10 ksd-right">{{$t('kylinLang.dataSource.totalRow')}} {{extendData.total_rows}}</p>
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
              sortable
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
             <el-table-column
            show-overflow-tooltip
              width="120"
              prop="exceed_precision_max_length_value"
              :label="$t('kylinLang.dataSource.exceedPrecisionMaxLengthValue')">
            </el-table-column>
  			  </el-table>

		    </el-tab-pane>
		    <el-tab-pane :label="$t('kylinLang.dataSource.sampleData')" name="fourth">
          <el-select filterable clearable
            class="filter-input"
            v-model="sampleTableFilter"
            :placeholder="$t('kylinLang.common.pleaseFilter')">
            <el-option
              v-for="label in sampleData[0]"
              :key="label"
              :label="label"
              :value="label">
            </el-option>
          </el-select>
		      <el-table :data="filtedSampleData.slice(1)" tooltip-effect="dark" border style="width: 100%">
  			    <el-table-column v-for="(val,index) in filtedSampleData[0]" :key="index"
  			      :prop="''+index"
              show-overflow-tooltip
              :width="15* (filtedSampleData[0][index] && filtedSampleData[0][index].length || 0) + 20"
  			      :label="filtedSampleData[0][index]">
  			    </el-table-column>
			    </el-table>
		    </el-tab-pane>
        <el-tab-pane :label="$t('kylinLang.dataSource.kafkaCluster')" name="fifth" v-if="tableData.source_type === 1">
            <el-button type="primary" plain size="medium" icon="edit" @click="openEditKafkaForm" class="ksd-fright">{{$t('kylinLang.common.edit')}}</el-button>
            <view_kafka  ref="addkafkaForm" v-on:validSuccess="kafkaValidSuccess" :streamingData="currentStreamingTableData"  :tableName="currentStreamingTable" ></view_kafka>
        </el-tab-pane>
        <el-tab-pane :label="$t('access')" name="sixth">
           <access :tableData="tableData" v-if="activeName==='sixth'"></access>
        </el-tab-pane>
      </el-tabs>
      </div>
      

      <el-dialog class="load-datasource-dialog" :title="dataSourceLoadDialogTitle" width="720px" :visible.sync="loadDataSourceVisible" :close-on-press-escape="false" :close-on-click-modal="false">
        <div v-if="activeLoadFormIndex===-1">
          <p class="ksd-center ksd-mt-40 select-title">{{$t('dataSourceTypeCheckTip')}}{{initDefaultCheck}}</p>
         
          <ul class="ksd-center datasource-type">
            <li class="type-hive" @click="checkDataSourceType('0')">
              <div :class="{active:currentUserCheckType=== '0'}"></div>
              <p>Hive</p>
            </li>
            <li class="type-rdbms" @click="checkDataSourceType('16')">
              <div :class="{active:currentUserCheckType==='16' || currentUserCheckType==='8'}"></div>
              <p>RDBMS</p>
            </li>
            <li class="type-kafka" @click="checkDataSourceType('1')">
              <div :class="{active:currentUserCheckType==='1'}"></div>
              <p>Kafka</p>
            </li>
          </ul>
           <p class="ksd-center ksd-mt-20 checksource-warn-msg">{{$t('singleSourceTip')}}</p>
        </div>
        <div v-if="activeLoadFormIndex===0">
          <div style="display:flex;">
           <div class="left-tree-part">
             <div class="dialog_tree_box">
               <tree
               @lazyload="loadChildNode"
               :multiple="true"
               @nodeclick="clickHiveTable"
               :lazy="true"
               :treedata="hiveData"
               :emptyText="dialogEmptyText"
               maxlevel="3"
               ref="subtree"
               :maxLabelLen="20"
               :showfilter="false"
               :allowdrag="false" ></tree>
              </div>
           </div>
           <div class="right-tree-part">
             <div class="tree_check_content ksd-mt-20">
                <arealabel :validateRegex="regex.validateHive" @validateFail="selectedHiveValidateFail" @refreshData="refreshHiveData" splitChar=","  :selectedlabels="selectTablesNames" :allowcreate='true' placeholder=" " @removeTag="removeSelectedHive"  :datamap="{label: 'label', value: 'value'}"></arealabel>
            <div class="ksd-mt-22 ksd-extend-tips" v-html="$store.state.system.sourceDefault === '0' ? $t('loadHiveTip') : $t('loadTip')"></div>
            <div class="ksd-mt-20">
              <slider @changeBar="changeBar" :show="load_hive_dalog_visible" class="ksd-mr-20 ksd-mb-20">
                <span slot="checkLabel">{{$t('sampling')}}</span>
                 <span slot="tipLabel">
                    <common-tip placement="right" :content="$t('kylinLang.dataSource.collectStatice')" >
                       <i class="el-icon-ksd-what"></i>
                    </common-tip>
                 </span>
              </slider>
            </div>
            </div> 
           </div>
         </div>
        </div>

        <div  v-if="activeLoadFormIndex===1" style="padding:20px;">
          <create_kafka  ref="kafkaForm" v-on:validSuccess="kafkaValidSuccess" :show="kafkaFormVisible"></create_kafka>
        </div>

        <div slot="footer" class="dialog-footer">
        <el-button @click="loadDataSourceVisible=false;activeLoadFormIndex===-1" v-if="activeLoadFormIndex===-1">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="activeLoadFormIndex=-1" v-if="!selectedProjectDatasource && activeLoadFormIndex!==-1">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button type="primary" plain @click="nextLoadForm"  v-if="activeLoadFormIndex===-1">{{$t('kylinLang.common.next')}}</el-button>
        <!-- <el-button type="primary" plain @click="saveDataSource" :loading="loadHiveLoad" v-if="activeLoadFormIndex!==-1">Save</el-button> -->
         

         <el-button type="primary" plain @click="loadHiveList" :loading="loadHiveLoad" v-if="(currentUserCheckType==='0' || currentUserCheckType==='16'  || currentUserCheckType==='8')&& activeLoadFormIndex!==-1">{{$t('kylinLang.common.sync')}}</el-button>
         <el-button type="primary" plain @click="checkKafkaForm" :loading="kafkaLoading" v-if="currentUserCheckType==='1' && activeLoadFormIndex!==-1">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
      </el-dialog>
     <!-- reload table dialog -->
     <el-dialog :title="$t('reload')" width="720px" :visible.sync="scanRatioDialogVisible" @close="cancelReloadTable" :close-on-press-escape="false" :close-on-click-modal="false">
        <el-row :gutter="20">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content ksd-mt-20">
              <div class="ksd-mt-20 ksd-mb-40">
                 <slider @changeBar="changeBar" :show="scanRatioDialogVisible">
                   <span slot="checkLabel">
                    {{$t('sampling')}}
                   </span>
                   <span slot="tipLabel">
                     <common-tip :content="$t('kylinLang.dataSource.collectStatice')" >
                       <i class="el-icon-ksd-what"></i>
                    </common-tip>
                   </span>
                 </slider>
              </div>
              </div>
            </div>
          </el-col>
        </el-row>
        <div slot="footer" class="dialog-footer">
          <el-button @click="cancelReloadTable">{{$t('kylinLang.common.cancel')}}</el-button>
           <kap-icon-button  type="primary" plain  :useload="true" @click.native="reloadTable" ref="reloadBtnConfirm">{{$t('kylinLang.common.submit')}}</kap-icon-button>
          <!-- <el-button type="primary" :loading="loadHiveLoad" @click="reloadTable">{{$t('kylinLang.common.submit')}}</el-button> -->
        </div>
      </el-dialog>
      <!-- 单个采样dialog -->
      <el-dialog :title="$t('setScanRange')" :visible.sync="scanSampleRatioDialogVisible" @close="cancelLoadSample" :close-on-press-escape="false" :close-on-click-modal="false" width="720px">
        <span slot="title">{{$t('setScanRange')}} <common-tip placement="right" :content="$t('kylinLang.dataSource.collectStatice')" >
                 <i class="el-icon-ksd-what"></i></common-tip></span>
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
          <el-button type="primary" plain @click="loadSample" :disabled="tableStaticsRange === 0">{{$t('kylinLang.common.submit')}}</el-button>
        </div>
      </el-dialog>

     <el-dialog title="Load Kafka Topic" :append-to-body="true" :visible.sync="editKafkaFormVisible" top="10%" :close-on-press-escape="false" :close-on-click-modal="false">
        <edit_kafka  ref="kafkaFormEdit" v-if="editKafkaFormVisible"  v-on:validEditSuccess="kafkaEditValidSuccess" :show="editKafkaFormVisible" :streamingData="currentStreamingTableData" :streamingConfig="currentStreamingConfig"  :tableName="currentStreamingTable" ></edit_kafka>
        <span slot="footer" class="dialog-footer">
          <el-button @click="editKafkaFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" plain @click="checkKafkaFormEdit" :loading="kafkaLoading">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>
      <el-dialog  :title="$t('kylinLang.common.tip')" width="720px"
        :visible.sync="loadResultVisible" :close-on-press-escape="false" :close-on-click-modal="false">
         <el-alert v-for=" su in loadResult.success" :key="su"
            :title="currentAction + $t('kylinLang.common.success') + ' ! ' + '['+su+']'"
            type="success"
            :closable="false"
            class="ksd-mt-10"
            show-icon>
          </el-alert>
            <el-alert v-for=" fa in loadResult.fail" :key="fa"
            :title="currentAction + $t('kylinLang.common.fail') + ' ! ' + '['+fa+']'"
            type="error"
            :closable="false"
            class="ksd-mt-10"
            show-icon>
          </el-alert>
           <el-alert v-for=" fa in loadResult.running" :key="fa"
            :title=" currentAction + $t('kylinLang.common.fail') + ' ! ' + $t('kylinLang.common.running') + '['+fa+']'"
            type="error"
            :closable="false"
            class="ksd-mt-10"
            show-icon>
          </el-alert>
        <span slot="footer" class="dialog-footer">
          <el-button type="primary" plain @click="loadResultVisible = false">{{$t('kylinLang.common.close')}}</el-button>
        </span>
      </el-dialog>
    </div>

</template>
<script>
import { mapActions, mapGetters } from 'vuex'
import { handleSuccess, handleError, hasRole, kapWarn, transToGmtTime, hasPermission } from '../../util/business'
import { permissions } from '../../config'
import { changeDataAxis, objectArraySort, objectClone } from '../../util/index'
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
      loadDataSourceVisible: false,
      activeLoadFormIndex: -1,
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
      },
      kafkaLoading: false,
      currentUserCheckType: '',
      stObj: null,
      sampleTableFilter: '',
      regex: {
        validateHive: /^\s*;?(\w+\.\w+)\s*(,\s*\w+\.\w+)*;?\s*$/
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
      getStreamingConfig: 'LOAD_STREAMING_CONFIG',
      updateProject: 'UPDATE_PROJECT',
      loadAllProjects: 'LOAD_ALL_PROJECT'
    }),
    updataProjectDatasource (cb) {
      if (!this.project) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        return
      }
      if (this.selectedProjectDatasource) {
        cb()
        return
      }
      var projectInstance = objectClone(this.currentProjectData)
      projectInstance.override_kylin_properties['kylin.source.default'] = this.currentUserCheckType
      this.updateProject({name: this.project, desc: JSON.stringify(projectInstance)}).then((result) => {
        this.loadAllProjects()
        cb()
      }, (res) => {
        handleError(res)
      })
    },
    checkDataSourceType (type) {
      // 如果项目有单独的数据源类型的配置就不让点击
      if (this.selectedProjectDatasource) {
        return
      }
      this.currentUserCheckType = type
    },
    nextLoadForm () {
      this.updataProjectDatasource(() => {
        if (this.currentUserCheckType === '0' || this.currentUserCheckType === '16' || this.currentUserCheckType === '8') {
          this.activeLoadFormIndex = 0
        } else {
          this.activeLoadFormIndex = 1
        }
        this.selectTables = []
        this.selectTablesNames = []
      })
    },
    openLoadDataSourceDialog () {
      if (!this.project) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        return
      }
      this.openCollectRange = false
      this.tableStaticsRange = 0
      this.loadDataSourceVisible = true
      this.activeLoadFormIndex = -1
      this.selectTables = []
      this.selectTablesNames = []
      this.$nextTick(() => {
        if (this.selectedProjectDatasource) {
          this.nextLoadForm()
        } else {
          this.currentUserCheckType = this.globalDefaultDatasource
        }
      })
    },
    // hasPermissionOfProject (project) {
    //   var projectList = this.$store.state.project.allProject
    //   var len = projectList && projectList.length || 0
    //   var projectId = ''
    //   for (var s = 0; s < len; s++) {
    //     if (projectList[s].name === project) {
    //       projectId = projectList[s].uuid
    //     }
    //   }
    //   return hasPermission(this, projectId, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask, permissions.OPERATION.mask)
    // },
    filterColumnChange (filterVal) {
      if (filterVal && this.tableData.columns) {
        this.tableColumnsByFilter = this.tableData.columns.filter((col) => {
          return col.name.toUpperCase().indexOf(filterVal.toUpperCase()) >= 0
        })
      } else {
        this.tableColumnsByFilter = this.tableData.columns || []
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
    openEditKafkaForm () {
      if (!this.project) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        return
      }
      this.kafkaLoading = false
      this.editKafkaFormVisible = true
    },
    collectKafkaSampleDialogOpen () {
      var tableName = this.tableData.database + '.' + this.tableData.name
      this.collectKafkaSampleData({tableName: tableName, project: this.project}).then((res) => {
        this.$message({message: this.$t('kylinLang.common.submitSuccess'), type: 'success'})
        this.$refs.kafkaSampleBtn.loading = false
      }, (res) => {
        this.$refs.kafkaSampleBtn.loading = false
        handleError(res)
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
      return this.loadHiveInProject({
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
        this.$refs.subtree && this.$refs.subtree.cancelCheckedAll()
      }, (res) => {
        btn.loading = false
        handleError(res)
      })
    },
    // reload table meta data
    reloadTableDialogVisible () {
      let tableName = this.tableData.database + '.' + this.tableData.name
      this.tableStaticsRange = 0
      this.openCollectRange = false
      this.scanRatioDialogVisible = false
      this.currentAction = this.$t('load')
      this.checkTableHasJob(tableName, () => {
        this.$refs.reloadBtn.loading = false
        this.$message(this.$t('kylinLang.dataSource.dataSourceHasJob'))
        return
      }, () => {
        this.scanRatioDialogVisible = true
      })
    },
    cancelReloadTable () {
      this.scanRatioDialogVisible = false
      this.$refs.reloadBtn.loading = false
      this.$refs.reloadBtnConfirm.loading = false
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
      this.loadHivesAction([tableName], this.$refs.reloadBtnConfirm).then(() => {
        this.loadHiveTree().then(() => {
          this.showTableDetail(this.tableData.database, this.tableData.name)
        })
      })
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
      return this.loadDataSourceByProject({project: this.project, isExt: true}).then((response) => {
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
              childObj.tags = databaseData[s][f].source_type === 1 ? ['S'] : null
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
            // let iTop = this.$el.querySelectorAll('.ksd_left_bar')[0].offsetTop
            let iHeight = window.innerHeight - 66 - 48 - 33 - 33 - 36 - 70
            this.$el.querySelector('.tree-list').style.height = iHeight + 'px'
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
              label: 'Hive Table',
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
        var addData = node.fullData.slice(0, node.index + this.treePerPage)
        var moreNodes = []
        for (var k = 0; k < addData.length; k++) {
          moreNodes.push({
            id: node.parentLabel + '.' + addData[k],
            label: addData[k]
          })
        }
        var renderChildrens = objectClone(moreNodes)
        if (node.index + this.treePerPage < node.fullData.length) {
          renderChildrens.push({
            id: node.parentLabel + '...',
            label: '。。。',
            parentLabel: node.parentLabel,
            fullData: node.fullData,
            children: [],
            index: node.index + this.treePerPage,
            isMore: true
          })
        }
        node.parentNode.children = renderChildrens
      }
    },
    loadChildNode (node, resolve) {
      if (node.level === 0) {
        return resolve([{label: this.selectedProjectDatasource === '0' ? 'Hive Table' : 'Table'}])
      } else if (node.level === 1) {
        this.loadDatabase(this.$store.state.project.selected_project).then((res) => {
          handleSuccess(res, (data) => {
            var datasourceTreeData = []
            for (var i = 0; i < data.length; i++) {
              datasourceTreeData.push({id: data[i], label: data[i], children: [], fullData: data})
            }
            resolve(datasourceTreeData)
          })
        }, (res) => {
          node.loading = false
          handleError(res)
        })
      } else if (node.level === 2) {
        var subData = []
        this.loadTablesByDatabse({database: node.label, project: this.$store.state.project.selected_project}).then((res) => {
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
          node.loading = false
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
        this.tableData = ''
        this.$nextTick(() => {
          this.showTableDetail(database, tableName)
        })
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
            _this.extendData.columns_stats[s].column = _this.tableData.columns[s].name
            _this.$set(_this.tableData.columns[s], 'cardinality', _this.extendData.columns_stats[s].cardinality)
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
            return
          }
        })
      }, (res) => {
        handleError(res)
      })
      this.tableColumnsByFilter = this.tableData.columns
    },
    loadHiveList () {
      // 先更新project的默认数据源
      if (this.selectTablesNames.length > 0) {
        this.currentAction = this.$t('load')
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
          this.loadDataSourceVisible = false
          this.loadResultVisible = true
          this.selectTables = []
          this.selectTablesNames = []
          this.loadHiveLoad = false
        }, (res) => {
          this.loadHiveLoad = false
          handleError(res)
          this.loadDataSourceVisible = false
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
      // 先更新project的默认数据源
      this.kafkaLoading = true
      let columns = []
      data.columnList.forEach(function (column, $index) {
        if (column.checked === 'Y') {
          let columnInstance = {
            id: ++$index,
            name: column.name,
            datatype: column.type,
            comment: /[|]/.test(column.comment) ? column.comment : '' // 不是嵌套结构的就不传该内容
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
          this.kafkaLoading = false
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.saveSuccess')
          })
          this.saveSampleData({ tableName: data.database + '.' + data.tableName, sampleData: data.sampleData, project: this.project })
          this.loadDataSourceVisible = false
          this.loadHiveTree()
          // this.showTableDetail(data.database, data.tableName)
        })
      }, (res) => {
        this.kafkaLoading = false
        handleError(res)
      })
    },
    kafkaEditValidSuccess: function (data) {
      this.kafkaLoading = true
      this.updateKafka({
        kafkaConfig: JSON.stringify(data.kafkaMeta),
        streamingConfig: JSON.stringify(data.streamingMeta),
        project: this.project,
        tableData: ''
      }).then((res) => {
        this.kafkaLoading = false
        // handleSuccess(res, (data) => {
        this.$message({message: this.$t('kylinLang.common.updateSuccess'), type: 'success'})
        this.editKafkaFormVisible = false
        this.loadHiveTree()
        this.showTableDetail(data.kafkaMeta.name.split('.')[0], data.kafkaMeta.name.split('.')[1])
        this.activeName = 'fifth'
        // })
      }, (res) => {
        this.kafkaLoading = false
        this.editKafkaFormVisible = false
        this.showTableDetail(data.kafkaMeta.name.split('.')[0], data.kafkaMeta.name.split('.')[1])
        handleError(res)
      })
    },
    hasProjectAdminPermission () {
      return hasPermission(this, permissions.ADMINISTRATION.mask)
    }
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    tableColumnsFilterCC () {
      return this.tableColumnsByFilter.filter((column) => {
        return !column.cc_expr
      })
    },
    ...mapGetters([
      'selectedProjectDatasource',
      'globalDefaultDatasource',
      'currentProjectData'
    ]),
    dataSourceLoadDialogTitle () {
      if (this.activeLoadFormIndex === -1) {
        return this.$t('newDataSource')
      } else if (this.currentUserCheckType === '0') {
        return this.$t('loadhiveTables')
      } else if (this.currentUserCheckType === '16' || this.currentUserCheckType === '8') {
        return this.$t('loadTables')
      } else if (this.currentUserCheckType === '1') {
        return this.$t('loadKafkaTopic')
      }
    },
    initDefaultCheck () {
      this.currentUserCheckType = this.selectedProjectDatasource || this.globalDefaultDatasource
      return ''
    },
    /**
     * Computed: filtedSampleData
     *
     * @desc SampleData's header can be filted. Remove the columes which user don't input.
     * @link https://github.com/Kyligence/KAP/issues/5810
     */
    filtedSampleData () {
      const tableHeaders = this.sampleData[0] || []
      const inputHeaders = this.sampleTableFilter.split(',').map(item => item.trim()).filter(item => item)
      // const filtedHeadStatus = tableHeaders.map(tableHeader => inputHeaders.some(inputHeader => tableHeader.toUpperCase().includes(inputHeader.toUpperCase())))
      const filtedHeadStatus = tableHeaders.map(tableHeader => inputHeaders.some(inputHeader => tableHeader.toUpperCase() === inputHeader.toUpperCase()))
      const filtedHeaders = tableHeaders.filter((tableHeader, index) => filtedHeadStatus[index])

      let filtedSampleData = [filtedHeaders]

      if (filtedHeaders.length > 0 && filtedHeadStatus.some(status => status)) {
        for (let i = 1; i < this.sampleData.length; i++) {
          const sampleDataValues = this.sampleData[i]
          const filtedSampleDataValues = sampleDataValues.filter((value, index) => filtedHeadStatus[index])

          filtedSampleData.push(filtedSampleDataValues)
        }
      } else if (!this.sampleTableFilter) {
        filtedSampleData = this.sampleData
      }

      return filtedSampleData
    }
  },
  mounted () {
  },
  locales: {
    'en': {'dialogHiveTreeNoData': 'Please click data source to load source tables', 'dialogHiveTreeLoading': 'loading', 'load': 'Load ', 'reload': 'Reload', 'samplingBtn': 'Sampling', 'sampling': 'Table Sampling', 'unload': 'Unload ', 'loadhiveTables': 'Load Hive Table Metadata', 'loadTables': 'Load Table Metadata', 'setScanRange': 'Table Sampling', 'filterInputTips': 'Please input the hive table name to filter', 'loadTableJobBeginTips': 'Collect job start running!You can go to Monitor page to watch the progress!', 'hasCollectJob': 'There has been a running collect job!You can go to Monitor page to watch the progress!', 'loadHiveTip': '<p style="font-weight: bold">HOW TO SYNC TABLE\'S METADATA</p><p class="ksd-mt-10"><span style="font-weight: bold">Select tables from source tree: </span>click tables in the left hive tree and the maximum is 1,000 tables.</p><p><span style="font-weight: bold">Enter table name as \'database.table\': </span>if you don\'t need to take a look at tables, just enter table name as \'database.table\'; use comma to separate multiple tables\' name; use ENTER to close entering. The maximum is 1000 tables.</p>', 'loadTip': '<p style="font-weight: bold">HOW TO SYNC TABLE\'S METADATA</p><p class="ksd-mt-10"><span style="font-weight: bold">Select tables from tree: </span>click tables in the left tree and the maximum is 1,000 tables.</p><p><span style="font-weight: bold">Enter table name as \'database.table\': </span>if you don\'t need to take a look at tables, just enter table name as \'database.table\'; use comma to separate multiple tables\' name; use ENTER to close entering. The maximum is 1000 tables.</p>', 'access': 'Access', 'selectedHiveValidateFailText': 'Please enter table name as \'database.table\'.', newDataSource: 'New Data Source', 'loadKafkaTopic': 'Load Kafka Topic', dataSourceTypeCheckTip: 'Please Select Data Source Type', singleSourceTip: 'You can choose one type of data source within the project and it cannot be modified after selection.'},
    'zh-cn': {'dialogHiveTreeNoData': '请点击数据源来加载源表', 'dialogHiveTreeLoading': '加载中', 'load': '加载', 'reload': '重载', 'samplingBtn': '采样', 'sampling': '表采样', 'unload': '卸载', 'loadhiveTables': '加载 Hive 表元数据', 'loadTables': '加载表元数据', 'setScanRange': '表采样', 'filterInputTips': '请输入hive表名进行过滤', 'loadTableJobBeginTips': '采集开始，您可以到 Monitor 页面查看采样进度！', 'hasCollectJob': '已有一个收集作业正在进行中，您可以去Monitor页面查看进度!', 'loadHiveTip': '<p style="font-weight: bold">加载表元数据的方式</p><p class="ksd-mt-10">选择表：可以在左侧的元数据树选择需要加载的表。每次最多可加载1000张表。</p><p>输入表：可以在右侧的加载框中直接输入‘database.table’，按回车键结束输入。输入多张表名时，请使用逗号分隔，每次最多可加载1000张表。</p>', 'loadTip': '<p style="font-weight: bold">加载表元数据的方式</p><p class="ksd-mt-10">选择表：可以在左侧的 tree 选择需要加载的表。每次最多可加载 1000 张表。</p><p>输入表：可以在右侧的加载框中直接输入‘database.table’，按回车键结束输入。输入多张表名时，请使用逗号分隔，每次最多可加载 1000 张表。</p>', 'access': '权限', 'selectedHiveValidateFailText': '请输入完整表名\'database.table\'。', newDataSource: '新数据源', 'loadKafkaTopic': '设置 Kafka 主题', dataSourceTypeCheckTip: '请选择数据源类型', singleSourceTip: '每个项目支持一种数据源类型，选定后将无法修改。'}
  }
}
</script>
<style lang="less" >
@import '../../assets/styles/variables.less';
.load-datasource-dialog{
  .el-dialog__body{
      padding: 0;
      .left-tree-part{
        width:218px;
        max-height:600px;
        overflow-y:auto;
        overflow-x:hidden;
      }
      .right-tree-part{
        border-left: solid 1px @line-border-color;
        flex:1;
        padding: 0 20px 20px;
      }
    }
    .el-select__tags{
      max-width: 100%!important;
      max-height: 320px;
    }
    .el-tabs__content{
      overflow-y: auto;
      overflow-x: auto;
    }
  .select-title{
    color:@text-title-color;
    font-weight: @font-medium;
  }
  .checksource-warn-msg {
    margin-bottom: 240px;
  }
  .datasource-type{
    margin-top: 40px;
    li{
      display: inline-block;
      margin-right: 15px;
      margin-left: 15px;
      cursor: pointer;
      div {
        width: 80px;
        height: 80px;
        background-color: @grey-4;
        background-repeat:no-repeat;
        background-position: center;
        margin-bottom: 10px;
        background-size: 50%;
      }
      &.type-hive {
        div{
          background-image: url('../../assets/img/datasource/hive_blue.png');
          background-size: 60%;
          &.active{
            background-color: @base-color;
            background-image: url('../../assets/img/datasource/hive_white.png');
          }
        }
      }
      &.type-rdbms {
        div{
          background-image: url('../../assets/img/datasource/rdbms_blue.png');
          &.active{
            background-color: @base-color;
            background-image: url('../../assets/img/datasource/rdbms_white.png');
          }
        }
      }
      &.type-kafka {
        div{
          background-image: url('../../assets/img/datasource/kafka_blue.png');
          &.active{
            background-color: @base-color;
            background-image: url('../../assets/img/datasource/kafka_white.png');
          }
        }
      }
    }
  }
}
.datasource{
  display: flex;
  .ksd-extend-tips {
    color:@text-normal-color;
  }

  .table-title{
    font-size:16px;
    color:@text-title-color;
  }
  .table-modify-time{
    color:@text-title-color;
    margin-top: 6px;
  }
  .extendInfo {
    float: left;
    margin-top: 4px;
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
	.ksd_left_bar {
		.tree-list {
      padding: 0 20px;
      margin: 20px 0;
    }
    .btn-group {
      text-align: center;
      width: 100%;
      >.el-button {
        margin-top: 18px;
        margin-bottom: 18px;
        // &:first-child {
        //   margin-left: 46px;
        // }
      }
      .usedbutton {
        background: @base-color-1;
        color:@fff;
      }
    }
    .table-type {
      border-bottom: solid 1px @line-border-color;
      height: 36px;
      line-height: 36px;
      font-size: 14px;
      padding-left: 10px;
      background-color: @line-border-color;
      color: @text-title-color;
    }
	}
	.table_content{
		padding: 20px;
    flex:1;
    width: 100%;
    overflow: hidden;
		// position: absolute;
		// left: 250px;
		// top:0;
		// right: 0;
		// border-left: solid 1px #d1dbe5;
  //   background-color: #fff;
		.el-tabs__content{
			overflow-y: auto;
		}
  }
  .filter-input {
    width: 200px;
    float: right;
    margin-bottom: 10px;
  }
}
#datasource .rightBtns {
  .el-button{
    padding:6px 15px;
  }
  .el-button+.el-button {
    margin-right: 10px;
    margin-left: 0px;
  }
  p {
    font-size:12px;
    margin-top:10px;
    text-align:right;
    padding-right:4px;
    display: inline-block;
  }
}
</style>
