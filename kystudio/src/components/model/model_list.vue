<template>
	<div class="modelist_box">
    <div v-if="modelsList&&modelsList.length || !(modelsList && modelsList.length) && showSearchResult">
      <p class="ksd-fright ksd-mb-14 ksd-mt-26" >
        <i class="el-icon-ksd-card model-list-mode" @click="changeGridModal('card')" :class="{active: viewModal==='card'}"></i>&nbsp;
        <i class="el-icon-ksd-list model-list-mode" @click="changeGridModal('list')"  :class="{active: viewModal!=='card'}"></i>
      </p>
      <div  class="ksd-mb-14 ksd-fright ksd-mt-20 ksd-mr-20">
        <el-input :placeholder="$t('kylinLang.common.pleaseFilterByModelName')" size="medium" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" v-model="serarchChar"  @input="searchModels" class="show-search-btn" >
        </el-input>
      </div>
      <el-button icon="el-icon-plus" type="primary" size="medium" plain class="ksd-mb-14 ksd-mt-20" id="addModel" v-visible="isAdmin || hasPermissionOfProject()" @click="addModel"><span>{{$t('kylinLang.common.model')}}</span></el-button>

		<el-row :gutter="20" v-if="(viewModal==='card') && (modelsList&&modelsList.length)">
		  <el-col :span="8"  v-for="(o, index) in modelsList" :key="o.uuid" class="card-model-list">
		    <el-card :body-style="{ padding: '0px'}" style="height:100%" :class="{'is_draft': o.is_draft}">
		      <p style="font-size: 12px;padding-left: 10px;" class="title">{{$t('kylinLang.model.modifiedGrid')}} {{ o.gmtTime }}</p>
		      <div style="padding: 20px;">
		        <h2>
            <el-tooltip class="item" effect="dark" :content="o.name" placement="top">
              <span @click="viewModel(o)" class="card-model-title" ><span v-show="o.is_draft" class="draft-model ksd-mr-10">[draft]</span>{{o.name}}</span>
            </el-tooltip>
           <common-tip :content="o.diagnose&&o.diagnose.messages.join('<br/>')" v-if="!o.is_draft && o.diagnose &&o.diagnose.heathStatus!=='RUNNING' &&o.diagnose.heathStatus!=='ERROR'" >
             <i  :style="{color:modelHealthStatus[o.diagnose.heathStatus].color}" :class="modelHealthStatus[o.diagnose.heathStatus].icon"></i></common-tip>

             <common-tip :content="o.diagnose&&o.diagnose.progress + '%'||'0%'"><el-progress :width="15" type="circle"  :stroke-width="2" :show-text="false" v-if="!o.is_draft&&o.diagnose&&o.diagnose.heathStatus==='RUNNING'" :percentage="o.diagnose&&o.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></common-tip>

<common-tip :content="o.diagnose&&o.diagnose.messages.join('<br/>')" v-if="!o.is_draft&&o.diagnose&&o.diagnose.heathStatus==='ERROR'"><el-progress :width="15" type="circle" status="exception" :stroke-width="2" :show-text="false"  :percentage="o.diagnose&&o.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></common-tip>
             </h2>
		        <div class="bottom-info clearfix">
		          <span class="owner" v-visible="o.owner" style="display:block">{{o.owner}}</span>
              <div class="action-btns" v-show="isAdmin || hasPermissionOfProject()">
                <common-tip :content="$t('kylinLang.common.edit')" class="ksd-ml-4"><i class="el-icon-ksd-table_edit" @click="handleEditModel(o.uuid)"></i></common-tip>
                <common-tip :content="$t('addCube')" v-if="!o.is_draft" class="ksd-ml-4"><i class="el-icon-circle-plus-outline" @click="handleAddCube(o.uuid)"></i></common-tip>
                <common-tip :content="$t('kylinLang.common.drop')"  v-if="o.is_draft" class="ksd-ml-4"><i class="el-icon-delete" @click="handleDropModel(o.uuid)"></i></common-tip>
                <common-tip :content="$t('kylinLang.common.moreActions')" v-show="!o.is_draft">
                  <el-dropdown @command="handleCommand" :id="o.name" trigger="click" class="ksd-ml-4" >
                    <span class="el-dropdown-link">
                      <i class="el-icon-ksd-table_others"></i>
                    </span>
                    <el-dropdown-menu slot="dropdown"  :uuid='o.uuid' >
                      <el-dropdown-item command="verify">{{$t('kylinLang.common.verifySql')}}</el-dropdown-item>
                      <!-- <el-dropdown-item command="cube" v-if="!o.is_draft">{{$t('addCube')}}</el-dropdown-item> -->
                      <!-- <el-dropdown-item command="edit">{{$t('kylinLang.common.edit')}}</el-dropdown-item> -->
                      <el-dropdown-item command="clone">{{$t('kylinLang.common.clone')}}</el-dropdown-item>
                      <el-dropdown-item command="stats">{{$t('kylinLang.common.check')}}</el-dropdown-item>
                      <el-dropdown-item command="drop">{{$t('kylinLang.common.drop')}}</el-dropdown-item>
                    </el-dropdown-menu>
                  </el-dropdown>
                </common-tip>
              </div>
		        </div>
		      </div>
		    </el-card>
		  </el-col>
		</el-row>

    <div class="ksd-center ksd-mt-30 ksd-mb-20" v-if="(viewModal==='card') && !(modelsList && modelsList.length) && showSearchResult">{{$t('kylinLang.common.noData')}}</div>


    <el-table v-if="(viewModal!=='card') && (modelsList&&modelsList.length || !(modelsList && modelsList.length) && showSearchResult)"
    :data="modelsList"
    border
    :row-class-name="showRowClass"
    tooltip-effect="dark"
    style="width: 100%">
    <el-table-column
    show-overflow-tooltip
      :label="$t('kylinLang.model.modelNameGrid')"
      width="280">
       <template slot-scope="scope">
        <div class="ksd-ml-20">
          <span v-show="scope.row.is_draft" class="draft-model ksd-mr-10">[draft]</span><span>{{scope.row.name}}</span>
          <common-tip  :content="scope.row.diagnose&&scope.row.diagnose.messages&&scope.row.diagnose.messages.join('<br/>')" v-if="!scope.row.is_draft && scope.row.diagnose && scope.row.diagnose.heathStatus!=='RUNNING'&& scope.row.diagnose.heathStatus!=='ERROR' && (scope.row.diagnose.progress===0 || scope.row.diagnose.progress===100)">
            <i class="el-icon-question" :name="modelHealthStatus[scope.row.diagnose.heathStatus].icon" :style="{color:modelHealthStatus[scope.row.diagnose.heathStatus].color}"></i>
          </common-tip>
          <common-tip :content="scope.row.diagnose&&scope.row.diagnose.progress + '%'||'0%'">
           <el-progress  :width="15" type="circle" :stroke-width="2" :show-text="false" v-if="!scope.row.is_draft&&scope.row.diagnose&&scope.row.diagnose.heathStatus==='RUNNING'" :percentage="scope.row.diagnose&&scope.row.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress>
          </common-tip>
           <common-tip :content="scope.row.diagnose&&scope.row.diagnose.messages&&scope.row.diagnose.messages.join('<br/>')">
             <el-progress  :width="15" type="circle" :stroke-width="2" :show-text="false" v-if="!scope.row.is_draft&&scope.row.diagnose&&scope.row.diagnose.heathStatus==='ERROR'" status="exception" :percentage="scope.row.diagnose&&scope.row.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress>
           </common-tip>

         </div>
      </template>
    </el-table-column>
    <el-table-column
      prop="project"
      :label="$t('kylinLang.common.project')"
      show-overflow-tooltip
      width="180">
    </el-table-column>
    <el-table-column
      prop="owner"
      show-overflow-tooltip
      width="100"
      :label="$t('kylinLang.model.ownerGrid')">
    </el-table-column>
    <el-table-column
      prop="fact_table"
      show-overflow-tooltip
      :label="$t('kylinLang.common.fact')">
    </el-table-column>
    <el-table-column
      prop="gmtTime"
      show-overflow-tooltip
      width="190"
      :label="$t('kylinLang.model.modifiedGrid')">
    </el-table-column>
     <el-table-column class="ksd-center"
      width="120"
      :label="$t('kylinLang.common.action')">
       <template slot-scope="scope">
       <span v-if="!(isAdmin || hasPermissionOfProject())"> N/A</span>
       <div v-show="isAdmin || hasPermissionOfProject()">
        <common-tip :content="$t('kylinLang.common.edit')" class="ksd-ml-10"><i class="el-icon-ksd-table_edit ksd-fs-16" @click="handleEditModel(scope.row.uuid)"></i></common-tip>
        <common-tip :content="$t('addCube')" class="ksd-ml-10" v-if="!scope.row.is_draft"><i class="el-icon-circle-plus-outline  ksd-fs-16" @click="handleAddCube(scope.row.uuid)"></i></common-tip>
        <common-tip :content="$t('kylinLang.common.drop')" class="ksd-ml-10"  v-if="scope.row.is_draft"><i class="el-icon-delete ksd-fs-16" @click="handleDropModel(scope.row.uuid)"></i></common-tip>
        <common-tip :content="$t('kylinLang.common.moreActions')" class="ksd-ml-10" v-if="!scope.row.is_draft">
          <el-dropdown @command="handleCommand" :id="scope.row.name" trigger="click" >
            <span class="el-dropdown-link" >
                <i class="el-icon-ksd-table_others ksd-fs-16"></i>
            </span>
           <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid' >
                <el-dropdown-item command="verify">{{$t('kylinLang.common.verifySql')}}</el-dropdown-item>
                <!-- <el-dropdown-item command="cube" v-if="!scope.row.is_draft">{{$t('addCube')}}</el-dropdown-item> -->
                <!-- <el-dropdown-item command="edit">{{$t('kylinLang.common.edit')}}</el-dropdown-item> -->
                <el-dropdown-item command="clone">{{$t('kylinLang.common.clone')}}</el-dropdown-item>
                <el-dropdown-item command="stats">{{$t('kylinLang.common.check')}}</el-dropdown-item>
                <el-dropdown-item command="drop">{{$t('kylinLang.common.drop')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </common-tip>
        </div>
       </template>
    </el-table-column>
  </el-table>


		<pager class="ksd-center ksd-mb-20" ref="pager"  :totalSize="modelsTotal"  v-on:handleCurrentChange='pageCurrentChange' :perPageSize="perPageSize"></pager>
  </div>
    <div class="ksd-null-pic-text" v-if="!(modelsList && modelsList.length) && !showSearchResult">
      <img src="../../assets/img/no_model.png">
      <p>{{$t('noModel')}}</p>
      <div>
      <el-button size="medium" type="primary" icon="el-icon-plus"  v-if="isAdmin || hasPermissionOfProject()" @click="addModel">{{$t('kylinLang.common.model')}}</el-button>
       </div>
    </div>

    <el-dialog :title="$t('modelClone')" width="440px" :visible.sync="cloneFormVisible" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form :model="cloneModelMeta" :rules="cloneFormRule" ref="cloneForm" label-width="100px">
        <el-form-item :label="$t('modelName')" prop="newName">
          <el-input v-model="cloneModelMeta.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="cloneFormVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain :loading="btnLoading" size="medium" @click="clone">{{$t('kylinLang.common.clone')}}</el-button>
      </div>
    </el-dialog>

    <!-- 添加model -->
    <el-dialog :title="$t('kylinLang.model.addModel')" width="440px" :visible.sync="createModelVisible" @close="resetAddModelForm">
      <el-form :model="createModelMeta"  :rules="createModelFormRule" ref="addModelForm" label-width="130px" label-position="top">
        <el-form-item prop="modelName" :label="$t('kylinLang.model.modelName')">
          <span slot="label">{{$t('kylinLang.model.modelName')}}
            <common-tip :content="$t('kylinLang.model.modelNameTips')" ><i class="el-icon-question"></i></common-tip>
          </span>
          <el-input v-model="createModelMeta.modelName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.model.modelDesc')" prop="modelDesc" style="margin-top: 20px;">
         <el-input
            type="textarea"
            :rows="2"
            :placeholder="$t('kylinLang.common.pleaseInput')"
            v-model="createModelMeta.modelDesc">
          </el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createModelVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="createModel" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <!-- 添加cube -->

    <el-dialog :title="$t('kylinLang.cube.addCube')" width="440px" :visible.sync="createCubeVisible" :append-to-body="true">
      <el-form :model="cubeMeta" :rules="createCubeFormRule" ref="addCubeForm" label-width="120px">
        <el-form-item :label="$t('kylinLang.cube.cubeName')" prop="cubeName">
         <span slot="label">{{$t('kylinLang.cube.cubeName')}}
            <common-tip :content="$t('kylinLang.cube.cubeNameTip')" ><i class="el-icon-question"></i></common-tip>
          </span>
          <el-input v-model="cubeMeta.cubeName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createCubeVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="createCube" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="modelCheck" width="660px" :title="$t('kylinLang.model.checkModel')" :append-to-body="true" :visible.sync="scanRatioDialogVisible" :close-on-press-escape="false" :close-on-click-modal="false" @close="resetModelCheckForm" >
        <el-row :gutter="20" class="ksd-mb-34">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content">
              <div class="tips">
                <ul class="ksd-lineheight-18">
                  <li>1. {{$t('kylinLang.model.modelCheckTips1')}}</li>
                  <li>2. {{$t('kylinLang.model.modelCheckTips2')}}</li>
                </ul>
              </div>
              <!-- <div class="line-primary"></div> -->
              <div class="ksd-mt-40">
                <div class="date-picker" v-if="hasPartition">
                    <p class="ksd-dialog-sub-title ksd-mb-20">{{$t('kylinLang.model.samplingSetting')}}
                      <common-tip placement="right" :content="$t('kylinLang.model.samplingSettingTips')" >
                        <i class="el-icon-question"></i>
                      </common-tip>
                    </h2>
                    <p/>
                    {{$t('kylinLang.model.timeRange')}}
                    <el-form :model="modelCheckTime" :rules="modelCheckDateRule" ref="modelCheckForm">
                      <el-row :gutter="20">
                        <el-col :span="10">
                          <div class="grid-content bg-purple">
                            <el-form-item prop="startTime">
                              <el-date-picker
                                :clearable="false" ref="startTimeInput"
                                v-model="modelCheckTime.startTime"
                                type="datetime"
                                :placeholder="$t('chooseStartDate')"
                                size="medium"
                                format="yyyy-MM-dd HH:mm"
                                >
                              </el-date-picker>
                            </el-form-item>
                          </div>
                        </el-col>
                        <el-col :span="1"><div class="ky-line ksd-mt-18"></div></el-col>
                        <el-col :span="10">
                          <div class="grid-content bg-purple">
                            <el-form-item prop="endTime">
                              <el-date-picker
                                :clearable="false" ref="endTimeInput"
                                v-model="modelCheckTime.endTime"
                                type="datetime"
                                :placeholder="$t('chooseEndDate')"
                                size="medium"
                                format="yyyy-MM-dd HH:mm"
                                :picker-options="pickerOptionsEnd">
                              </el-date-picker>
                            </el-form-item>
                          </div>
                        </el-col>
                      </el-row>
                    </el-form>
                  </div>
                  <slider @changeBar="changeBar" :hideCheckbox="true" :range="100" style="width:90%" :label="$t('kylinLang.model.checkModel')" :show="scanRatioDialogVisible">
                    <span slot="sliderLabel">
                      {{$t('kylinLang.model.samplingPercentage')}}
                      <common-tip placement="right" :content="$t('kylinLang.model.samplingPercentageTips')" >
                        <i class="el-icon-question"></i>
                      </common-tip>
                      <!-- <span class="ksd-ml-20">{{modelStaticsRange*100}}%</span> -->
                    </span>
                  </slider>
                  <div class="el-col-24 ksd-mb-10 ksd-mt-24">
                   {{$t('kylinLang.model.checkOption')}}
                     <common-tip placement="right" :content="$t('kylinLang.model.modelCheckOptionsTips')">
                        <i class="el-icon-question"></i>
                      </common-tip>
                  </div>
                  <div class="el-col-8">
                    <el-checkbox v-model="checkModel.modelStatus">{{$t('kylinLang.model.modelStatusCheck')}}</el-checkbox>
                  </div>
                  <div class="el-col-8">
                    <el-checkbox v-model="checkModel.factTable">{{$t('kylinLang.model.factTableSampling')}}</el-checkbox>
                  </div>
                  <div class="el-col-8">
                    <el-checkbox v-model="checkModel.lookupTable">{{$t('kylinLang.model.lookupTableSampling')}}</el-checkbox>
                  </div>
                </div>
              </div>
            </div>
          </el-col>
        </el-row>
        <div slot="footer" class="dialog-footer">
          <el-button @click="cancelSetModelStatics" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="stats" plain :loading="btnLoading" :disabled="!checkModel.lookupTable && !checkModel.factTable && !checkModel.modelStatus" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
        </div>
      </el-dialog>

      <el-dialog
        :title="$t('kylinLang.common.tip')"
        :visible.sync="useCubeDialogVisible"
        >
         {{$t('modelUsedTip')}}<br/>
         <el-tag type="primary"  v-for="tips in usedCubes" :key="tips.name" class="ksd-mt-10 ksd-ml-10">{{tips.name}}</el-tag>
         <!-- <el-alert :closable="false" :title="tips.name" type="info" v-for="tips in usedCubes" class="ksd-mt-10"></el-alert> -->
        <span slot="footer" class="dialog-footer">
          <el-button @click="useCubeDialogVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" plain @click="gotoView" size="medium">{{$t('kylinLang.common.view')}}</el-button>
        </span>
      </el-dialog>


      <el-dialog :title="$t('kylinLang.common.verifySql')" width="780px" :visible.sync="checkSQLFormVisible" :before-close="sqlClose" :close-on-press-escape="false" :close-on-click-modal="false">
        <p style="font-size:12px">{{$t('verifyModelTip1')}}</p>
        <p style="font-size:12px">{{$t('verifyModelTip2')}}</p>
        <div :class="{hasCheck: hasCheck}">
          <kap_editor ref="sqlbox" class="ksd-mt-20" height="200" lang="sql" theme="chrome" v-model="sqlString" dragbar="#393e53">
          </kap_editor>
        </div>
        <div class="ksd-mt-10"><el-button :disabled="sqlString === ''" size="medium" type="primary" :loading="checkSqlLoadBtn" @click="validateSql" >{{$t('kylinLang.common.verify')}}</el-button> 
          <el-button type="primary" text v-show="checkSqlLoadBtn" @click="cancelCheckSql">{{$t('kylinLang.common.cancel')}}</el-button></div>
        <!-- <div class="line" v-if="currentSqlErrorMsg && currentSqlErrorMsg.length || successMsg || errorMsg"></div> -->
        <div class="ksd-mt-20">
          <div v-if="successMsg">
           <el-alert
              :title="successMsg"
              show-icon
              :closable="false"
              type="success">
            </el-alert>
          </div>
          <div v-if="errorMsg">
           <el-alert
              title=""
              show-icon
              :closable="false"
              type="error">
              <div v-html="errorMsg.replace(/\n/g,'<br/>')"></div>
            </el-alert>
          </div>
        </div>
        <div v-if="currentSqlErrorMsg && currentSqlErrorMsg.length && !successMsg && !errorMsg" class="suggestBox">
          <div v-for="sug in currentSqlErrorMsg">
            <h3>{{$t('kylinLang.common.errorDetail')}}</h3>
            <p>{{sug.incapableReason}}</p>
            <h3>{{$t('kylinLang.common.suggest')}}</h3>
            <p v-html="sug.suggestion"></p>
          </div>
        </div>

        <span slot="footer" class="dialog-footer">
          <!-- <el-button @click="sqlClose()">{{$t('kylinLang.common.cancel')}}</el-button> -->
          <el-button type="primary" plain :loading="sqlBtnLoading" size="medium" @click="sqlClose()">{{$t('kylinLang.common.close')}}</el-button>
        </span>
      </el-dialog>
	</div>
</template>
<script>
import { mapActions, mapGetters } from 'vuex'
import cubeList from '../cube/cube_list'
import { modelHealthStatus, permissions, NamedRegex } from '../../config'
import { transToGmtTime, handleError, handleSuccess, kapConfirm, hasRole, hasPermission, filterMutileSqlsToOneLine } from 'util/business'
export default {
  data () {
    return {
      modelHealthStatus: modelHealthStatus,
      checkSQLFormVisible: false,
      sqlBtnLoading: false,
      checkSqlLoadBtn: false,
      hasCheck: false,
      successMsg: '',
      errorMsg: '',
      checkSqlResult: [],
      currentSqlErrorMsg: [],
      useCubeDialogVisible: false,
      scanRatioDialogVisible: false,
      openCollectRange: true,
      usedCubes: [],
      btnLoading: false,
      stCycleRequest: null,
      modelStaticsRange: 1,
      checkModel: {
        modelStatus: true,
        factTable: true,
        lookupTable: true,
        checkList: 0
      },
      modelCheckTime: {
        startTime: '',
        endTime: ''
      },
      serarchChar: '',
      sqlString: '',
      pickerOptionsEnd: {},
      viewModal: 'card',
      hasPartition: false,
      currentDate: new Date(),
      currentPage: 1,
      subMenu: 'Model',
      cloneFormVisible: false,
      createModelVisible: false,
      createCubeVisible: false,
      currentModelData: {},
      searchLoading: false,
      cloneModelMeta: {
        newName: '',
        oldName: '',
        project: ''
      },
      cubeMeta: {
        cubeName: '',
        modelName: '',
        projectName: ''
      },
      createModelMeta: {
        modelName: '',
        modelDesc: ''
      },
      project: localStorage.getItem('selected_project'),
      cloneFormRule: {
        newName: [
          {required: true, message: this.$t('inputCloneName'), trigger: 'blur'},
          {validator: this.checkName, trigger: 'blur'}]
      },
      createModelFormRule: {
        modelName: [
          {required: true, message: this.$t('inputModelName'), trigger: 'blur'},
          {validator: this.checkName, trigger: 'blur'}
        ]
      },
      createCubeFormRule: {
        cubeName: [
          {required: true, message: this.$t('inputCubeName'), trigger: 'blur'},
          {validator: this.checkName, trigger: 'blur'}
        ]
      },
      modelCheckDateRule: {
        startTime: [
          { validator: this.validateStartDate, trigger: 'blur' }
        ],
        endTime: [
          { validator: this.validateEndDate, trigger: 'blur' }
        ]
      },
      ST: null,
      perPageSize: 9,
      showSearchResult: false
    }
  },
  components: {
    cubeList
  },
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST',
      cloneModel: 'CLONE_MODEL',
      statsModel: 'COLLECT_MODEL_STATS',
      delModel: 'DELETE_MODEL',
      loadModelDiagnoseList: 'DIAGNOSELIST',
      checkModelName: 'CHECK_MODELNAME',
      checkCubeName: 'CHECK_CUBE_NAME_AVAILABILITY',
      getCubesList: 'GET_CUBES_LIST',
      getModelProgress: 'GET_MODEL_PROGRESS',
      getModelCheckable: 'MODEL_CHECKABLE',
      diagnose: 'DIAGNOSE',
      verifySql: 'VERIFY_MODEL_SQL'
    }),
    searchModels () {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.searchLoading = true
        this.loadModelsAndDiagnose().then(() => {
          this.searchLoading = false
          if (this.serarchChar) {
            this.showSearchResult = true
          } else {
            this.showSearchResult = false
          }
        }, () => {
          this.searchLoading = false
        })
      }, 500)
    },
    resetAddModelForm () {
      this.$refs['addModelForm'].resetFields()
    },
    sqlClose () {
      this.checkSQLFormVisible = false
    },
    changeGridModal (val) {
      this.viewModal = val
      this.pageCurrentChange(this.currentPage)
    },
    resetModelCheckForm: function () {
      if (this.hasPartition && this.$refs['modelCheckForm']) {
        this.$refs['modelCheckForm'].resetFields()
      }
    },
    validateStartDate: function (rule, value, callback) {
      let realValue = this.$refs['startTimeInput'].$el.querySelectorAll('.el-input__inner')[0].value
      realValue = realValue.replace(/(^\s*)|(\s*$)/g, '')
      if (realValue) {
        let reg = /^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])(\s+[0-9]\d:[0-9]\d)?$/
        let regExp = new RegExp(reg)
        let isLegalDate = regExp.test(realValue)

        if (isLegalDate) {
          let regDate = /^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$/
          let regExpDate = new RegExp(regDate)
          let isLegalDateFormat = regExpDate.test(realValue)
          if (isLegalDateFormat) {
            realValue = realValue + ' 00:00'
            this.modelCheckTime.startTime = new Date()
            this.modelCheckTime.startTime = new Date(realValue)
            this.$nextTick(() => {
              this.$refs['startTimeInput'].$el.querySelectorAll('.el-input__inner')[0].value = realValue
            })
          }
        } else {
          callback(new Error(this.$t('legalDate')))
        }
      }
      let endTime = isNaN((new Date(this.modelCheckTime.endTime)).getTime()) ? 0 : (new Date(this.modelCheckTime.endTime)).getTime()
      let startTime = isNaN((new Date(this.modelCheckTime.startTime)).getTime()) ? 0 : (new Date(this.modelCheckTime.startTime)).getTime()
      if (startTime !== 0 && endTime !== 0 && (endTime <= startTime)) {
        // callback(new Error(this.$t('timeCompare')))
      } else {
        // 开始时间处不触发大小验证，但如果验证通过，需要取消结束时间的错误信息
        this.$refs['modelCheckForm'].validateField('endTime')
        callback()
      }
    },
    validateEndDate: function (rule, value, callback) {
      let realValue = this.$refs['endTimeInput'].$el.querySelectorAll('.el-input__inner')[0].value
      realValue = realValue.replace(/(^\s*)|(\s*$)/g, '')
      if (realValue) {
        let reg = /^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])(\s+[0-9]\d:[0-9]\d)?$/
        let regExp = new RegExp(reg)
        let isLegalDate = regExp.test(realValue)

        if (isLegalDate) {
          let regDate = /^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$/
          let regExpDate = new RegExp(regDate)
          let isLegalDateFormat = regExpDate.test(realValue)
          if (isLegalDateFormat) {
            realValue = realValue + ' 00:00'
            this.modelCheckTime.endTime = new Date()
            this.modelCheckTime.endTime = new Date(realValue)
            this.$nextTick(() => {
              this.$refs['endTimeInput'].$el.querySelectorAll('.el-input__inner')[0].value = realValue
            })
          }
        } else {
          callback(new Error(this.$t('legalDate')))
        }
      }

      let endTime = isNaN((new Date(this.modelCheckTime.endTime)).getTime()) ? 0 : (new Date(this.modelCheckTime.endTime)).getTime()
      let startTime = isNaN((new Date(this.modelCheckTime.startTime)).getTime()) ? 0 : (new Date(this.modelCheckTime.startTime)).getTime()
      if (startTime !== 0 && endTime !== 0 && (endTime <= startTime)) {
        callback(new Error(this.$t('timeCompare')))
      } else {
        callback()
      }
    },
    changeStartTime () {
      this.pickerOptionsEnd.disabledDate = (time) => { // set date-picker endTime
        let nowDate = new Date(this.modelCheckTime.startTime)
        if (this.modelCheckTime.endTime < this.modelCheckTime.startTime) {
          this.modelCheckTime.endTime = this.modelCheckTime.startTime
        }
        let v1 = time.getTime() < +nowDate
        return v1
      }
    },
    showRowClass (o) {
      return o.is_draft ? 'is_draft' : ''
    },
    changeBar (val) {
      this.modelStaticsRange = (val / 100).toFixed(2)
      this.openCollectRange = !!val
    },
    cancelSetModelStatics () {
      this.scanRatioDialogVisible = false
    },
    loadModelsAndDiagnose () {
      var params = {pageSize: this.perPageSize, pageOffset: this.currentPage - 1, modelName: this.serarchChar, exactMatch: false}
      var params1 = {pageSize: this.perPageSize, pageOffset: this.currentPage - 1, modelName: this.serarchChar, exactMatch: false}
      if (localStorage.getItem('selected_project')) {
        params.projectName = localStorage.getItem('selected_project')
        params1.projectName = localStorage.getItem('selected_project')
      }
      return this.loadModels(params).then(() => {
        this.loadModelDiagnoseList(params1)
      }, (res) => {
        handleError(res)
      })
    },
    pageCurrentChange (currentPage) {
      this.currentPage = currentPage
      this.loadModelsAndDiagnose()
    },
    viewModel (modelInfo) {
      this.$emit('addtabs', 'viewmodel', '[view] ' + modelInfo.name, 'modelEdit', {
        project: modelInfo.project,
        modelName: modelInfo.name,
        uuid: modelInfo.uuid,
        status: modelInfo.is_draft,
        mode: 'view',
        i18n: 'view'
      })
    },
    addModel () {
      if (!this.project) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        return
      }
      this.createModelVisible = true
      this.createModelMeta = {
        modelName: '',
        modelDesc: ''
      }
    },
    createModel () {
      this.$refs['addModelForm'].validate((valid) => {
        if (valid) {
          this.btnLoading = true
          this.checkModelName({modelName: this.createModelMeta.modelName}).then((res) => {
            this.btnLoading = false
            handleSuccess(res, (data) => {
              if (data) {
                this.createModelVisible = false
                this.$emit('addtabs', 'model', this.createModelMeta.modelName, 'modelEdit', {
                  project: localStorage.getItem('selected_project'),
                  modelName: this.createModelMeta.modelName,
                  modelDesc: this.createModelMeta.modelDesc,
                  actionMode: 'add'
                })
              } else {
                this.$message({
                  duration: 0,  // 不自动关掉提示
                  showClose: true,    // 给提示框增加一个关闭按钮
                  message: this.$t('kylinLang.model.sameModelName'),
                  type: 'warning'
                })
              }
            })
          }, (res) => {
            this.btnLoading = false
            handleError(res)
          })
        }
      })
    },
    initCubeMeta () {
      this.cubeMeta = {
        cubeName: '',
        modelName: '',
        projectName: ''
      }
    },
    createCube () {
      this.$refs['addCubeForm'].validate((valid) => {
        if (valid) {
          this.btnLoading = true
          this.checkCubeName({cubeName: this.cubeMeta.cubeName}).then((res) => {
            this.btnLoading = false
            handleSuccess(res, (data) => {
              if (!data) {
                this.$message({
                  duration: 0,  // 不自动关掉提示
                  showClose: true,    // 给提示框增加一个关闭按钮
                  message: this.$t('kylinLang.cube.sameCubeName'),
                  type: 'warning'
                })
              } else {
                this.createCubeVisible = false
                this.$emit('addtabs', 'cube', this.cubeMeta.cubeName, 'cubeEdit', {
                  project: this.cubeMeta.projectName,
                  cubeName: this.cubeMeta.cubeName,
                  modelName: this.cubeMeta.modelName,
                  isEdit: false
                })
              }
            })
          }, (res) => {
            this.btnLoading = false
            handleError(res)
          })
        }
      })
    },
    initCloneMeta () {
      this.cloneModelMeta = {
        newName: '',
        oldName: '',
        project: ''
      }
    },
    gotoView () {
      var modelData = this.currentModelData
      this.useCubeDialogVisible = false
      this.viewModel({
        name: modelData.name,
        project: modelData.project,
        is_draft: modelData.is_draft,
        uuid: modelData.uuid
      })
    },
    // 渲染编辑器行号列尺寸
    renderEditerRender (editor) {
      // var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
      if (!(editor && editor.sesssion)) {
        return
      }
      editor.sesssion.gutterRenderer = {
        getWidth: (session, lastLineNumber, config) => {
          return lastLineNumber.toString().length * config.characterWidth
        },
        getText: (session, row) => {
          return row
        }
      }
    },
    // 取消校验sql
    cancelCheckSql () {
      this.checkSqlLoadBtn = false
    },
    // 校验sql
    validateSql () {
      var sqls = filterMutileSqlsToOneLine(this.sqlString)
      if (sqls.length === 0) {
        return
      }
      this.hasCheck = false
      var editor = this.$refs.sqlbox && this.$refs.sqlbox.$refs.kapEditor.editor || ''
      editor && editor.removeListener('change', this.editerChangeHandle)
      this.renderEditerRender(editor)
      this.errorMsg = false
      this.checkSqlLoadBtn = true
      // this.sqlString = sqls.join(';\r\n')
      this.sqlString = sqls.length > 0 ? sqls.join(';\r\n') + ';' : ''
      this.verifySql({
        sqls: sqls,
        modelName: this.currentModelData.name
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.hasCheck = true
          this.checkSqlResult = data
          this.currentSqlErrorMsg = []
          this.addBreakPoint(data, editor)
          this.bindBreakClickEvent(editor)
          this.checkSqlLoadBtn = false
          editor && editor.on('change', this.editerChangeHandle)
        })
      }, (res) => {
        handleError(res)
      })
    },
    // 添加错误标志
    addBreakPoint (data, editor) {
      this.errorMsg = ''
      this.successMsg = ''
      if (!editor) {
        return
      }
      if (data && data.length) {
        var hasFailValid = false
        data.forEach((r, index) => {
          if (r.capable === false) {
            hasFailValid = true
            editor.session.setBreakpoint(index)
          } else {
            editor.session.clearBreakpoint(index)
          }
        })
        if (hasFailValid) {
          this.errorMsg = this.$t('validFail')
        } else {
          this.successMsg = this.$t('validSuccess')
        }
      }
    },
    // 绑定错误标记事件
    bindBreakClickEvent (editor) {
      if (!editor) {
        return
      }
      editor.on('guttermousedown', (e) => {
        var row = +e.domEvent.target.innerHTML
        // var row = e.getDocumentPosition().row
        var pointDoms = this.$el.querySelectorAll('.ace_gutter-cell')
        pointDoms.forEach((dom) => {
          if (+dom.innerHTML === row) {
            dom.className += ' active'
          } else {
            dom.className = dom.className.replace(/\s+active/g, '')
          }
        })
        var data = this.checkSqlResult
        row = row - 1
        if (data && data.length) {
          if (data[row].capable === false) {
            if (data[row].sqladvices) {
              this.errorMsg = ''
              this.currentSqlErrorMsg = data[row].sqladvices
            }
          } else {
            this.errorMsg = ''
            this.successMsg = ''
            this.currentSqlErrorMsg = []
          }
        }
        e.stop()
      })
    },
    editerChangeHandle () {
      this.hasCheck = false
    },
    handleEditModel (uuid) {
      var modelData = this.getModelDataByUuid(uuid)
      var modelName = modelData.name
      var projectName = modelData.project
      this.getModelCheckMode(projectName, modelName, () => {
        this.$emit('addtabs', 'model', modelName, 'modelEdit', {
          project: projectName,
          modelName: modelName,
          uuid: uuid,
          status: modelData.is_draft
        })
      }, () => {
        this.$message(this.$t('kylinLang.model.modelHasJob'))
      })
    },
    handleAddCube (uuid) {
      var modelData = this.getModelDataByUuid(uuid)
      var modelName = modelData.name
      var projectName = modelData.project
      this.initCubeMeta()
      this.createCubeVisible = true
      this.cubeMeta.projectName = projectName
      this.cubeMeta.modelName = modelName
    },
    handleDropModel (uuid) {
      var modelData = this.getModelDataByUuid(uuid)
      this.currentModelData = modelData
      var modelName = modelData.name
      var projectName = modelData.project
      this.getModelCheckMode(projectName, modelName, () => {
        kapConfirm(this.$t('delModelTip')).then(() => {
          this.drop()
        })
      }, () => {
        this.$message({message: this.$t('kylinLang.model.modelHasJob'), type: 'warning'})
      })
    },
    handleCommand (command, component) {
      var handleData = component.$parent.$el
      var uuid = handleData.getAttribute('uuid')
      var modelData = this.getModelDataByUuid(uuid)
      this.currentModelData = modelData
      // this.currentModelData.modelName = modelData.name
      // this.currentModelData.project = modelData.project
      var modelName = modelData.name
      var projectName = modelData.project
      if (command === 'verify') {
        this.checkSQLFormVisible = true
        this.sqlString = ''
        this.hasCheck = false
        this.currentSqlErrorMsg = false
        this.errorMsg = ''
        this.successMsg = ''
      } else if (command === 'edit') {
        this.handleEditModel(uuid)
      } else if (command === 'clone') {
        this.initCloneMeta()
        this.cloneFormVisible = true
        this.cloneModelMeta.newName = modelName + '_clone'
        this.cloneModelMeta.oldName = modelName
        this.cloneModelMeta.project = projectName
        // this.cloneModel(modelName, projectName)
      } else if (command === 'stats') {
        this.isModelAllowCheck(this.currentModelData.project, this.currentModelData.name).then((res) => {
          handleSuccess(res, (data) => {
            for (var i in data) {
              if ('' + i === 'true') {
                this.getModelProgress({
                  project: projectName,
                  modelName: modelName
                }).then((res) => {
                  handleSuccess(res, (data) => {
                    for (var i in data) {
                      if ('' + i === 'false') {
                        this.scanRatioDialogVisible = true
                        this.modelCheckTime.startTime = ''
                        if (modelData.partition_desc.partition_date_column) {
                          this.hasPartition = true
                        } else {
                          this.hasPartition = false
                        }
                        this.checkModel.factTable = true
                        this.checkModel.lookupTable = true
                        this.checkModel.modelStatus = true
                        this.checkModel.checkList = 0
                        return
                      }
                    }
                    this.$message({
                      type: 'warning',
                      message: this.$t('hasChecked')
                    })
                  })
                })
              } else {
                this.$message({
                  message: data[i] || this.$t('canNotChecked')
                })
              }
              return
            }
          })
        })
        // this.stats(projectName, modelName)
      } else if (command === 'drop') {
        this.handleDropModel(uuid)
      } else if (command === 'cube') {
        this.handleAddCube(uuid)
      }
    },
    getModelCheckMode (projectName, modelName, noCb, yesCb) {
      this.getModelProgress({
        project: projectName,
        modelName: modelName
      }).then((res) => {
        handleSuccess(res, (data) => {
          for (var i in data) {
            if (i === 'false') {
              if (typeof noCb === 'function') {
                noCb()
              }
              return
            }
          }
          if (typeof yesCb === 'function') {
            yesCb()
          }
        })
      })
    },
    clone () {
      this.$refs['cloneForm'].validate((valid) => {
        if (valid) {
          this.btnLoading = true
          var modelData = this.getModelData(this.cloneModelMeta.oldName, this.cloneModelMeta.project)
          this.cloneModel({
            oldName: this.cloneModelMeta.oldName,
            data: {
              project: this.cloneModelMeta.project,
              modelName: this.cloneModelMeta.newName,
              modelDescData: JSON.stringify(modelData)
            }
          }).then((response) => {
            this.cloneFormVisible = false
            this.btnLoading = false
            this.$message({
              type: 'success',
              message: this.$t('kylinLang.common.cloneSuccess')
            })
            this.loadModelsAndDiagnose()
          }, (res) => {
            this.btnLoading = false
            handleError(res)
          })
        }
      })
    },
    _statsModel (para) {
      this.btnLoading = true
      if (this.checkModel.factTable) {
        this.checkModel.checkList = this.checkModel.checkList + 1
      }
      if (this.checkModel.lookupTable) {
        this.checkModel.checkList = this.checkModel.checkList + 2
      }
      if (this.checkModel.modelStatus) {
        this.checkModel.checkList = this.checkModel.checkList + 4
      }
      this.statsModel({
        project: this.currentModelData.project,
        modelname: this.currentModelData.name,
        data: {
          startTime: para.start,
          endTime: para.end,
          ratio: this.modelStaticsRange,
          checkList: this.checkModel.checkList,
          forceUpdate: true
        }
      }).then(() => {
        this.btnLoading = false
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.submitSuccess')
        })
        this.scanRatioDialogVisible = false
      }, (res) => {
        handleError(res)
        this.btnLoading = false
      })
    },
    stats () {
      if (+this.modelStaticsRange === 0) {
        this.$message({
          message: this.$t('kylinLang.common.pleaseSelectSampleRange')
        })
        return
      }
      if (this.hasPartition) {
        this.$refs['modelCheckForm'].validate((valid) => {
          if (valid) {
            var tempStartTime = isNaN((new Date(this.modelCheckTime.startTime)).getTime()) ? 0 : (new Date(this.modelCheckTime.startTime)).getTime()
            var tempEndTime = isNaN((new Date(this.modelCheckTime.endTime)).getTime()) ? 0 : (new Date(this.modelCheckTime.endTime)).getTime()
            var tempObj = {start: null, end: null}
            if (tempStartTime === 0 && tempEndTime === 0) {
              tempObj = {start: 0, end: 0}
            } else if (tempStartTime === 0 && tempEndTime !== 0) {
              tempObj = {start: null, end: tempEndTime}
            } else if (tempStartTime !== 0 && tempEndTime === 0) {
              tempObj = {start: tempStartTime, end: null}
            } else {
              tempObj = {start: tempStartTime, end: tempEndTime}
            }
            this._statsModel(tempObj)
          }
        })
      } else {
        this._statsModel({
          start: 0, end: 0
        })
      }
    },
    isModelAllowCheck (projectName, modelName) {
      return this.getModelCheckable({
        project: projectName,
        modelName: modelName
      })
    },
    drop () {
      this.delModel({modelName: this.currentModelData.name, project: this.currentModelData.project}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.loadModelsAndDiagnose()
      }, (res) => {
        handleError(res)
      })
    },
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    },
    getModelData (modelName, project) {
      var modelsList = this.$store.state.model.modelsList
      for (var k = 0; k < modelsList.length; k++) {
        if (modelsList[k].name === modelName && modelsList[k].project === project) {
          return modelsList[k]
        }
      }
      return []
    },
    getModelDataByUuid (uuid) {
      var modelsList = this.$store.state.model.modelsList
      for (var k = 0; k < modelsList.length; k++) {
        if (modelsList[k].uuid === uuid) {
          return modelsList[k]
        }
      }
      return []
    },
    isUsedInCubes (modelName, callback, noUseCallback) {
      this.getCubesList({
        pageSize: 10000,
        pageOffset: 0,
        projectName: localStorage.getItem('selected_project'),
        modelName: modelName
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.usedCubes = data.cubes
          if (this.usedCubes && this.usedCubes.length) {
            callback()
            return
          }
          noUseCallback()
        })
      })
    },
    hasPermissionOfProject () {
      return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    }
  },
  computed: {
    ...mapGetters([
      'selectedProjectDatasource'
    ]),
    modelsList () {
      if (this.$store.state.model.modelsList && this.$store.state.model.modelsDianoseList) {
        return this.$store.state.model.modelsList.map((m) => {
          m.gmtTime = transToGmtTime(m.last_modified, this)
          this.$store.state.model.modelsDianoseList.forEach((d) => {
            if (d.modelName === m.name) {
              d.progress = d.progress === 0 ? 0 : parseInt(d.progress)
              d.messages = d.messages && d.messages.length ? d.messages.map((x) => {
                return x.replace(/\r\n/g, '<br/>')
              }) : [modelHealthStatus[d.heathStatus].message]
              m.diagnose = d
            }
          })
          return m
        })
      }
    },
    modelsTotal () {
      return this.$store.state.model.modelsTotal
    },
    modelHelth () {
      return this.$store.state.model.modelsDianoseList
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  created () {
    this.loadModelsAndDiagnose()
    var cycleDiagnose = () => {
      window.clearTimeout(this.stCycleRequest)
      this.stCycleRequest = setTimeout(() => {
        if (this.$route.path !== '/studio/model') {
          // cycleDiagnose()
          return
        }
        if (this.$store.state.model.activeMenuName !== 'Overview') {
          cycleDiagnose()
          return
        }
        this.loadModelsAndDiagnose().then(() => {
          cycleDiagnose()
        }, (res) => {
          handleError(res)
        })
      }, 10000)
    }
    cycleDiagnose()
  },
  beforeDestroy () {
    window.clearTimeout(this.stCycleRequest)
  },
  locales: {
    'en': {'modelName': 'Model name', 'addCube': 'Add Cube', modelClone: 'Model Clone', modelUsedTip: 'The model has been used by following cubes, so it cannot be edit for now.', 'inputCloneName': 'Please enter name', 'inputModelName': 'Please enter model name', 'inputCubeName': 'Please enter cube name', 'delModelTip': 'Are you sure to drop this model?', 'hasNotChecked': 'Haven\'t checked health yet.', hasChecked: 'There has been a running model check job. Switch to "Monitor" page to view the progress.', canNotChecked: 'Something went wrong, this model cannot be checked for now. Please contact us for more help here.', chooseStartDate: 'Please select the start time.', chooseEndDate: 'Please select the end time.', verifyModelTip1: '1. This function will help you to verify if the model can answer following SQL statements.', verifyModelTip2: '2. Multiple SQL statements will be separated by ";".', validFail: 'Uh oh, some SQL went wrong. Click the failed SQL to learn why it didn\'t work and how to refine it.', validSuccess: 'Great! All SQL can perfectly work on this model.', selectDate: 'Please select the date.', legalDate: 'Please enter a complete date formatted as YYYY-MM-DD.', timeCompare: 'End time should be later than the start time.', noModel: 'You can click below button to add a model'},
    'zh-cn': {'modelName': '模型名称', 'addCube': '添加Cube', modelClone: '模型克隆', 'modelUsedTip': '该模型已经被下列Cube引用，暂时无法被编辑。', 'inputCloneName': '请输入名称。', 'inputModelName': '请输入模型名称。', 'inputCubeName': '请输入Cube名称。', 'delModelTip': '你确认要删除该模型吗？', 'hasNotChecked': '尚未进行健康检测。', hasChecked: '已有一个检测任务正在进行中，您可以去“监控”页面查看进度。', canNotChecked: '该模型暂时无法进行检测，请联系售后人员获得支持。', chooseStartDate: '请选择起始时间。', chooseEndDate: '请选择结束时间。', verifyModelTip1: '1. 系统将帮助您检验以下SQL是否能被本模型回答。', verifyModelTip2: '2. 输入多条SQL语句时将以“；”作为分隔。', validFail: '有无法运行的SQL查询。请点击未验证成功的SQL，获得具体原因与修改建议。', validSuccess: '所有SQL都能被本模型验证。', selectDate: '请选择时间', legalDate: '请输入完整日期，格式为YYYY-MM-DD。', timeCompare: '结束日期应晚于起始时间', noModel: '您可以点击下面的按钮来添加模型'}
  }
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
.modelCheck{
  .ksd-dialog-sub-title {
    font-weight: bolder;
  }
  .date-picker {
    .el-col {
      margin-bottom: 0px;
      height: 60px;
    }
  }
  .el-date-editor.el-input{
    width: 100%;
  }
  .el-dialog__body {
    padding: 20px;
  }
  .line-primary {
    margin: 10px -30px 0px -20px;
    background-color: #292b38;
    height:1px;
  }
}
.modelist_box{
  .card-model-list{
    height: 138px;
  }
  .model-status{
    i{
      font-size:18px;
    }
  }
  .el-table__row {
    .cell{
      &>span{
        padding-right: 30px;
      }
    }
  }
  .draft-model {
    color:@warning-color-1;
  }
  margin-left: 20px;
  /* min-height:600px; */
  margin-right: 20px;
  .card-model-title{
    color:@text-title-color;
    max-width: 80%;
    display: inline-block;
    text-overflow: ellipsis;
    overflow: hidden;
    vertical-align:middle;
    font-size: 18px;
  }
  .null_pic_box{
      text-align:center;
      padding-top:100px;
      .null_pic_2{
        width:150px;
      }
      >div {
        position: relative;
        left: 50%;
        .el-button {
           position:relative;
           left: -38px;
        }
      }
    }
  // .line{
  //   background: #292b38;
  //   margin-left: -20px;
  //   margin-right: -20px;
  // }
  .suggestBox{
    border-radius: 4px;
    background-color: @aceditor-bg-color;
    border:solid 1px @text-secondary-color;
    margin-top:20px;
    padding: 10px 10px;
    max-height: 200px;
    overflow-y: auto;
    font-size: 12px;
    color:@text-title-color;
    h3{
      margin-bottom:10px;
      // margin-top: 10px;
      color:@error-color-1;
      font-size: 12px;
    }
    p{
      margin-bottom: 10px;
    }
  }

  .tips{
    font-size: 12px;
  }
  .icon_card, .icon_table{
    display: inline-block;
    width: 16px;
    height: 14px;
    cursor: pointer;
  }
  .model-list-mode {
    font-size: 16px;
    color:@text-secondary-color;
    &.active {
      color:@base-color;
    }
  }
  // .icon_card {
  //   background-image: url('../../assets/img/cardlist.png');
  //   background-size: cover;
  // }
  // .icon_card:hover,.icon_card.active {
  //   background-image: url('../../assets/img/cardlisthover.png');
  //   background-size: cover;
  // }
  // .icon_table{
  //   background-image: url('../../assets/img/tablelist.png');
  //   background-size: cover;
  // }
  // .icon_table:hover,.icon_table.active{
  //   background-image: url('../../assets/img/tablelisthover.png');
  //   background-size: cover;
  // }
  .fa-icon{
    cursor: pointer;
    &.active{
      color:@base-color;
    }
  }
  .el-table {
    font-size: 12px;
    // .is_draft {
    //   // td {
    //   //   background: #515770!important;
    //   // }
    //   &>td:first-child {
    //    &>div{
    //     // background-image: url('../../assets/img/draft.png');
    //     background-repeat: no-repeat;
    //     // background-color: #515770;
    //     // border:dashed 1px @fff;
    //     background-size: 20px;
    //     background-position: 90% 80%;
    //     padding-right:30px;
    //    }
    //   }
    // }
  }
  .el-card{
    border:solid 1px @line-border-color;
    box-shadow: none;
    border-radius: 0;
    position: relative;
    &:hover{
      box-shadow: @box-shadow;
    }
    // &.is_draft {
    //   background-image: url('../../assets/img/draft.png');
    //   background-repeat: no-repeat;
    //   // background-color: #515771;
    //   border:dashed 1px #7881AA;
    //   background-position: 90% 80%;
    // }
  }
 h2{
 	// color:#fff;
  font-size:14px;
  font-weight: normal;
  cursor:pointer;
  i{
    color:#13ce66;
    font-size: 18px;
  }
 }
 .title{
 	 margin-left: 10px;
 	 margin-top: 10px;
 	 color: @text-normal-color;
 	 font-size: 14px;
 }
 .el-col {
    margin-bottom: 20px;
    &:last-child {
      margin-bottom: 0;
    }
  }
 .owner {
    float: left;
    font-size: 13px;
    color: @text-normal-color;
  }
  .action-btns{
    float: right;
    margin-right: 0;
    cursor: pointer;
    i{
      font-size: 16px;
      vertical-align:top;
      &:hover{
        color:@base-color-2;
      }
    }
  }
  .el-table{
    .el-dropdown{
      float: none;
    }
  }
  .bottom-info {
    margin-top: 40px;
    line-height: 12px;
  }
  .image {
    width: 100%;
    display: block;
  }
  .tip_box {
    .icon {
      margin-left: 10px;
    }
  }
}
</style>
