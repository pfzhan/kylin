<template>
	<div class="modelist_box">

    <el-button type="trans" icon="plus" class="ksd-mb-10 radius" id="addModel" v-if="isAdmin || hasPermissionOfProject(project)" @click="addModel"><span>{{$t('kylinLang.common.model')}}</span></el-button>
    <br/>
    <p class="ksd-right ksd-mb-10" v-if="modelsList&&modelsList.length">
      <span class="icon_card" @click="changeGridModal('card')" :class="{active: viewModal==='card'}"></span>
      <span class="icon_table" @click="changeGridModal('list')"  :class="{active: viewModal!=='card'}"></span>
    </p>
		<el-row :gutter="20" v-if="viewModal==='card'">
		  <el-col :span="8"  v-for="(o, index) in modelsList" :key="o.uuid" :style="{height:'152px'}">
		    <el-card :body-style="{ padding: '0px'}" style="height:100%" :class="{'is_draft': o.is_draft}">
		      <p style="font-size: 12px;padding-left: 10px;" class="title">{{$t('kylinLang.model.modifiedGrid')}} {{ o.gmtTime }}
					<el-dropdown style="margin-right: 20px;" @command="handleCommand" :id="o.name" trigger="click"  v-show="isAdmin || hasPermissionOfProject(o.project)">
					  <span class="el-dropdown-link" >
					    <icon name="ellipsis-h"></icon>
					  </span>
					  <el-dropdown-menu slot="dropdown"  :uuid='o.uuid' >
              <el-dropdown-item command="verify" v-if="!o.is_draft">{{$t('kylinLang.common.verifySql')}}</el-dropdown-item>
              <el-dropdown-item command="cube" v-if="!o.is_draft">{{$t('addCube')}}</el-dropdown-item>
					    <el-dropdown-item command="edit">{{$t('kylinLang.common.edit')}}</el-dropdown-item>
					    <el-dropdown-item command="clone" v-if="!o.is_draft">{{$t('kylinLang.common.clone')}}</el-dropdown-item>
					    <el-dropdown-item command="stats" v-if="!o.is_draft">{{$t('kylinLang.common.check')}}</el-dropdown-item>
              <el-dropdown-item command="drop" >{{$t('kylinLang.common.drop')}}</el-dropdown-item>
					  </el-dropdown-menu>
					</el-dropdown>
		    </p>
		      <div style="padding: 20px;">
		        <h2>
            <el-tooltip class="item" effect="dark" :content="o.name" placement="top">
              <span @click="viewModel(o)">{{o.name|omit(24, '...')}}</span>
            </el-tooltip>
           <common-tip :content="o.diagnose&&o.diagnose.messages.join('<br/>')" v-if="!o.is_draft && o.diagnose &&o.diagnose.heathStatus!=='RUNNING' &&o.diagnose.heathStatus!=='ERROR'" >
             <icon  :style="{color:modelHealthStatus[o.diagnose.heathStatus].color}" :name="modelHealthStatus[o.diagnose.heathStatus].icon"></icon></common-tip>

             <common-tip :content="o.diagnose&&o.diagnose.progress + '%'||'0%'"><el-progress :width="15" type="circle"  :stroke-width="2" :show-text="false" v-if="!o.is_draft&&o.diagnose&&o.diagnose.heathStatus==='RUNNING'" :percentage="o.diagnose&&o.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></common-tip>

<common-tip :content="o.diagnose&&o.diagnose.messages.join('<br/>')" v-if="!o.is_draft&&o.diagnose&&o.diagnose.heathStatus==='ERROR'"><el-progress :width="15" type="circle" status="exception" :stroke-width="2" :show-text="false"  :percentage="o.diagnose&&o.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></common-tip>
             </h2>
		        <div class="bottom clearfix">
		          <time class="time" v-visible="o.owner" style="display:block">{{o.owner}}</time>
		        </div>
		      </div>
		    </el-card>
		  </el-col>
		</el-row>


    <el-table v-if="viewModal!=='card'"
    :data="modelsList"
    border
    :row-class-name="showRowClass"
    stripe
    tooltip-effect="dark"
    style="width: 100%">
    <el-table-column
    show-overflow-tooltip
      :label="$t('kylinLang.model.modelNameGrid')"
      width="180">
       <template scope="scope" >
       <span style="cursor:pointer" @click="viewModel(scope.row)">{{scope.row.name}}</span>
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
      width="130"
      class-name="ksd-center"
      :label="$t('kylinLang.model.statusGrid')">
      <template scope="scope" >
         <!-- <icon v-if="!scope.row.is_draft && scope.row.diagnose && scope.row.diagnose.progress===0" :name="modelHealthStatus[scope.row.diagnose.heathStatus].icon" :style="{color:modelHealthStatus[scope.row.diagnose.heathStatus].color}"></icon> -->
          <common-tip  :content="scope.row.diagnose&&scope.row.diagnose.messages&&scope.row.diagnose.messages.join('<br/>')" v-if="!scope.row.is_draft && scope.row.diagnose && scope.row.diagnose.heathStatus!=='RUNNING'&& scope.row.diagnose.heathStatus!=='ERROR' && (scope.row.diagnose.progress===0 || scope.row.diagnose.progress===100)"> <icon  :name="modelHealthStatus[scope.row.diagnose.heathStatus].icon" :style="{color:modelHealthStatus[scope.row.diagnose.heathStatus].color}"></icon></common-tip>
         <common-tip :content="scope.row.diagnose&&scope.row.diagnose.progress + '%'||'0%'">
         <el-progress  :width="15" type="circle" :stroke-width="2" :show-text="false" v-if="!scope.row.is_draft&&scope.row.diagnose&&scope.row.diagnose.heathStatus==='RUNNING'" :percentage="scope.row.diagnose&&scope.row.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></common-tip></h2>

         <common-tip :content="scope.row.diagnose&&scope.row.diagnose.messages&&scope.row.diagnose.messages.join('<br/>')">
         <el-progress  :width="15" type="circle" :stroke-width="2" :show-text="false" v-if="!scope.row.is_draft&&scope.row.diagnose&&scope.row.diagnose.heathStatus==='ERROR'" status="exception" :percentage="scope.row.diagnose&&scope.row.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></common-tip></h2>
      </template>
    </el-table-column>
    <el-table-column
      prop="fact_table"
      show-overflow-tooltip
      :label="$t('kylinLang.common.fact')">
    </el-table-column>
    <el-table-column
      prop="gmtTime"
      show-overflow-tooltip
      :label="$t('kylinLang.model.modifiedGrid')">
    </el-table-column>
     <el-table-column class="ksd-center"
      width="100"
      :label="$t('kylinLang.common.action')">
       <template scope="scope">
       <span v-if="!(isAdmin || hasPermissionOfProject(scope.row.project))"> N/A</span>
        <el-dropdown @command="handleCommand" :id="scope.row.name" trigger="click" v-show="isAdmin || hasPermissionOfProject(scope.row.project)">
           <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
         <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid'>
              <el-dropdown-item command="verify" v-if="!scope.row.is_draft">{{$t('kylinLang.common.verifySql')}}</el-dropdown-item>
              <el-dropdown-item command="cube" v-if="!scope.row.is_draft">{{$t('addCube')}}</el-dropdown-item>
              <el-dropdown-item command="edit">{{$t('kylinLang.common.edit')}}</el-dropdown-item>
              <el-dropdown-item command="clone" v-if="!scope.row.is_draft">{{$t('kylinLang.common.clone')}}</el-dropdown-item>
              <el-dropdown-item command="stats" v-if="!scope.row.is_draft">{{$t('kylinLang.common.check')}}</el-dropdown-item>
              <el-dropdown-item command="drop" >{{$t('kylinLang.common.drop')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
       </template>
    </el-table-column>
  </el-table>


		<pager class="ksd-center ksd-mb-20" ref="pager"  :totalSize="modelsTotal"  v-on:handleCurrentChange='pageCurrentChange' ></pager>

    <div class="null_pic_box" v-if="!(modelsList && modelsList.length)"><img src="../../assets/img/no_model.png" class="null_pic_2"></div>

    <el-dialog title="Clone Model" v-model="cloneFormVisible" size="tiny" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form :model="cloneModelMeta" :rules="cloneFormRule" ref="cloneForm">
        <el-form-item :label="$t('modelName')" prop="newName">
          <el-input v-model="cloneModelMeta.newName" auto-complete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="cloneFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="btnLoading" @click="clone">{{$t('kylinLang.common.clone')}}</el-button>
      </div>
    </el-dialog>

    <!-- 添加model -->
    <el-dialog class="add-m" title="Add Model" v-model="createModelVisible" size="tiny">
      <el-form :model="createModelMeta" :rules="createModelFormRule" ref="addModelForm">
        <el-form-item prop="modelName" :label="$t('kylinLang.model.modelName')">
          <span slot="label">{{$t('kylinLang.model.modelName')}}
            <common-tip :content="$t('kylinLang.model.modelNameTips')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip>
          </span>
          <el-input v-model="createModelMeta.modelName" auto-complete="off"></el-input>
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
        <el-button @click="createModelVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="createModel" :loading="btnLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <!-- 添加cube -->

    <el-dialog title="Add Cube" v-model="createCubeVisible" size="tiny">
      <el-form :model="cubeMeta" :rules="createCubeFormRule" ref="addCubeForm">
        <el-form-item :label="$t('kylinLang.cube.cubeName')" prop="cubeName">
         <span slot="label">{{$t('kylinLang.cube.cubeName')}}
            <common-tip :content="$t('kylinLang.cube.cubeNameTip')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip>
          </span>
          <el-input v-model="cubeMeta.cubeName" auto-complete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createCubeVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="createCube" :loading="btnLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="modelCheck" :title="$t('kylinLang.model.checkModel')" size="tiny" v-model="scanRatioDialogVisible" :close-on-press-escape="false" :close-on-click-modal="false" @close="resetModelCheckForm">
        <el-row :gutter="20">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content">
              <div class="tips">
                <ul>
                  <li>1. {{$t('kylinLang.model.modelCheckTips1')}}</li>
                  <li>2. {{$t('kylinLang.model.modelCheckTips2')}}</li>
                </ul>
              </div>
              <div class="ksd-mt-20">
              <div class="date-picker" v-if="hasPartition">
                  <h2>{{$t('kylinLang.model.samplingSetting')}}
                    <common-tip placement="right" :content="$t('kylinLang.model.samplingSettingTips')" >
                      <icon name="question-circle" class="ksd-question-circle"></icon>
                    </common-tip>

                      <!--<kap-common-popover>
                      <div slot="content">
                        <ul>
                          <li>{{$t('kylinLang.model.samplingSettingTips')}}</li>
                        </ul>
                      </div>
                        <icon name="question-circle" class="ksd-question-circle"></icon>
                      </kap-common-popover>-->
                  </h2>
                  <br/>
                  {{$t('kylinLang.model.timeRange')}}
                <el-form :model="modelCheckTime" :rules="modelCheckDateRule" ref="modelCheckForm">
                  <el-row :gutter="20">
                    <el-col :span="11">
                      <div class="grid-content bg-purple">
                        <el-form-item prop="startTime">
                          <el-date-picker
                            :clearable="false" ref="startTimeInput"
                            v-model="modelCheckTime.startTime"
                            type="datetime"
                            :placeholder="$t('chooseStartDate')"
                            size="small"
                            format="yyyy-MM-dd HH:mm"
                            >
                          </el-date-picker>
                        </el-form-item>
                      </div>
                    </el-col>
                    <el-col :span="2"><div class="grid-content bg-purple" style="line-height:60px;text-align:center;">－</div></el-col>
                    <el-col :span="11">
                      <div class="grid-content bg-purple">
                        <el-form-item prop="endTime">
                          <el-date-picker
                            :clearable="false" ref="endTimeInput"
                            v-model="modelCheckTime.endTime"
                            type="datetime"
                            :placeholder="$t('chooseEndDate')"
                            size="small"
                            format="yyyy-MM-dd HH:mm"
                            :picker-options="pickerOptionsEnd">
                          </el-date-picker>
                        </el-form-item>
                      </div>
                    </el-col>
                  </el-row>
                </el-form>
                  <span class="line"></span>

                </div>
                  <slider @changeBar="changeBar" :hideCheckbox="true" :range="100" :label="$t('kylinLang.model.checkModel')" :show="scanRatioDialogVisible">
                    <span slot="sliderLabel">{{$t('kylinLang.dataSource.samplingPercentage')}} <common-tip placement="right" :content="$t('kylinLang.model.samplingPercentageTips')" >
                 <icon name="question-circle" class="ksd-question-circle"></icon>
              </common-tip></span>
                  </slider>
              </div>
              </div>
            </div>
          </el-col>
        </el-row>
        <div slot="footer" class="dialog-footer">
          <el-button @click="cancelSetModelStatics">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="stats" :loading="btnLoading">{{$t('kylinLang.common.submit')}}</el-button>
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
          <el-button @click="useCubeDialogVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="gotoView">{{$t('kylinLang.common.view')}}</el-button>
        </span>
      </el-dialog>


      <el-dialog :title="$t('kylinLang.common.verifySql')" v-model="checkSQLFormVisible" :before-close="sqlClose" :close-on-press-escape="false" :close-on-click-modal="false">
        <p style="font-size:12px">{{$t('verifyModelTip1')}}</p>
        <p style="font-size:12px">{{$t('verifyModelTip2')}}</p>
        <div :class="{hasCheck: hasCheck}">
        <editor v-model="sqlString" ref="sqlbox" theme="chrome"  class="ksd-mt-20" width="95%" height="200" ></editor>
        </div>
        <div class="ksd-mt-10"><el-button :disabled="sqlString === ''" :loading="checkSqlLoadBtn" @click="validateSql" >{{$t('kylinLang.common.verify')}}</el-button> <el-button type="text" v-show="checkSqlLoadBtn" @click="cancelCheckSql">{{$t('kylinLang.common.cancel')}}</el-button></div>
        <div class="line" v-if="currentSqlErrorMsg && currentSqlErrorMsg.length || successMsg || errorMsg"></div>
        <div v-if="currentSqlErrorMsg && currentSqlErrorMsg.length || successMsg || errorMsg" class="suggestBox">
          <div v-if="successMsg">
           <el-alert class="pure"
              :title="successMsg"
              show-icon
              :closable="false"
              type="success">
            </el-alert>
          </div>
          <div v-if="errorMsg">
           <el-alert class="pure"
              :title="errorMsg"
              show-icon
              :closable="false"
              type="error">
            </el-alert>
          </div>
          <div v-for="sug in currentSqlErrorMsg">
            <h3>{{$t('kylinLang.common.errorDetail')}}</h3>
            <p>{{sug.incapableReason}}</p>
            <h3>{{$t('kylinLang.common.suggest')}}</h3>
            <p v-html="sug.suggestion"></p>
          </div>
        </div>

        <span slot="footer" class="dialog-footer">
          <!-- <el-button @click="sqlClose()">{{$t('kylinLang.common.cancel')}}</el-button> -->
          <el-button type="primary" :loading="sqlBtnLoading" @click="sqlClose()">{{$t('kylinLang.common.close')}}</el-button>
        </span>
      </el-dialog>
	</div>
</template>
<script>
import { mapActions } from 'vuex'
import cubeList from '../cube/cube_list'
import { pageCount, modelHealthStatus, permissions, NamedRegex } from '../../config'
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
      modelCheckTime: {
        startTime: '',
        endTime: ''
      },
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
      }
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
    sqlClose () {
      // if (this.sqlString === '') {
      //   this.checkSQLFormVisible = false
      //   return
      // }
      // kapConfirm(this.$t('kylinLang.common.willClose'), {
      //   confirmButtonText: this.$t('kylinLang.common.close'),
      //   cancelButtonText: this.$t('kylinLang.common.cancel')
      // }).then(() => {
      //   this.checkSQLFormVisible = false
      // })
      this.checkSQLFormVisible = false
    },
    changeGridModal (val) {
      this.viewModal = val
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
        this.$refs['modelCheckForm'].fields[1].onFieldBlur()
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
    reloadModelList () {
      this.pageCurrentChange(this.currentPage)
    },
    reloadDiagnoseList () {
      var params1 = {pageSize: pageCount, pageOffset: this.currentPage - 1}
      if (localStorage.getItem('selected_project')) {
        params1.project = localStorage.getItem('selected_project')
      }
      return this.loadModelDiagnoseList(params1)
    },
    changeBar (val) {
      this.modelStaticsRange = (val / 100).toFixed(2)
      this.openCollectRange = !!val
    },
    cancelSetModelStatics () {
      this.scanRatioDialogVisible = false
    },
    pageCurrentChange (currentPage) {
      this.currentPage = currentPage
      var params = {pageSize: pageCount, pageOffset: currentPage - 1}
      var params1 = {pageSize: pageCount, pageOffset: currentPage - 1}
      if (localStorage.getItem('selected_project')) {
        params.projectName = localStorage.getItem('selected_project')
        params1.project = localStorage.getItem('selected_project')
      }
      this.loadModels(params).then(() => {
        this.loadModelDiagnoseList(params1)
      }, (res) => {
        handleError(res)
      })
    },
    viewModel (modelInfo) {
      this.$emit('addtabs', 'viewmodel', '[view] ' + modelInfo.name, 'modelEdit', {
        project: modelInfo.project,
        modelName: modelInfo.name,
        uuid: modelInfo.uuid,
        status: modelInfo.is_draft,
        mode: 'view'
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
      var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
      editor && editor.removeListener('change', this.editerChangeHandle)
      this.renderEditerRender(editor)
      this.errorMsg = false
      this.checkSqlLoadBtn = true
      editor.setOption('wrap', 'free')
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
        this.$nextTick(() => {
          var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
          if (editor) {
            editor.setOption('wrap', 'free')
          }
        })
      } else if (command === 'edit') {
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
                        }
                        return
                      }
                    }
                    this.$message({
                      type: 'success',
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
        this.getModelCheckMode(projectName, modelName, () => {
          kapConfirm(this.$t('delModelTip')).then(() => {
            this.drop()
          })
        }, () => {
          this.$message(this.$t('kylinLang.model.modelHasJob'))
        })
      } else if (command === 'cube') {
        this.initCubeMeta()
        this.createCubeVisible = true
        this.cubeMeta.projectName = projectName
        this.cubeMeta.modelName = modelName
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
            this.reloadModelList()
          }, (res) => {
            this.btnLoading = false
            handleError(res)
          })
        }
      })
    },
    _statsModel (para) {
      this.btnLoading = true
      this.statsModel({
        project: this.currentModelData.project,
        modelname: this.currentModelData.name,
        data: {
          // startTime: (new Date(this.modelCheckTime.startTime)).getTime(),
          // endTime: (new Date(this.modelCheckTime.endTime)).getTime(),
          startTime: para.start,
          endTime: para.end,
          ratio: this.modelStaticsRange
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
        this.reloadModelList()
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
    hasPermissionOfProject (project) {
      var projectList = this.$store.state.project.allProject
      var len = projectList && projectList.length || 0
      var projectId = ''
      for (var s = 0; s < len; s++) {
        if (projectList[s].name === project) {
          projectId = projectList[s].uuid
        }
      }
      return hasPermission(this, projectId, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    }
  },
  computed: {
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
    var params = {pageSize: pageCount, pageOffset: 0}
    var params1 = {pageSize: pageCount, pageOffset: 0}
    if (localStorage.getItem('selected_project')) {
      params.projectName = localStorage.getItem('selected_project')
      params1.project = localStorage.getItem('selected_project')
    }
    this.loadModelDiagnoseList(params1).then(() => {
      this.loadModels(params)
    }, (res) => {
      handleError(res)
    })
    var cycleDiagnose = () => {
      window.clearTimeout(this.stCycleRequest)
      this.stCycleRequest = setTimeout(() => {
        if (this.$route.path !== '/studio/model') {
          // cycleDiagnose()
          return
        }
        this.reloadDiagnoseList().then(() => {
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
    'en': {'modelName': 'Model name', 'addCube': 'Add Cube', 'modelUsedTip': 'The model has been used by following cubes, so it cannot be edit for now.', 'inputCloneName': 'Please enter name', 'inputModelName': 'Please enter model name', 'inputCubeName': 'Please enter cube name', 'delModelTip': 'Are you sure to drop this model?', 'hasNotChecked': 'Haven\'t checked health yet.', hasChecked: 'There has been a running model check job. Switch to "Monitor" page to view the progress.', canNotChecked: 'Something went wrong, this model cannot be checked for now. Please contact us for more help here.', chooseStartDate: 'Please select the start time.', chooseEndDate: 'Please select the end time.', verifyModelTip1: '1. This function will help you to verify if the model can answer following SQL statements.', verifyModelTip2: '2. Multiple SQL statements will be separated by ";".', validFail: 'Uh oh, some SQL went wrong. Click the failed SQL to learn why it didn\'t work and how to refine it.', validSuccess: 'Great! All SQL can perfectly work on this model.', selectDate: 'Please select the date.', legalDate: 'Please enter a complete date formatted as YYYY-MM-DD.', timeCompare: 'End time should be later than the start time.'},
    'zh-cn': {'modelName': '模型名称', 'addCube': '添加Cube', 'modelUsedTip': '该模型已经被下列Cube引用，暂时无法被编辑。', 'inputCloneName': '请输入名称。', 'inputModelName': '请输入模型名称。', 'inputCubeName': '请输入Cube名称。', 'delModelTip': '你确认要删除该模型吗？', 'hasNotChecked': '尚未进行健康检测。', hasChecked: '已有一个检测任务正在进行中，您可以去“监控”页面查看进度。', canNotChecked: '该模型暂时无法进行检测，请联系售后人员获得支持。', chooseStartDate: '请选择起始时间。', chooseEndDate: '请选择结束时间。', verifyModelTip1: '1. 系统将帮助您检验以下SQL是否能被本模型回答。', verifyModelTip2: '2. 输入多条SQL语句时将以“；”作为分隔。', validFail: '有无法运行的SQL查询。请点击未验证成功的SQL，获得具体原因与修改建议。', validSuccess: '所有SQL都能被本模型验证。', selectDate: '请选择时间', legalDate: '请输入完整日期，格式为YYYY-MM-DD。', timeCompare: '结束日期应晚于起始时间'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
.modelist_box{
  margin-left: 30px;
  /* min-height:600px; */
  margin-right: 30px;

  .null_pic_box{
    text-align:center;
    padding-top:100px;
    .null_pic_2{
      width:150px;
    }
  }

  .line{
    background: #292b38;
    margin-left: -20px;
    margin-right: -20px;
  }
  .suggestBox{
    border-radius: 4px;
    background-color: #20222e;
    padding: 10px 10px;
    max-height: 200px;
    overflow-y: auto;
    font-size: 12px;
    color:#9DA3B3;
    h3{
      margin-bottom:10px;
      // margin-top: 10px;
      color:#F44236;
      font-size: 12px;
    }
    p{
      margin-bottom: 10px;
    }
  }
  .modelCheck{
    .el-date-editor.el-input{
      width: 100%;
    }
  }
  .el-table--striped .el-table__body tr.el-table__row--striped td {
    background-color: #292b38;
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
  .icon_card {
    background-image: url('../../assets/img/cardlist.png');
    background-size: cover;
  }
  .icon_card:hover,.icon_card.active {
    background-image: url('../../assets/img/cardlisthover.png');
    background-size: cover;
  }
  .icon_table{
    background-image: url('../../assets/img/tablelist.png');
    background-size: cover;
  }
  .icon_table:hover,.icon_table.active{
    background-image: url('../../assets/img/tablelisthover.png');
    background-size: cover;
  }
  h2{
    font-size: 16px;
    .fa-icon{
      /*color: #ccc;*/
    }
  }
  .fa-icon{
    cursor: pointer;
    &.active{
      color:@base-color;
    }
  }
  .el-table {
    font-size: 12px;
    .is_draft {
      td {
        background: #515770!important;
      }
      &>td:first-child {
       &>div{
        background-image: url('../../assets/img/draft.png');
        background-repeat: no-repeat;
        // background-color: #515770;
        // border:dashed 1px @fff;
        background-size: 20px;
        background-position: 90% 80%;
        padding-right:30px;
       }
      }
    }
  }
  .el-card{
    background-color: #393e52;
    border:none;
    border-radius:0;
    &:hover{
      border:solid 1px #58b7ff;
    }
    &.is_draft {
      background-image: url('../../assets/img/draft.png');
      background-repeat: no-repeat;
      background-color: #515771;
      border:dashed 1px #7881AA;
      background-position: 90% 80%;
    }
  }
 h2{
 	color:#fff;
  font-weight: normal;
  cursor:pointer;
  i{
    color:#13ce66;
    font-size: 18px;
    margin-left: 10px;
  }
 }
 .title{
 	 margin-left: 10px;
 	 margin-top: 10px;
 	 color: #9da3b3;
 	 font-size: 14px;
 }
 .el-col {
    margin-bottom: 20px;
    &:last-child {
      margin-bottom: 0;
    }
  }
 .time {
    font-size: 13px;
    color: #9da3b3;
  }
  .el-dropdown{
    float: right;
    margin-right: 8px;
    cursor: pointer;
  }
  .el-table{
    .el-dropdown{
      float: none;
    }
  }
  .bottom {
    margin-top: 45px;
    margin-right: 10px;
    line-height: 12px;
  }
  .view_btn{
  	float: right;
  	/*margin-right: 10px;*/
  	color:#58b7ff;
  }
  .button {
    padding: 0;
    float: right;
    margin-top: -4px;
    margin-right: 10px;
    color:#ccc;
  }
  .button:hover{
    color:#58b7ff;
  }
  .button span{
      font-size: 14px;
    }
   .view_btn{
   	font-size: 20px;
   }
  .image {
    width: 100%;
    display: block;
  }

  .clearfix:before,
  .clearfix:after {
      display: table;
      content: "";
  }

  .clearfix:after {
      clear: both
  }
  .el-dropdown{
    color: #9da3b3;
  }
  .el-dropdown svg:hover{
    color: #fff;
  }
  #addModel{
    background: transparent;
    margin-top: 14px;
  }
  #addModel > span{
    span:first-child{
      margin-right: 5px;
      font-weight: normal;
    }
  }
  #addModel:hover{
    background: @base-color;
  }
}
.add-m{
  .el-form-item{
    margin-bottom: 10px;
  }
  .el-dialog__body{
    padding-top: 15px;
    padding-bottom: 18px;
  }
  .el-input{
    padding: 0;
    margin-top: 15px;
  }
}
</style>
