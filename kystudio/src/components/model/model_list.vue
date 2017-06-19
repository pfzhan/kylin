<template>
	<div class="paddingbox modelist_box" style="margin-left: 30px;min-height:600px; margin-right: 30px;">
   <img src="../../assets/img/no_model.png" class="null_pic" v-if="!(modelsList && modelsList.length)">
    <el-button type="default" class="ksd-mb-10" id="addModel" v-if="isAdmin" @click="addModel" style="font-weight: bold;border-radius: 20px;"><span class="add">+</span><span>{{$t('kylinLang.common.model')}}</span></el-button>
    <br/>
    <p class="ksd-right ksd-mb-10" v-if="modelsList&&modelsList.length">
      <span class="icon_card" @click="changeGridModal('card')" :class="{active: viewModal==='card'}"></span>
      <span class="icon_table" @click="changeGridModal('list')"  :class="{active: viewModal!=='card'}"></span>
    </p>
		<el-row :gutter="20" v-if="viewModal==='card'"> 
		  <el-col :span="8"  v-for="(o, index) in modelsList" :key="o.uuid" :style="{height:'152px'}">
		    <el-card :body-style="{ padding: '0px'}" style="height:100%" :class="{'is_draft': o.is_draft}">
		      <p style="font-size: 12px;padding-left: 10px;" class="title">{{$t('kylinLang.model.modifiedGrid')}} {{ o.gmtTime }}
					<el-dropdown style="margin-right: 20px;" @command="handleCommand" :id="o.name" trigger="click"  v-show="isAdmin || hasPermission(o.uuid)">
					  <span class="el-dropdown-link" >
					    <icon name="ellipsis-h"></icon>
					  </span>
					  <el-dropdown-menu slot="dropdown"  :uuid='o.uuid' >
              <el-dropdown-item command="cube" v-if="!o.is_draft">{{$t('addCube')}}</el-dropdown-item>
					    <el-dropdown-item command="edit">{{$t('kylinLang.common.edit')}}</el-dropdown-item>
					    <el-dropdown-item command="clone" v-if="!o.is_draft">{{$t('kylinLang.common.clone')}}</el-dropdown-item>
					    <el-dropdown-item command="stats" v-if="!o.is_draft">{{$t('kylinLang.common.check')}}</el-dropdown-item>
              <el-dropdown-item command="drop" >{{$t('kylinLang.common.drop')}}</el-dropdown-item>
					  </el-dropdown-menu>
					</el-dropdown>
		    </p>
		      <div style="padding: 20px;">
		        <h2 :title="o.name" >
            <el-tooltip class="item" effect="dark" :content="o.name|omit(24, '...')" placement="top">
              <span @click="viewModel(o)">{{o.name|omit(24, '...')}}</span>
            </el-tooltip>
           <common-tip :content="o.diagnose&&o.diagnose.messages.join('<br/>')" v-if="o.diagnose&&o.diagnose.heathStatus!=='RUNNING'"> 
           <icon v-if="!o.is_draft && o.diagnose &&o.diagnose.status!=='RUNNING'" :name="modelHealthStatus[o.diagnose.heathStatus].icon" :style="{color:modelHealthStatus[o.diagnose.heathStatus].color}"></icon></common-tip>
             <el-progress  :width="20" type="circle" :stroke-width="2" :show-text="false" v-if="!o.is_draft&&o.diagnose&&o.diagnose.heathStatus==='RUNNING'" :percentage="o.diagnose&&o.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></h2>
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
    style="width: 100%">
    <el-table-column
      :label="$t('kylinLang.model.modelNameGrid')"
      width="180">
       <template scope="scope" >
         <span @click="viewModel(scope.row)" style="cursor:pointer;">{{scope.row.name}}</span>
       </template>
    </el-table-column>
    <el-table-column
      prop="project"
      :label="$t('kylinLang.common.project')"
      width="180">
    </el-table-column>
    <el-table-column
      prop="owner"
      width="100"
      :label="$t('kylinLang.model.ownerGrid')">
    </el-table-column>
     <el-table-column
      width="130"
      class-name="ksd-center"
      :label="$t('kylinLang.model.statusGrid')">
      <template scope="scope" >
         <!-- <icon v-if="!scope.row.is_draft && scope.row.diagnose && scope.row.diagnose.progress===0" :name="modelHealthStatus[scope.row.diagnose.heathStatus].icon" :style="{color:modelHealthStatus[scope.row.diagnose.heathStatus].color}"></icon> -->
          <common-tip  :content="scope.row.diagnose&&scope.row.diagnose.messaes&&scope.row.diagnose.messaes.join('<br/>')" > <icon v-if="!scope.row.is_draft && scope.row.diagnose && scope.row.diagnose.heathStatus!=='RUNNING' && (scope.row.diagnose.progress===0 || scope.row.diagnose.progress===100)" :name="modelHealthStatus[scope.row.diagnose.heathStatus].icon" :style="{color:modelHealthStatus[scope.row.diagnose.heathStatus].color}"></icon></common-tip>
         <el-progress   :width="20" type="circle" :stroke-width="2" :show-text="false" v-if="!scope.row.is_draft&&scope.row.diagnose&&scope.row.diagnose.heathStatus==='RUNNING'" :percentage="scope.row.diagnose&&scope.row.diagnose.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></h2>
      </template>
    </el-table-column>
    <el-table-column
      prop="fact_table"
      :label="$t('kylinLang.common.fact')">
    </el-table-column>
    <el-table-column
      prop="gmtTime"
      :label="$t('kylinLang.model.modifiedGrid')">
    </el-table-column>
     <el-table-column class="ksd-center" 
      width="100"
      :label="$t('kylinLang.common.action')">
       <template scope="scope">
       <span v-if="!(isAdmin || hasPermission(scope.row.uuid))"> N/A</span>
        <el-dropdown @command="handleCommand" :id="scope.row.name" trigger="click" v-show="isAdmin || hasPermission(scope.row.uuid)">
           <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
         <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid'>
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


		<pager class="ksd-center" ref="pager"  :totalSize="modelsTotal"  v-on:handleCurrentChange='pageCurrentChange' ></pager>

    <el-dialog title="Clone Model" v-model="cloneFormVisible">
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
    <el-dialog title="Add Model" v-model="createModelVisible" size="tiny">
      <el-form :model="createModelMeta" :rules="createModelFormRule" ref="addModelForm">
        <el-form-item prop="modelName" :label="$t('kylinLang.model.modelName')">
          <span slot="label">{{$t('kylinLang.model.modelName')}}
            <common-tip :content="$t('kylinLang.model.modelNameTips')" ><icon name="exclamation-circle"></icon></common-tip>
          </span>
          <el-input v-model="createModelMeta.modelName" auto-complete="off"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.model.modelDesc')" prop="modelDesc">
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
            <common-tip :content="$t('kylinLang.cube.cubeNameTip')" ><icon name="exclamation-circle"></icon></common-tip>
          </span>
          <el-input v-model="cubeMeta.cubeName" auto-complete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createCubeVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="createCube" :loading="btnLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog class="modelCheck" :title="$t('kylinLang.model.checkModel')" size="tiny" v-model="scanRatioDialogVisible" >
        <el-row :gutter="20">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content">
              <div class="tips ksd-mt-10">
                <ul>
                  <li>1.{{$t('kylinLang.model.modelCheckTips1')}}</li>
                  <li>2.{{$t('kylinLang.model.modelCheckTips2')}}</li>
                </ul>
              </div>
              <div class="ksd-mt-20">
              <div class="date-picker" v-if="hasPartition">
                  <h2>{{$t('kylinLang.model.samplingSetting')}}
                      <kap-common-popover>
                      <div slot="content">
                        <ul>
                          <li>{{$t('kylinLang.model.samplingSettingTips')}}</li>
                        </ul>
                      </div>
                      <icon name="question-circle-o"></icon>
                      </kap-common-popover>
                  </h2>
                  <br/>
                  {{$t('kylinLang.model.timeRange')}}
                  <el-row :gutter="20">
                    <el-col :span="11">
                      <div class="grid-content bg-purple">
                        <el-date-picker
                          v-model="startTime"
                          type="datetime"
                          @change="changeStartTime"
                          :placeholder="$t('chooseDate')"
                          size="small"
                          format="yyyy-MM-dd HH:mm"
                          >
                        </el-date-picker>
                      </div>
                    </el-col>
                    <el-col :span="2"><div class="grid-content bg-purple" style="line-height:60px;text-align:center;">－</div></el-col>
                    <el-col :span="11">
                      <div class="grid-content bg-purple">
                        <el-date-picker
                          v-model="endTime"
                          type="datetime"
                          :placeholder="$t('chooseDate')"
                          size="small"
                          format="yyyy-MM-dd HH:mm"
                          :picker-options="pickerOptionsEnd">
                        </el-date-picker>
                      </div>
                    </el-col>
                  </el-row>
                  
                  <span class="line"></span>
                  
                </div>
                <br/>
               <!--  <el-checkbox v-model="openCollectRange">Check Model</el-checkbox> -->
                 <!-- <el-slider v-model="modelStaticsRange" :max="100" :format-tooltip="formatTooltip" :disabled = '!openCollectRange'></el-slider> -->
                 <h2>{{$t('kylinLang.model.samplingPercentage')}}
                 <kap-common-popover>
                    <div slot="content">
                      <ul>
                        <li v-html="$t('kylinLang.model.samplingPercentageTips')"></li>
                      </ul>
                    </div>
                    <icon name="question-circle-o"></icon>
                  </kap-common-popover>
                  </h2>
                 
                  <slider @changeBar="changeBar" :hideCheckbox="true" :range="100" label="Check Model" :show="scanRatioDialogVisible"></slider>
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
	</div>
</template>
<script>
import { mapActions } from 'vuex'
import cubeList from '../cube/cube_list'
import { pageCount, modelHealthStatus, permissions } from '../../config'
import { transToGmtTime, handleError, handleSuccess, kapConfirm, hasRole, hasPermissionOfModel } from 'util/business'
export default {
  data () {
    return {
      modelHealthStatus: modelHealthStatus,
      useCubeDialogVisible: false,
      scanRatioDialogVisible: false,
      openCollectRange: true,
      usedCubes: [],
      // cloneBtnLoading: false,
      btnLoading: false,
      stCycleRequest: null,
      modelStaticsRange: 1,
      startTime: 0,
      endTime: 0,
      pickerOptionsEnd: {
        disabledDate: (time) => {
          let nowDate = new Date(this.startTime)
          let v1 = time.getTime() < +nowDate
          return v1
        }
      },
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
      getModelCheckable: 'MODEL_CHECKABLE'
    }),
    changeGridModal (val) {
      this.viewModal = val
    },
    changeStartTime () {
      this.pickerOptionsEnd.disabledDate = (time) => { // set date-picker endTime
        let nowDate = new Date(this.startTime)
        this.endTime = this.startTime
        // nowDate.setMonth(nowDate.getMonth() + 1)// 后一个月
        // let v1 = time.getTime() > +new Date(_this.startTime) + 30 * 24 * 60 * 60 * 1000
        let v1 = time.getTime() < +nowDate
        // let v2 = time.getTime() < +new Date(this.startTime) - 8.64e7
        // this.maxTime = +nowDate // 缓存最大值 endTime
        return v1
      }
    },
    checkActionRole () {

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
      // this.loadModels(params)
      // this.loadModelDiagnoseList(params1)
      this.loadModels(params).then(() => {
        this.loadModelDiagnoseList(params1)
      })
    },
    sizeChange () {
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
          this.checkModelName(this.createModelMeta.modelName).then((res) => {
            this.btnLoading = false
            handleSuccess(res, (data) => {
              if (data.size === 0) {
                this.createModelVisible = false
                this.$emit('addtabs', 'model', this.createModelMeta.modelName, 'modelEdit', {
                  project: localStorage.getItem('selected_project'),
                  modelName: this.createModelMeta.modelName,
                  modelDesc: this.createModelMeta.modelDesc,
                  actionMode: 'add'
                })
              } else {
                this.$message({
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
          this.checkCubeName(this.cubeMeta.cubeName).then((res) => {
            this.btnLoading = false
            handleSuccess(res, (data) => {
              if (data && data.size > 0) {
                this.$message({
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
    handleCommand (command, component) {
      var handleData = component.$parent.$el
      var uuid = handleData.getAttribute('uuid')
      var modelData = this.getModelDataByUuid(uuid)
      this.currentModelData = modelData
      // this.currentModelData.modelName = modelData.name
      // this.currentModelData.project = modelData.project
      var modelName = modelData.name
      var projectName = modelData.project
      if (command === 'edit') {
        this.getModelCheckMode(projectName, modelName, () => {
          this.isUsedInCubes(modelName, (res) => {
            this.useCubeDialogVisible = true
          }, () => {
            this.$emit('addtabs', 'model', modelName, 'modelEdit', {
              project: projectName,
              modelName: modelName,
              uuid: uuid,
              status: modelData.is_draft
            })
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
                        this.startTime = 0
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
        })
      } else if (command === 'cube') {
        this.initCubeMeta()
        this.createCubeVisible = true
        this.cubeMeta.projectName = projectName
        this.cubeMeta.modelName = modelName
        // this.$prompt('请输入Cube名称', '提示', {
        //   confirmButtonText: '确定',
        //   cancelButtonText: '取消',
        //   inputPattern: /^\w+$/
        // }).then(({ value }) => {
        //   this.checkCubeName(value).then((data) => {
        //     console.log(data, 'ssss')
        //     return false
        //   }, () => {
        //     return false
        //   })
        //   // return false
        //   // this.$emit('addtabs', 'cube', value, 'cubeEdit', {
        //   //   project: projectName,
        //   //   cubeName: value,
        //   //   modelName: modelName,
        //   //   isEdit: false
        //   // })
        // }).catch(() => {
        // })
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
    stats () {
      if (+this.modelStaticsRange === 0) {
        this.$message({
          message: this.$t('kylinLang.common.pleaseSelectSampleRange')
        })
        return
      }
      this.btnLoading = true
      this.statsModel({
        project: this.currentModelData.project,
        modelname: this.currentModelData.name,
        data: {
          startTime: (new Date(this.startTime)).getTime(),
          endTime: (new Date(this.endTime)).getTime(),
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
    isModelAllowCheck (projectName, modelName) {
      return this.getModelCheckable({
        project: projectName,
        modelName: modelName
      })
    },
    drop () {
      this.delModel(this.currentModelData.name).then(() => {
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
      if (!/^\w+$/.test(value)) {
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
      console.log(modelsList)
      for (var k = 0; k < modelsList.length; k++) {
        if (modelsList[k].uuid === uuid) {
          return modelsList[k]
        }
      }
      return []
    },
    // getHelthInfo (modelName) {
    //   var len = this.modelHelth && this.modelHelth.length || 0
    //   for (var i = 0; i < len; i++) {
    //     if (this.modelHelth[i].modelName === modelName) {
    //       this.modelHelth[i].progress = this.modelHelth[i].progress ? Number(this.modelHelth[i].progress).toFixed(2) : 0
    //       return this.modelHelth[i]
    //     }
    //   }
    //   return {
    //     progress: 0,
    //     heathStatus: '',
    //     message: []
    //   }
    // },
    renderHelthStatusIcon () {
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
    hasPermission (modelId) {
      return hasPermissionOfModel(this, modelId, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask, permissions.OPERATION.mask)
    }
  },
  computed: {
    modelsList () {
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
    // console.log(this.project, 112233)
    if (localStorage.getItem('selected_project')) {
      params.projectName = localStorage.getItem('selected_project')
      params1.project = localStorage.getItem('selected_project')
    }
    this.loadModelDiagnoseList(params1).then(() => {
      this.loadModels(params)
    })
    var cycleDiagnose = () => {
      window.clearTimeout(this.stCycleRequest)
      this.stCycleRequest = setTimeout(() => {
        if (this.$route.path !== '/studio/model') {
          cycleDiagnose()
          return
        }
        this.reloadDiagnoseList().then(() => {
          cycleDiagnose()
        }, () => {
          cycleDiagnose()
        })
      }, 10000)
    }
    cycleDiagnose()
  },
  beforeDestroy () {
    window.clearTimeout(this.stCycleRequest)
  },
  locales: {
    'en': {'modelName': 'Model name', 'addCube': 'Add Cube', 'modelUsedTip': 'The model has been used by cubes as follows，you can only view the Model！', 'inputCloneName': 'Please input new name', 'inputModelName': 'Please input model name', 'inputCubeName': 'Please input cube name', 'delModelTip': 'Are you sure to drop this model?', 'hasNotChecked': 'Not checked health yet', hasChecked: 'There has been a running check job!You can go to Monitor page to watch the progress!', canNotChecked: 'This model can not be checked'},
    'zh-cn': {'modelName': '模型名称', 'addCube': '添加Cube', 'modelUsedTip': '该Model已经被下列cube使用过，无法编辑！您可以预览该Model！', 'inputCloneName': '请输入克隆后的名字', 'inputModelName': '请输入model名称', 'inputCubeName': '请输入cube名称', 'delModelTip': '你确认删除该model吗?', 'hasNotChecked': '还未进行健康检测', hasChecked: '已有一个检测作业正在进行中，您可以去Monitor页面查看进度!', canNotChecked: '该模型无法进行检测'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
.modelist_box{
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
      color: #ccc;
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
       }
      }
    }
  }
  .el-card{
    background-color: #393e52;
    border:none;
    &:hover{
      border:solid 1px #58b7ff;
    }
    &.is_draft {
      background-image: url('../../assets/img/draft.png');
      background-repeat: no-repeat;
      background-color: #515770;
      border:dashed 1px @fff;
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
</style>
