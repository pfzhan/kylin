<template>
	<div class="paddingbox modelist_box">
    <el-button type="primary" class="ksd-mb-10" @click="addModel">+model</el-button>
    <br/>
		<el-row :gutter="20">
		  <el-col :span="8"  v-for="(o, index) in modelsList" :key="o.uuid" :style="{height:'152px'}">
		    <el-card :body-style="{ padding: '0px'}" style="height:100%">
		      <p class="title">Last updated {{ o.gmtTime }}
					<el-dropdown @command="handleCommand" :id="o.name" trigger="click">
					  <span class="el-dropdown-link">
					    <icon name="ellipsis-h"></icon>
					  </span>
					  <el-dropdown-menu slot="dropdown"  :uuid='o.uuid'>
              <el-dropdown-item command="cube" v-if="!o.status">Add Cube</el-dropdown-item>
					    <el-dropdown-item command="edit">Edit</el-dropdown-item>
					    <el-dropdown-item command="clone" v-if="!o.status">Clone</el-dropdown-item>
					    <el-dropdown-item command="stats" v-if="!o.status">Stats</el-dropdown-item>
              <el-dropdown-item command="drop" >Drop</el-dropdown-item>
					  </el-dropdown-menu>
					</el-dropdown>

		    </p>
		      <div style="padding: 20px;">
		        <h2 :title="o.name" @click="viewModel(o)">{{o.name|omit(24, '...')}} <icon v-if="!o.status" :name="getModelStatusIcon(o)&&getModelStatusIcon(o).icon" :style="{color:getModelStatusIcon(o) && getModelStatusIcon(o).color}"></icon> </h2>
            <el-progress v-visible="getHelthInfo(o.name).progress" :percentage="(getHelthInfo(o.name).progress||0)*100" style="width:150px;"></el-progress>
		        <div class="bottom clearfix">
		          <time class="time" v-visible="o.owner" style="display:block">{{o.owner}}</time>
		          <!-- <div class="view_btn" v-on:click="viewModel(o.name)"><icon name="long-arrow-right" style="font-size:20px"></icon></div> -->
		        </div>
		      </div>
		    </el-card>
		  </el-col>
		</el-row>
		<pager class="ksd-center" ref="pager"  :totalSize="modelsTotal"  v-on:handleCurrentChange='pageCurrentChange' ></pager>

    <el-dialog title="Model Clone" v-model="cloneFormVisible">
      <el-form :model="cloneModelMeta" :rules="cloneFormRule" ref="cloneForm">
        <el-form-item label="Model Name" prop="newName">
          <el-input v-model="cloneModelMeta.newName" auto-complete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="cloneFormVisible = false">取 消</el-button>
        <el-button type="primary" @click="clone">Clone</el-button>
      </div>
    </el-dialog>

    <!-- 添加model -->
    <el-dialog title="Add Model" v-model="createModelVisible" size="tiny">
      <el-form :model="createModelMeta" :rules="createModelFormRule" ref="addModelForm">
        <el-form-item label="Model Name" prop="modelName">
          <el-input v-model="createModelMeta.modelName" auto-complete="off"></el-input>
        </el-form-item>
        <el-form-item label="Model Description" prop="modelDesc">
         <el-input
            type="textarea"
            :rows="2"
            placeholder="请输入内容"
            v-model="createModelMeta.modelDesc">
          </el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createModelVisible = false">取 消</el-button>
        <el-button type="primary" @click="createModel">添 加</el-button>
      </div>
    </el-dialog>

    <!-- 添加cube -->

    <el-dialog title="Add Cube" v-model="createCubeVisible" size="tiny">
      <el-form :model="cubeMeta" :rules="createCubeFormRule" ref="addCubeForm">
        <el-form-item label="Cube Name" prop="cubeName">
          <el-input v-model="cubeMeta.cubeName" auto-complete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createCubeVisible = false">取 消</el-button>
        <el-button type="primary" @click="createCube">添 加</el-button>
      </div>
    </el-dialog>

    <el-dialog title="设置扫描范围" v-model="scanRatioDialogVisible" >
        <el-row :gutter="20">
          <el-col :span="24"><div class="grid-content bg-purple">
            <div class="tree_check_content ksd-mt-20">
              <div class="ksd-mt-20">
                <el-checkbox v-model="openCollectRange">Model Stats</el-checkbox>
                 <el-slider v-model="modelStaticsRange" :max="100" :format-tooltip="formatTooltip" :disabled = '!openCollectRange'></el-slider>
              </div>
              </div>
            </div>
          </el-col>
        </el-row>
        <div slot="footer" class="dialog-footer">
          <el-button @click="cancelSetModelStatics">取 消</el-button>
          <el-button type="primary" @click="stats">确 定</el-button>
        </div>
      </el-dialog>
	</div>
</template>
<script>
import { mapActions } from 'vuex'
import cubeList from '../cube/cube_list'
import { pageCount, modelHealthStatus } from '../../config'
import { transToGmtTime, handleError } from 'util/business'
export default {
  data () {
    return {
      scanRatioDialogVisible: false,
      openCollectRange: false,
      modelStaticsRange: 0,
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
          {required: true, message: '请输入clone后的model名字', trigger: 'blur'},
          {validator: this.checkName, trigger: 'blur'}]
      },
      createModelFormRule: {
        modelName: [
          {required: true, message: '请输入model名字', trigger: 'blur'},
          {validator: this.checkName, trigger: 'blur'}
        ]
      },
      createCubeFormRule: {
        cubeName: [
          {required: true, message: '请输入cube名字', trigger: 'blur'},
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
      checkCubeName: 'CHECK_CUBE_NAME_AVAILABILITY'
    }),
    reloadModelList () {
      this.pageCurrentChange(this.currentPage)
    },
    formatTooltip (val) {
      return val + '%'
    },
    cancelSetModelStatics () {
      this.modelStaticsRange = 0
      this.scanRatioDialogVisible = false
      this.openCollectRange = false
    },
    pageCurrentChange (currentPage) {
      this.currentPage = currentPage
      this.loadModels({pageSize: pageCount, pageOffset: currentPage - 1, projectName: localStorage.getItem('selected_project')})
      this.loadModelDiagnoseList({project: this.project, pageOffset: currentPage - 1, pageSize: pageCount})
    },
    sizeChange () {
    },
    getModelStatusIcon (modelInfo) {
      var helthInfo = this.getHelthInfo(modelInfo.name)
      return modelHealthStatus[helthInfo.heathStatus]
    },
    viewModel (modelInfo) {
      this.$emit('addtabs', 'viewmodel', '[view] ' + modelInfo.name, 'modelEdit', {
        project: modelInfo.project,
        modelName: modelInfo.name,
        uuid: modelInfo.uuid,
        status: modelInfo.status,
        mode: 'view'
      })
    },
    addModel () {
      if (!this.project) {
        this.$message('请先选择一个project')
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
          this.checkModelName(this.createModelMeta.modelName).then((res) => {
            this.$message({
              message: '已经存在同名的model了',
              type: 'warning'
            })
          }, (res) => {
            handleError(res, (data, code, status, msg) => {
              if (status === 400) {
                this.createModelVisible = false
                this.$emit('addtabs', 'model', this.createModelMeta.modelName, 'modelEdit', {
                  project: localStorage.getItem('selected_project'),
                  modelName: this.createModelMeta.modelName,
                  modelDesc: this.createModelMeta.modelDesc,
                  actionMode: 'add'
                })
              } else {
                this.$message({
                  message: msg,
                  type: 'warning'
                })
              }
            })
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
          this.checkCubeName(this.cubeMeta.cubeName).then((data) => {
            this.$message({
              message: '已经存在同名的Cube了',
              type: 'warning'
            })
          }, (res) => {
            handleError(res, (data, code, status, msg) => {
              if (status === 400) {
                this.createCubeVisible = false
                this.$emit('addtabs', 'cube', this.cubeMeta.cubeName, 'cubeEdit', {
                  project: this.cubeMeta.projectName,
                  cubeName: this.cubeMeta.cubeName,
                  modelName: this.cubeMeta.modelName,
                  isEdit: false
                })
              } else {
                this.$message({
                  message: msg,
                  type: 'warning'
                })
              }
            })
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
    handleCommand (command, component) {
      var handleData = component.$parent.$el
      var uuid = handleData.getAttribute('uuid')
      var modelData = this.getModelDataByUuid(uuid)
      this.currentModelData.modelName = modelData.name
      this.currentModelData.project = modelData.project
      var modelName = modelData.name
      var projectName = modelData.project
      if (command === 'edit') {
        this.$emit('addtabs', 'model', modelName, 'modelEdit', {
          project: projectName,
          modelName: modelName,
          uuid: uuid,
          status: modelData.status
        })
      } else if (command === 'clone') {
        this.initCloneMeta()
        this.cloneFormVisible = true
        this.cloneModelMeta.newName = ''
        this.cloneModelMeta.oldName = modelName
        this.cloneModelMeta.project = projectName
        // this.cloneModel(modelName, projectName)
      } else if (command === 'stats') {
        this.scanRatioDialogVisible = true
        // this.stats(projectName, modelName)
      } else if (command === 'drop') {
        this.$confirm('此操作将永久删除该model, 是否继续?', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning'
        }).then(() => {
          this.drop()
        }).catch(() => {
          this.$message({
            type: 'info',
            message: '已取消删除'
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
    clone () {
      this.$refs['cloneForm'].validate((valid) => {
        if (valid) {
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
            this.$message({
              type: 'success',
              message: '克隆成功!'
            })
            this.reloadModelList()
          }, (res) => {
            handleError(res)
          })
        }
      })
    },
    stats (project, modelName) {
      this.statsModel({
        project: this.currentModelData.project,
        modelname: this.currentModelData.modelName,
        data: {
          ratio: this.modelStaticsRange
        }
      }).then(() => {
        this.$message({
          type: 'success',
          message: '提交成功!'
        })
        this.scanRatioDialogVisible = false
      })
    },
    drop (modelName) {
      this.delModel(this.currentModelData.modelName).then(() => {
        this.$message({
          type: 'success',
          message: '删除成功!'
        })
        this.reloadModelList()
      }, (res) => {
        handleError(res)
      })
    },
    checkName (rule, value, callback) {
      if (!/^\w+$/.test(value)) {
        callback(new Error(this.$t('名字格式有误')))
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
    getHelthInfo (modelName) {
      var len = this.modelHelth && this.modelHelth.length || 0
      for (var i = 0; i < len; i++) {
        if (this.modelHelth[i].modelName === modelName) {
          return this.modelHelth[i]
        }
      }
      return {
        progress: 0,
        heathStatus: '',
        message: []
      }
    },
    renderHelthStatusIcon () {
    }
  },
  computed: {
    modelsList () {
      return this.$store.state.model.modelsList.map((m) => {
        m.gmtTime = transToGmtTime(m.last_modified, this)
        return m
      })
    },
    modelsTotal () {
      return this.$store.state.model.modelsTotal
    },
    modelHelth () {
      return this.$store.state.model.modelsDianoseList
    }
  },
  created () {
    this.loadModels({pageSize: pageCount, pageOffset: 0, projectName: this.project})
    this.loadModelDiagnoseList({project: this.project, pageOffset: 0, pageSize: pageCount})
  }
}
</script>
<style lang="less">
.modelist_box{
  .el-card{
    &:hover{
      border:solid 1px #58b7ff;
    }
  }
 h2{
 	color:#475669;
  font-weight: normal;
  cursor:pointer;
  i{
    color:#13ce66;
    font-size: 18px; 
    margin-left: 10px;
  }
 }
 h2:hover{
  color:#58b7ff;
 }
 .title{
 	 margin-left: 20px;
 	 margin-top: 10px;
 	 color: #383838;
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
    color: #999;
  }
  .el-dropdown{
    float: right;
    margin-right: 8px;
    cursor: pointer;
  }
  .bottom {
    margin-top: 10px;
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
}
</style>
