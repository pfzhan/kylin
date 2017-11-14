<template>
<div class="fulllayout" id="fullBox">
<el-row class="panel layout_left_right_top" :class="briefMenu" id="top">
  <el-col :span="24" class="panel-center">
    <aside class="left_menu">
      <img v-show="briefMenu!=='brief_menu'" src="../../assets/img/logo_all.png" class="logo" id="logo" @click="goHome" style="cursor:pointer;">
      <img v-show="briefMenu==='brief_menu'" src="../../assets/img/logo.png" class="logo" @click="goHome" style="cursor:pointer;"><span class="logo_text"></span>
      <el-menu :default-active="defaultActive" class="el-menu-vertical-demo J_menu" @open="handleopen" @close="handleclose" @select="handleselect" theme="dark" unique-opened router>
        <template v-for="(item,index) in menus" >
          <el-menu-item :index="item.path" v-if="showMenuByRole(item.name)" :key="index">
              <el-tooltip class="item" v-if="briefMenu==='brief_menu'"  effect="dark" :content="$t('kylinLang.menu.' + item.name)" placement="top">
                <img :src="item.icon">
              </el-tooltip>
            <img :src="item.icon" v-if="briefMenu!=='brief_menu'">
            <span>{{$t('kylinLang.menu.' + item.name)}}</span>
          </el-menu-item>
        </template>
      </el-menu>
    </aside>
    <div class="topbar">
      <icon name="bars" style="color: #d4d7e3;" v-on:click.native="toggleMenu"></icon>
      <project_select  class="project_select" v-on:changePro="changeProject" ref="projectSelect"></project_select>
      <!--  <project_select v-show='gloalProjectSelectShow' class="project_select" v-on:changePro="changeProject" ref="projectSelect"></project_select> -->
      <el-button v-show='gloalProjectSelectShow' :title="$t('kylinLang.project.projectList')" :class="{'isProjectPage':defaultActive==='projectActive'}" @click="goToProjectList"><icon name="window-restore" scale="0.8"></icon></el-button>
      <el-button :title="$t('kylinLang.project.addProject')" @click="addProject" v-show="isAdmin"><icon name="plus" scale="0.8"></icon></el-button>

      <ul class="topUl">
        <li><help></help></li>
        <li><change_lang></change_lang></li>
        <li>
          <el-dropdown @command="handleCommand" trigger="click">
            <span class="el-dropdown-link">{{currentUserInfo && currentUserInfo.username}} <i style="font-size:12px;" class="el-icon-caret-bottom"></i>
            </span>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item command="setting">{{$t('kylinLang.common.setting')}}</el-dropdown-item>
              <el-dropdown-item command="loginout">{{$t('kylinLang.common.logout')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </li>
      </ul>
    </div>
    <div class="panel-c-c" id="scrollBox" >
      <div class="grid-content bg-purple-light">
        <el-col :span="24" style="margin-bottom:15px;" id="breadcrumb_box">
          <!-- 面包屑在dashboard页面不显示 -->
          <el-breadcrumb separator="/" style="margin-left: 30px;" :style="gloalProjectSelectShow ? 'opacity: 100' : 'opacity: 0'">
            <el-breadcrumb-item :to="{ path: '/dashboard' }">
              <!-- <icon class="home_icon" name="home" ></icon> -->
              <span style="color: #fff;">KAP</span>
            </el-breadcrumb-item>
            <el-breadcrumb-item v-if="currentPathName!=''">
              <span style="color: #fff;">{{$t('kylinLang.menu.' + currentPath.toLowerCase())}}</span>
            </el-breadcrumb-item>
            <!-- <el-breadcrumb-item v-if="currentPathNameParent!=''" >{{currentPathNameParent}}</el-breadcrumb-item>  -->
          </el-breadcrumb>
        </el-col>
        <el-col :span="24" style="box-sizing: border-box;" id="mainBox">
          <!--<transition name="fade">-->
          <router-view v-on:addProject="addProject" v-on:changeCurrentPath="changePath"></router-view>
          <!--</transition>-->
        </el-col>
        <el-dialog :title="$t('kylinLang.common.project')" v-model="FormVisible" @close="resetProjectForm" class="add-project">
          <project_edit :project="project" ref="projectForm" v-on:validSuccess="validSuccess" :visible="FormVisible" :isEdit="isEdit">
          </project_edit>
          <span slot="footer" class="dialog-footer">
            <el-button @click="FormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
            <el-button type="primary" @click.native="Save" :loading="projectSaveLoading">{{$t('kylinLang.common.ok')}}</el-button>
          </span>
        </el-dialog>
      </div>
    </div>
  </el-col>
</el-row>
<el-dialog @close="closeResetPassword" size="tiny" :title="$t('resetPassword')" v-model="resetPasswordFormVisible">
    <reset_password :curUser="currentUser" ref="resetPassword" :show="resetPasswordFormVisible" v-on:validSuccess="resetPasswordValidSuccess"></reset_password>
    <div slot="footer" class="dialog-footer">
      <el-button @click="resetPasswordFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" @click="checkResetPasswordForm">{{$t('kylinLang.common.ok')}}</el-button>
    </div>
  </el-dialog>

<el-dialog class="linsencebox"
  :title="kapVersion"
  v-model="lisenceDialogVisible"
  :modal="false"
  size="tiny">
  <p><span>{{$t('validPeriod')}}</span>{{kapDate}}<!-- <span>2012<i>/1/2</i></span><span>－</span><span>2012<i>/1/2</i></span> --></p>
  <p class="ksd-pt-10">{{$t('overtip1')}}<span class="hastime">{{lastTime}} </span>{{$t('overtip2')}}</p>
  <span slot="footer" class="dialog-footer">
    <el-button @click="getLicense">{{$t('applayLisence')}}</el-button>
    <el-button type="primary" @click="lisenceDialogVisible = false">{{$t('continueUse')}}</el-button>
  </span>
</el-dialog>

  </div>
</template>

<script>
  // import { handleSuccess, handleError, kapConfirm, hasRole } from '../../util/business'
  import { handleError, kapConfirm, hasRole } from '../../util/business'
  import { objectClone, isFireFox } from '../../util/index'
  import { mapActions, mapMutations } from 'vuex'
  import projectSelect from '../project/project_select'
  import projectEdit from '../project/project_edit'
  import changeLang from '../common/change_lang'
  import help from '../common/help'
  import resetPassword from '../system/reset_password'
  import $ from 'jquery'
  // import Scrollbar from 'smooth-scrollbar'
  export default {
    data () {
      return {
        projectSaveLoading: false,
        project: {},
        isEdit: false,
        FormVisible: false,
        currentPathName: 'DesignModel',
        currentPathNameParent: 'Model',
        defaultActive: '/dashbord',
        lisenceDialogVisible: false,
        currentUserInfo: {
          username: ''
        },
        overlock: false,
        form: {
          name: '',
          region: '',
          date1: '',
          date2: '',
          delivery: false,
          type: [],
          resource: '',
          desc: ''
        },
        menus: [
          {name: 'dashboard', path: '/dashboard', icon: require('../../assets/img/dashboard.png')},
          {name: 'studio', path: '/studio/model', icon: require('../../assets/img/studio.png')},
          {name: 'insight', path: '/insight', icon: require('../../assets/img/insight.png')},
          {name: 'monitor', path: '/monitor', icon: require('../../assets/img/monitor.png')},
          {name: 'system', path: '/system/config', icon: require('../../assets/img/system.png')}
        ],
        resetPasswordFormVisible: false
      }
    },
    components: {
      'project_select': projectSelect,
      'project_edit': projectEdit,
      'change_lang': changeLang,
      'reset_password': resetPassword,
      help
    },
    created () {
      /* this.getCurUserInfo().then((res) => {
        handleSuccess(res, (data) => {
          this.reloadRouter()
          this.getConf()
          this.getEncoding()
          this.getAboutKap()
          this.setCurUser({ user: data })
        })
      }) */
      // this.overlock = localStorage.getItem('buyit')
      this.reloadRouter()
      this.getEncoding().then(() => {}, (res) => {
        handleError(res)
      })
      this.getAboutKap(() => {}, (res) => {
        handleError(res)
      })
    },
    methods: {
      ...mapActions({
        loginOut: 'LOGIN_OUT',
        saveProject: 'SAVE_PROJECT',
        getConf: 'GET_CONF',
        getCurUserInfo: 'USER_AUTHENTICATION',
        getEncoding: 'GET_ENCODINGS',
        loadProjects: 'LOAD_PROJECT_LIST',
        loadAllProjects: 'LOAD_ALL_PROJECT',
        resetPassword: 'RESET_PASSWORD',
        getAboutKap: 'GET_ABOUTKAP',
        getProjectEndAccess: 'GET_PROJECT_END_ACCESS',
        getUserAccess: 'USER_ACCESS'
      }),
      ...mapMutations({
        setCurUser: 'SAVE_CURRENT_LOGIN_USER',
        resetProjectState: 'RESET_PROJECT_STATE'
      }),
      showMenuByRole (menuName) {
        if (menuName === 'system' && this.isAdmin === false) {
          return false
        }
        return true
      },
      reloadRouter () {
        let hash = location.hash.replace(/#/, '')
        var matched = false
        for (let i = 0; i < this.menus.length; i++) {
          if (hash.indexOf(this.menus[i].name) >= 0) {
            this.currentPathName = this.menus[i].name
            this.defaultActive = hash
            if (this.menus[i].name === 'studio') {
              this.defaultActive = '/studio/datasource'
            }
            if (this.menus[i].name === 'system') {
              this.defaultActive = ''
              this.$nextTick(() => {
                this.defaultActive = '/system/config'
              })
            }
            matched = true
            break
          }
        }
        if (!matched) {
          if (hash === '/project') {
            this.defaultActive = 'projectActive'
          } else {
            this.defaultActive = hash
            if (hash.indexOf('studio') >= 0) {
              this.defaultActive = '/studio/datasource'
            }
            if (hash.indexOf('system') >= 0) {
              this.defaultActive = ''
              this.$nextTick(() => {
                this.defaultActive = '/system/config'
              })
            }
          }
        }
      },
      changePath (path) {
        this.defaultActive = path
      },
      hoverMenu () {
        let imgSrc = ''
        let index = 0
        let _this = this
        let cookMenus = objectClone(this.menus)
        $('.J_menu').on('mouseenter', 'li', function () {
          let $this = $(this)
          index = $this.index()
          if (index === 0) {
            imgSrc = require('../../assets/img/dashboard_hover.png')
          } else if (index === 1) {
            imgSrc = require('../../assets/img/studio_hover.png')
          } else if (index === 2) {
            imgSrc = require('../../assets/img/insight_hover.png')
          } else if (index === 3) {
            imgSrc = require('../../assets/img/monitor_hover.png')
          } else if (index === 4) {
            imgSrc = require('../../assets/img/system_hover.png')
          }
          _this.menus[index].icon = imgSrc
        })
        $('.J_menu').on('mouseleave', 'li', function () {
          var index = $(this).index()
          _this.$set(_this.menus[index], 'icon', cookMenus[index].icon)
        })
      },
      getLicense () {
        location.href = 'mailto:g-ent-lic@kyligence.io'
      },
      defaultVal (obj) {
        if (!obj) {
          return 'N/A'
        } else {
          return obj
        }
      },
      _getUuidFromProjects (list, projectName) {
        var uuid = list[0].uuid
        for (var i = 0; i < list.length; i++) {
          var item = list[i]
          if (item.name === projectName) {
            uuid = item.uuid
            break
          }
        }
        return uuid
      },
      _replaceRouter (currentPath) {
        if (currentPath.indexOf('dashboard') < 0) {
          this.$router.replace('/')
        } else {
          this.$router.replace('/system/config')
        }
        this.$nextTick(() => {
          this.$router.replace(currentPath)
        })
      },
      _isAjaxProjectAcess (allProject, curProjectName, curProjectId, currentPath) {
        let uuid = allProject.length > 0 ? this._getUuidFromProjects(allProject, curProjectName) : null
        // 当前project的权限没拿过才需要发请求,project list 空的话，uuid也就不存在，就直接跳
        if (uuid && !curProjectId[uuid]) {
          let curProjectEndAccessPromise = this.getProjectEndAccess(uuid)
          let curProjectUserAccess = this.getUserAccess({project: curProjectName})
          Promise.All([curProjectEndAccessPromise, curProjectUserAccess]).then(() => {
            this._replaceRouter(currentPath)
          })
        } else {
          this._replaceRouter(currentPath)
        }
      },
      changeProject (val) {
        var currentPath = this.$router.currentRoute.path
        /* if (currentPath.indexOf('dashboard') < 0) {
          this.$router.replace('/')
        } else {
          this.$router.replace('/system/config')
        }
        this.$nextTick(() => {
          this.$router.replace(currentPath)
        }) */
        this._isAjaxProjectAcess(this.$store.state.project.allProject, val, this.$store.state.project.projectEndAccess, currentPath)
      },
      addProject () {
        this.isEdit = false
        this.FormVisible = true
        this.project = {name: '', description: '', override_kylin_properties: {}}
      },
      Save () {
        this.$refs.projectForm.$emit('projectFormValid')
      },
      validSuccess (data) {
        this.projectSaveLoading = true
        this.saveProject(JSON.stringify(data)).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.saveSuccess')
          })
          localStorage.setItem('selected_project', data.name)
          this.$store.state.project.selected_project = data.name
          this.FormVisible = false
          this.projectSaveLoading = false
          this.loadAllProjects().then(() => {
            this.$nextTick(() => {
              this.defaultActive = '/studio/datasource'
              this.$router.push('/studio/datasource')
            })
          })
        }, (res) => {
          this.FormVisible = false
          this.projectSaveLoading = false
          handleError(res)
        })
      },
      onSubmit () {
      },
      handleopen () {
      },
      handleclose () {
      },
      handleselect: function (a, b) {
        if (a !== '/project') {
          this.defaultActive = a
        }
      },
      toggleMenu: function () {
        this.$store.state.config.layoutConfig.briefMenu = this.$store.state.config.layoutConfig.briefMenu ? '' : 'brief_menu'
        localStorage.setItem('menu_type', this.$store.state.config.layoutConfig.briefMenu)
      },
      logoutConfirm: function () {
        return kapConfirm(this.$t('confirmLoginOut'))
      },
      handleCommand (command) {
        if (command === 'loginout') {
          this.logoutConfirm().then(() => {
            this.loginOut().then(() => {
              localStorage.setItem('buyit', false)
              // reset 所有的project信息
              this.resetProjectState()
              this.$router.push({name: 'Login'})
            })
          })
        } else if (command === 'setting') {
          this.resetPasswordFormVisible = true
        }
      },
      goToProjectList () {
        this.defaultActive = 'projectActive'
        this.$router.push({name: 'Project'})
      },
      goHome () {
        $('.el-menu').find('li').eq(0).click()
      },
      closeResetPassword: function () {
        this.$refs['resetPassword'].$refs['resetPasswordForm'].resetFields()
      },
      checkResetPasswordForm: function () {
        this.$refs['resetPassword'].$emit('resetPasswordFormValid')
      },
      resetPasswordValidSuccess: function (data) {
        let userPassword = {
          username: data.username,
          password: data.oldPassword,
          newPassword: data.password
        }
        this.resetPassword(userPassword).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.updateSuccess')
          })
        }).catch((res) => {
          handleError(res)
        })
        this.resetPasswordFormVisible = false
      },
      resetProjectForm () {
        this.$refs['projectForm'].$refs['projectForm'].resetFields()
      }
    },
    computed: {
      isAdmin () {
        return hasRole(this, 'ROLE_ADMIN')
      },
      briefMenu () {
        return this.$store.state.config.layoutConfig.briefMenu
      },
      gloalProjectSelectShow () {
        return this.$store.state.config.layoutConfig.gloalProjectSelectShow
      },
      currentPath () {
        return this.$store.state.config.routerConfig.currentPathName
      },
      currentUser () {
        this.currentUserInfo = this.$store.state.user.currentUser
        let info = Object.create(this.currentUserInfo)
        info.password = ''
        info.confirmPassword = ''
        return info
      },
      kapInfo () {
        return this.$store.state.system.serverAboutKap
      },
      kapDate () {
        return this.defaultVal(this.kapInfo && this.kapInfo['kap.dates'] || null)
      },
      kapVersion () {
        return this.defaultVal(this.kapInfo && this.kapInfo['kap.version'] || null)
      },
      isModeler () {
        return hasRole(this, 'ROLE_MODELER')
      },
      serverAboutKap () {
        return this.$store.state.system.serverAboutKap
      },
      lastTime () {
        var date = this.serverAboutKap && this.serverAboutKap['kap.dates'] || ''
        var splitTime = date.split(',')
        if (splitTime.length >= 2) {
          var nowdate = new Date()
          nowdate.setMonth(nowdate.getMonth() + 1)
          var endTime = splitTime[1]
          var ms = (new Date(endTime + ' 23:59:59')) - (new Date(nowdate))
          if (ms <= 0) {
            var lastTimes = (new Date(endTime + ' 23:59:59')) - (new Date())
            var days = Math.ceil(lastTimes / 1000 / 60 / 60 / 24)
            // var limittime = Math.round(Math.abs(days))
            if (days >= 0) {
              days = Math.ceil(Math.abs(days))
              if (!this.$store.state.config.overLock) {
                this.lisenceDialogVisible = true
                this.$store.state.config.overLock = true
              }
              localStorage.setItem('buyit', true)
            } else {
              days = 0
            }
            return days
          }
        }
        return 0
      }
    },
    mounted () {
      this.hoverMenu()
      if (isFireFox()) {
        // Scrollbar.init(document.getElementById('scrollBox'))
      }
    },
    locales: {
      'en': {resetPassword: 'Reset Password', confirmLoginOut: 'Are you sure to exit?', validPeriod: 'Valid Period: ', overtip1: 'This Evaluation License will be expired in ', overtip2: 'days. Please contact sales support to apply for the Enterprise License.', applayLisence: 'Apply for Enterprise License', 'continueUse': 'I Know'},
      'zh-cn': {resetPassword: '重置密码', confirmLoginOut: '确认退出吗？', validPeriod: '使用期限: ', overtip1: '当前使用的试用版许可证将在 ', overtip2: '天后过期。欢迎联系销售支持人员申请企业版许可证。', applayLisence: '申请企业版许可证', 'continueUse': '我知道了'}
    }
  }
</script>

<style lang="less">
  @import '../../less/config.less';
  .fulllayout{
    .add-project {
      .el-dialog {
        width: 390px;
        .el-dialog__header {
          font-family:Montserrat-Regular;
          font-size:14px;
          color:#ffffff;
          letter-spacing:0;
          line-height:16px;
          text-align:left;
          span {
            line-height: 29px;
          }
          .el-dialog__headerbtn{
            line-height: 29px;
            margin-top: 0px;
          }
        }
        .el-dialog__body {
          padding: 15px 20px 0px 20px;
          min-height: 90px;
        }
      }
    }
    .linsencebox{
      i{
        font-size: 18px;
        font-weight: bold;
      }
      .hastime{
        font-size: 28px;
        color:@base-color;
      }
      .el-dialog{
        top:initial!important;
        bottom:0;
        right:0;
        left:initial;
        height: 240px;
        width: 400px;
        margin-bottom: 0;
        margin-right: 0;
        transform:initial;
        .el-dialog__title{
          font-size: 12px;
        }
        .el-dialog__body{
          font-size: 12px;
        }
        .el-dialog__header .el-dialog__headerbtn{
          margin-top: 0;
        }
        .el-dialog__footer{
            bottom: 0;
            position: absolute;
            right: 0;
            width: 100%;
        }
      }
    }
  }
   .brief_menu {
      .logo_text {
        display: none;
      }
      .logo{
        margin: 16px 10px 10px 10px;
      }
      .left_menu {
        width: 100px;
        text-align:center;
        img.logo {
          cursor: pointer;
        }
      }
      .topbar > svg {
        margin-left: 124px;
      }


      .panel-c-c{
        left: 100px;
      }
      .el-menu {
        li {
          text-align: center;
          span {
            display: none;
          }

        }
      }
   }
  .el-breadcrumb{
    margin-left: 0;
    margin-top: 20px;
  }
  .isProjectPage {
    color: @base-color;
  }
  .logo_text{
    color:#fff;
    font-size: 24px;
    vertical-align: middle;
    font-weight: 500;
    display: inline-block;
    margin-top: 2px;
  }
	.fade-enter-active,
	.fade-leave-active {
		transition: opacity .5s
	}

	.fade-enter,
	.fade-leave-active {
		opacity: 0
	}

	.panel {
		position: absolute;
		top: 0px;
		bottom: 0px;
		width: 100%;
	}

	.panel-top {
		height: 60px;
		line-height: 60px;
		background: #1F2D3D;
		color: #c0ccda;
	}

	.panel-center {
		// background: #324057;
		position: absolute;
		top: 0px;
		bottom: 0px;
		overflow: hidden;
	}

	.panel-c-c {
		// background: #f1f2f7;
		position: absolute;
		right: 0px;
		top: 67px;
		bottom: -16px;
		left: 200px;
		overflow-y: auto;
	}

	.logout {
		/*background: url(../assets/logout_36.png);*/
		background-size: contain;
		width: 20px;
		height: 20px;
		float: left;
	}

	.logo {
		height: 40px;
    vertical-align: middle;
		z-index:999;
		margin: 13px 10px 13px 20px;
	}

	.tip-logout {
		float: right;
		margin-right: 20px;
		padding-top: 5px;
	}

	.tip-logout i {
		cursor: pointer;
	}

	.admin {
		color: #c0ccda;
		text-align: center;
	}
  .home_icon{
      margin-top: -4px;
    }
  .el-menu-vertical-demo {
    height: 100%;
    background: @bg-top;
    font-size: 12px;
    color: #d0d0d0;
    border-radius: 0;
    .el-menu-item {
      border-left: 4px solid @bg-top;
    }
    .el-menu-item:hover {
      background: none;
      span {
        color: #fff;
      }
    }
  }
	.topbar{
		height: 66px;
		width: 100%;
		background: @bg-top;
		position: absolute;
    top:0;
    >svg{
    	margin-left:224px;
    	margin-top:24px;
    	cursor:pointer;
      float: left;
    }
    .project_select{
    	margin-left: 20px;
      margin-top: 16px;
      float: left;
    }
    .el-dropdown{
    	cursor:pointer;
      color: #fff;
      svg{
    	vertical-align:middle;
    	margin-left:2px;
      }
    }
    >.el-button {
      padding: 8px 12px;
      margin-top: 18px;
      margin-left: 8px;
      background: @grey-color;
      color: @fff;
      border: none;
    }
    >.el-input{
      margin-right: 30px;
      .el-input__icon{
        margin-right: 32px;
      }
    }
    .topUl {
      margin-right: 30px;
    	float:right;
    	li{
    		width:120px;
    		display:inline-block;
    		height:66px;
    		line-height:66px;
    		text-align:right;
    	}
      li:nth-child(2){
        // margin-left: -30px;
      }
      span{
        color: rgba(255,255,255,0.7);
      }
      span:hover{
        color: #fff;
      }
    }
	}
	.el-menu{
    padding-top: 40px;
		li{
      text-align: left;
      span{
        color: #c0ccda;
        font-size: 14px;
      }
			img{
        vertical-align: middle;
        width: 20px;
        height: 20px;
        margin-right: 10px;
      }
		}
		.is-active{
		  border-left: solid 4px #20a0ff;
      color: #fff;
		}
	}
	.left_menu{
		position:relative;
		z-index:999;
		background-color: #218fea;
    width: 200px;
    height: 100%;
    box-shadow: 0 12px 35px 0 rgba(0,0,0,.12), 0 0 16px 0 rgba(0,0,0,.04);
	}
  .el-icon-arrow-down:before{
		content: ''
	}
  #scrollBox{
    background: @tableBC;
  }
</style>

