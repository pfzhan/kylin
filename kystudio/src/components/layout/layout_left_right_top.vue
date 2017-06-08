<template>
<div class="fulllayout">
<el-row class="panel layout_left_right_top" :class="briefMenu">
  <el-col :span="24" class="panel-center">
    <aside class="left_menu">
      <img v-show="briefMenu!=='brief_menu'" src="../../assets/img/logo_all.png" class="logo" id="logo" @click="goHome" style="cursor:pointer;">
      <img v-show="briefMenu==='brief_menu'" src="../../assets/img/logo.png" class="logo" @click="goHome" style="cursor:pointer;"><span class="logo_text"></span>
      <el-menu :default-active="defaultActive" class="el-menu-vertical-demo J_menu" @open="handleopen" @close="handleclose" @select="handleselect" theme="dark" unique-opened router>
        <template v-for="(item,index) in menus" >
          <el-menu-item :index="item.path" v-if="showMenuByRole(item.name)" :key="index"><img :src="item.icon"> <span>{{$t('kylinLang.menu.' + item.name)}}</span></el-menu-item>
        </template>
      </el-menu>
    </aside>
    <div class="topbar">
      <icon name="bars" style="color: #d4d7e3;" v-on:click.native="toggleMenu"></icon>
      <project_select  v-if='gloalProjectSelectShow' class="project_select" v-on:changePro="changeProject" ref="projectSelect"></project_select>
      <el-button  v-if='gloalProjectSelectShow' :class="{'isProjectPage':defaultActive==='projectActive'}" @click="goToProjectList"><icon name="window-restore" scale="0.8"></icon></el-button>
      <el-button v-if='gloalProjectSelectShow' @click="addProject" v-show="isModeler"><icon name="plus" scale="0.8"></icon></el-button>

      <ul class="topUl">
        <li><help></help></li>
        <li><change_lang></change_lang></li>
        <li>
          <el-dropdown @command="handleCommand">
            <span class="el-dropdown-link">{{currentUserInfo && currentUserInfo.username}} <i style="font-size:12px;" class="el-icon-caret-bottom"></i>
            </span>
            <el-dropdown-menu slot="dropdown" >
              <el-dropdown-item command="setting">{{$t('kylinLang.common.setting')}}</el-dropdown-item>
              <el-dropdown-item command="loginout">{{$t('kylinLang.common.logout')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </li>
      </ul>
    </div>
    <section class="panel-c-c" id="scrollBox">
      <div class="grid-content bg-purple-light">
        <el-col :span="24" style="margin-bottom:15px;">

          <el-breadcrumb separator="/" >
            <el-breadcrumb-item :to="{ path: '/dashboard' }"><icon class="home_icon" name="home" ></icon></el-breadcrumb-item>
             <el-breadcrumb-item v-if="currentPathName!=''">{{$t('kylinLang.menu.' + currentPath.toLowerCase())}}</el-breadcrumb-item>
            <!-- <el-breadcrumb-item v-if="currentPathNameParent!=''" >{{currentPathNameParent}}</el-breadcrumb-item>	 -->

          </el-breadcrumb>
        </el-col>
        <el-col :span="24" style="box-sizing: border-box;" id="mainBox">
          <!--<transition name="fade">-->
          <router-view v-on:addProject="addProject" v-on:changeCurrentPath="changePath"></router-view>
          <!--</transition>-->
        </el-col>
        <el-dialog title="Project" v-model="FormVisible" @close="resetProjectForm">
          <project_edit :project="project" ref="projectForm" v-on:validSuccess="validSuccess">
          </project_edit>
          <span slot="footer" class="dialog-footer">
            <el-button @click="FormVisible = false">取 消</el-button>
            <el-button type="primary" @click.native="Save">确 定</el-button>
          </span>
        </el-dialog>
      </div>
    </section>
  </el-col>
</el-row>
<el-dialog @close="closeResetPassword" :title="$t('resetPassword')" v-model="resetPasswordFormVisible">
    <reset_password :userDetail="currentUser" ref="resetPassword" v-on:validSuccess="resetPasswordValidSuccess"></reset_password>
    <div slot="footer" class="dialog-footer">
      <el-button @click="resetPasswordFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkResetPasswordForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>

<el-dialog class="linsencebox"
  :title="kapVersion+'试用版'"
  :visible.sync="lisenceDialogVisible"
  :close-on-click-modal="false"
  :modal="false"
  size="tiny">
  <p><span>试用期限: </span>{{kapDate}}<!-- <span>2012<i>/1/2</i></span><span>－</span><span>2012<i>/1/2</i></span> --></p>
  <p class="ksd-mt-20">您的试用期将在<span class="hastime">30</span>天后过期，必须申请正式许可，才能继续使用</p>
  <span slot="footer" class="dialog-footer">
    <el-button @click="getLicense">申请正式许可</el-button>
    <el-button type="primary" @click="lisenceDialogVisible = false">继续使用</el-button>
  </span>
</el-dialog>

  </div>
</template>

<script>
  import { handleSuccess, handleError, kapConfirm, hasRole } from '../../util/business'
  import { objectClone } from '../../util/index'
  import { mapActions, mapMutations } from 'vuex'
  import projectSelect from '../project/project_select'
  import projectEdit from '../project/project_edit'
  import changeLang from '../common/change_lang'
  import help from '../common/help'
  import resetPassword from '../system/reset_password'
  import $ from 'jquery'

  export default {
    data () {
      return {
        project: {},
        FormVisible: false,
        currentPathName: 'DesignModel',
        currentPathNameParent: 'Model',
        defaultActive: '/dashbord',
        lisenceDialogVisible: false,
        currentUserInfo: {
          username: ''
        },
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
          {name: 'studio', path: '/studio/datasource', icon: require('../../assets/img/studio.png')},
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
      this.getCurUserInfo().then((res) => {
        handleSuccess(res, (data) => {
          this.reloadRouter()
          this.getConf()
          this.getEncoding()
          this.getAboutKap()
          this.setCurUser({ user: data })
        })
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
        getAboutKap: 'GET_ABOUTKAP'
      }),
      ...mapMutations({
        setCurUser: 'SAVE_CURRENT_LOGIN_USER'
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
            if (hash.indeOf('studio') >= 0) {
              this.defaultActive = '/studio/datasource'
            }
            if (hash.indeOf('system') >= 0) {
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
        // let cookMenus = Object.create(this.menus)
        let cookMenus = objectClone(this.menus)
        console.log('cookMenus', cookMenus)
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
        location.href = './api/kap/system/requestLicense'
      },
      defaultVal (obj) {
        if (!obj) {
          return 'N/A'
        } else {
          return obj
        }
      },
      changeProject () {
        this.$router.go(0)
        location.reload()
      },
      addProject () {
        this.FormVisible = true
        this.project = {name: '', description: '', override_kylin_properties: {}}
      },
      Save () {
        this.$refs.projectForm.$emit('projectFormValid')
      },
      validSuccess (data) {
        this.saveProject(JSON.stringify(data)).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.saveSuccess')
          })
          localStorage.setItem('selected_project', data.name)
          this.$store.state.project.selected_project = data.name
          this.FormVisible = false
          this.loadAllProjects()
          this.$router.push('/studio/datasource')
          this.defaultActive = '/studio/datasource'
        }, (res) => {
          this.FormVisible = false
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
        // let userPassword = {
        //   username: data.username,
        //   password: data.oldPassword,
        //   newPassword: data.password
        // }
        // this.resetPassword(userPassword).then((result) => {
        //   this.$message({
        //     type: 'success',
        //     message: result.statusText
        //   })
        // }).catch((result) => {
        //   this.$message({
        //     type: 'error',
        //     message: result.statusText
        //   })
        // })
        // this.resetPasswordFormVisible = false
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
      }
    },
    mounted () {
      this.hoverMenu()
    },
    locales: {
      'en': {resetPassword: 'Reset Password', confirmLoginOut: 'Confirm exit?'},
      'zh-cn': {resetPassword: '重置密码', confirmLoginOut: '确认退出吗？'}
    }
  }
</script>

<style lang="less">
  @import '../../less/config.less';
  .fulllayout{
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
      .el-dialog__footer{
          bottom: 0;
          position: absolute;
          right: 0;
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
    margin-left: 26px;
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
		bottom: 0px;
		left: 200px;
		overflow-y: scroll;
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
    .el-button {
      padding: 8px 12px;
      margin-top: 18px;
      margin-left: 4px;
      background: @grey-color;
      color: @fff;
    }
    .el-input{
      margin-right: 30px;
      .el-input__icon{
        margin-right: 32px;
      }
    }
    .topUl {
    	float:right;
    	li{
    		min-width:150px;
    		display:inline-block;
    		height:66px;
    		line-height:66px;
    		text-align:left;
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
	}
  .el-icon-arrow-down:before{
		content: ''
	}

</style>
<style lang="less">
   #mainBox{
      .fullbox{

      }
      .paddingbox{
        padding: 20px;
      }
    }
</style>

