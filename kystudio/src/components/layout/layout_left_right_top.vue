<template>
  <div class="full-layout" id="fullBox">
  <el-row class="panel" :class="briefMenu">
    <el-col :span="24" class="panel-center">
      <aside class="left-menu">
        <img v-show="briefMenu!=='brief_menu'" src="../../assets/img/big_logo.png" class="logo" @click="goHome">
        <img v-show="briefMenu==='brief_menu'" src="../../assets/img/small_logo.png" class="logo" @click="goHome">
        <div class="ky-line"></div>
        <el-menu :default-active="defaultActive" id="menu-list" @select="handleselect" unique-opened router :collapse="briefMenu==='brief_menu'">
          <template v-for="(item,index) in menus" >
            <el-menu-item :index="item.path" v-if="!item.children && showMenuByRole(item.name)" :key="index">
              <i :class="item.icon" class="ksd-fs-16"></i>
              <span slot="title">{{$t('kylinLang.menu.' + item.name)}}</span>
            </el-menu-item>
            <el-submenu :index="item.path" v-if="item.children">
              <template slot="title">
                <i :class="item.icon" class="ksd-fs-16"></i>
                <span>{{$t('kylinLang.menu.' + item.name)}}</span><div v-if="item.name === 'studio'" class="dot-icon"></div>
              </template>
              <el-menu-item-group>
                <el-menu-item :index="child.path" v-for="child in item.children" :key="child.path">{{$t('kylinLang.menu.' + child.name)}}<div class="number-icon" v-if="child.name === 'model'">2</div></el-menu-item>
              </el-menu-item-group>
            </el-submenu>
          </template>
        </el-menu>
      </aside>
      <div class="topbar">
        <i class="ksd-fs-14" :class="[briefMenu !== 'brief_menu' ? 'el-icon-ksd-grid_01' : 'el-icon-ksd-grid_02']" @click="toggleMenu"></i>
        <project_select v-on:changePro="changeProject" ref="projectSelect"></project_select>
        <el-button v-show='gloalProjectSelectShow' :title="$t('kylinLang.project.projectList')" :class="{'project-page':defaultActive==='projectActive'}" @click="goToProjectList" size="medium">
          <i class="el-icon-ksd-project_list"></i>
        </el-button>
        <el-button :title="$t('kylinLang.project.addProject')" @click="addProject" v-show="isAdmin" size="medium">
          <i class="el-icon-plus"></i>
        </el-button>

        <ul class="top-ul ksd-fright">
          <li v-if="isAdmin"><canary></canary></li>
          <!-- <li><help></help></li> -->
          <li><change_lang ref="changeLangCom"></change_lang></li>
          <li>
            <el-dropdown @command="handleCommand" trigger="click">
              <span class="el-dropdown-link  ksd-ml-10">
                <i class="el-icon-ksd-user ksd-mr-10" style="transform:scale(1.7)"></i>
                {{currentUserInfo && currentUserInfo.username}}
                <i class="el-icon-caret-bottom"></i>
              </span>
              <el-dropdown-menu slot="dropdown">
                <el-dropdown-item command="setting">{{$t('kylinLang.common.setting')}}</el-dropdown-item>
                <el-dropdown-item command="loginout">{{$t('kylinLang.common.logout')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </li>
        </ul>
      </div>
      <div class="panel-content" id="scrollBox" >
        <div class="grid-content bg-purple-light">
          <el-col :span="24" style="padding:20px 0; background: #f9fafc;" v-show="gloalProjectSelectShow">
            <!-- 面包屑在dashboard页面不显示 -->
            <el-breadcrumb separator="/" class="ksd-ml-30">
              <el-breadcrumb-item>
                <span>{{$t('kylinLang.menu.' + currentRouterNameArr[0])}}</span>
              </el-breadcrumb-item>
              <el-breadcrumb-item v-if="currentRouterNameArr[1]" :to="{ path: '/' + currentRouterNameArr[0] + '/' + currentRouterNameArr[1]}">
                <span>{{$t('kylinLang.menu.' + currentRouterNameArr[1])}}</span>
              </el-breadcrumb-item>
              <el-breadcrumb-item v-if="currentRouterNameArr[2]" >
                {{currentRouterNameArr[2]}}
              </el-breadcrumb-item> 
            </el-breadcrumb>
          </el-col>
          <el-col :span="24" class="main-content">
            <!--<transition name="fade">-->
            <router-view v-on:addProject="addProject" v-on:changeCurrentPath="changePath"></router-view>
            <!--</transition>-->
          </el-col>
          <el-dialog :title="$t('kylinLang.common.project')" :visible.sync="FormVisible" @close="resetProjectForm" class="add-project" width="440px">
            <project_edit :project="project" ref="projectForm" v-on:validSuccess="validSuccess" :visible="FormVisible" :isEdit="isEdit">
            </project_edit>
            <span slot="footer" class="dialog-footer">
              <el-button size="medium" @click="FormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
              <el-button type="primary" plain size="medium" @click.native="Save" :loading="projectSaveLoading">{{$t('kylinLang.common.ok')}}</el-button>
            </span>
          </el-dialog>
        </div>
      </div>
    </el-col>
  </el-row>
  <el-dialog @close="closeResetPassword" :title="$t('resetPassword')" :visible.sync="resetPasswordFormVisible" width="440px">
      <reset_password :curUser="currentUser" ref="resetPassword" :show="resetPasswordFormVisible" v-on:validSuccess="resetPasswordValidSuccess"></reset_password>
      <div slot="footer" class="dialog-footer">
        <el-button @click="resetPasswordFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="checkResetPasswordForm">{{$t('kylinLang.common.ok')}}</el-button>
      </div>
  </el-dialog>
  <el-dialog class="linsencebox"
    :title="kapVersion"
    width="440px"
    :visible.sync="lisenceDialogVisible"
    :modal="false">
    <p><span>{{$t('validPeriod')}}</span>{{kapDate}}<!-- <span>2012<i>/1/2</i></span><span>－</span><span>2012<i>/1/2</i></span> --></p>
    <p class="ksd-pt-10">{{$t('overtip1')}}<span class="hastime">{{lastTime}} </span>{{$t('overtip2')}}</p>
    <span slot="footer" class="dialog-footer">
      <el-button @click="getLicense">{{$t('applayLisence')}}</el-button>
      <el-button type="primary" plain @click="lisenceDialogVisible = false">{{$t('continueUse')}}</el-button>
    </span>
  </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
// import { handleSuccess, handleError, kapConfirm, hasRole } from '../../util/business'
import { handleError, kapConfirm, hasRole, hasPermission } from '../../util/business'
import { objectClone, getQueryString } from '../../util/index'
import { permissions, menusData } from '../../config'
import { mapActions, mapMutations, mapGetters } from 'vuex'
import projectSelect from '../project/project_select'
import projectEdit from '../project/project_edit'
import changeLang from '../common/change_lang'
import help from '../common/help'
import canary from '../security/canary'
import resetPassword from '../security/reset_password'
import $ from 'jquery'
// import Scrollbar from 'smooth-scrollbar'
@Component({
  methods: {
    ...mapActions({
      loginOut: 'LOGIN_OUT',
      saveProject: 'SAVE_PROJECT',
      getCurUserInfo: 'USER_AUTHENTICATION',
      // for newten
      // getEncoding: 'GET_ENCODINGS',
      loadProjects: 'LOAD_PROJECT_LIST',
      loadAllProjects: 'LOAD_ALL_PROJECT',
      resetPassword: 'RESET_PASSWORD',
      getAboutKap: 'GET_ABOUTKAP',
      getUserAccess: 'USER_ACCESS'
    }),
    ...mapMutations({
      setCurUser: 'SAVE_CURRENT_LOGIN_USER',
      resetProjectState: 'RESET_PROJECT_STATE',
      resetMonitorState: 'RESET_MONITOR_STATE'
    }),
    ...mapActions('UserEditModal', {
      callUserEditModal: 'CALL_MODAL'
    })
  },
  components: {
    'project_select': projectSelect,
    'project_edit': projectEdit,
    'change_lang': changeLang,
    'reset_password': resetPassword,
    help,
    canary
  },
  computed: {
    ...mapGetters([
      'currentPathNameGet'
    ])
  },
  locales: {
    'en': {resetPassword: 'Reset Password', confirmLoginOut: 'Are you sure to exit?', validPeriod: 'Valid Period: ', overtip1: 'This License will be expired in ', overtip2: 'days. Please contact sales support to apply for the Enterprise License.', applayLisence: 'Apply for Enterprise License', 'continueUse': 'I Know'},
    'zh-cn': {resetPassword: '重置密码', confirmLoginOut: '确认退出吗？', validPeriod: '使用期限: ', overtip1: '当前使用的许可证将在 ', overtip2: '天后过期。欢迎联系销售支持人员申请企业版许可证。', applayLisence: '申请企业版许可证', 'continueUse': '我知道了'}
  }
})
export default class LayoutLeftRightTop extends Vue {
  projectSaveLoading = false
  project = {}
  isEdit = false
  FormVisible = false
  currentPathName = 'DesignModel'
  currentPathNameParent = 'Model'
  defaultActive = ''
  lisenceDialogVisible = false
  currentUserInfo = {
    username: ''
  }
  overlock = false
  form = {
    name: '',
    region: '',
    date1: '',
    date2: '',
    delivery: false,
    type: [],
    resource: '',
    desc: ''
  }
  menus = menusData
  resetPasswordFormVisible = false

  created () {
    // this.reloadRouter()
    this.defaultActive = this.$route.path || '/overview'
    // for newten
    // this.getEncoding().then(() => {}, (res) => {
    //   handleError(res)
    // })
    // for newten
    // this.getAboutKap(() => {}, (res) => {
    //   handleError(res)
    // })
  }
  showMenuByRole (menuName) {
    if ((menuName === 'system' && this.isAdmin === false) || (menuName === 'monitor' && !this.hasPermissionWithoutQuery && !this.isAdmin) || (menuName === 'auto' && this.isAdmin === false && !this.hasAdminPermissionOfProject)) {
      return false
    }
    return true
  }
  changePath (path) {
    this.defaultActive = path
  }
  getLicense () {
    location.href = 'mailto:g-ent-lic@kyligence.io'
  }
  defaultVal (obj) {
    if (!obj) {
      return 'N/A'
    } else {
      return obj
    }
  }
  _replaceRouter (currentPath) {
    this.$router.replace('/refresh')
    this.$nextTick(() => {
      if ((currentPath === '/monitor' && !this.hasPermissionWithoutQuery && !this.isAdmin) || (currentPath === '/auto' && this.isAdmin === false && !this.hasAdminPermissionOfProject)) {
        this.$router.replace('/dashboard')
      } else {
        this.$router.replace(currentPath)
      }
    })
  }
  _isAjaxProjectAcess (allProject, curProjectName, currentPath) {
    let curProjectUserAccess = this.getUserAccess({project: curProjectName})
    Promise.all([curProjectUserAccess]).then(() => {
      this._replaceRouter(currentPath)
    })
  }
  changeProject (val) {
    var currentPath = this.$router.currentRoute.path
    this._isAjaxProjectAcess(this.$store.state.project.allProject, val, currentPath)
  }
  addProject () {
    this.isEdit = false
    this.FormVisible = true
    this.project = {name: '', description: '', override_kylin_properties: {}}
  }
  Save () {
    this.$refs.projectForm.$emit('projectFormValid')
  }
  validSuccess (data) {
    this.projectSaveLoading = true
    var saveData = objectClone(data)
    this.saveProject(JSON.stringify(saveData)).then((result) => {
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.saveSuccess')
      })
      localStorage.setItem('selected_project', saveData.name)
      this.$store.state.project.selected_project = saveData.name
      this.FormVisible = false
      this.projectSaveLoading = false
      this.$router.push('/studio/source')
      this._replaceRouter(this.$router.currentRoute.path)
    }, (res) => {
      this.FormVisible = false
      this.projectSaveLoading = false
      handleError(res)
    })
  }
  onSubmit () {
  }
  handleselect (a, b) {
    if (a !== '/project') {
      this.defaultActive = a
    }
  }
  toggleMenu () {
    this.$store.state.config.layoutConfig.briefMenu = this.$store.state.config.layoutConfig.briefMenu ? '' : 'brief_menu'
    localStorage.setItem('menu_type', this.$store.state.config.layoutConfig.briefMenu)
  }
  logoutConfirm () {
    return kapConfirm(this.$t('confirmLoginOut'))
  }
  changePassword (userDetail) {
    this.callUserEditModal({ editType: 'password', userDetail })
  }
  handleCommand (command) {
    if (command === 'loginout') {
      this.logoutConfirm().then(() => {
        this.loginOut().then(() => {
          localStorage.setItem('buyit', false)
          // reset 所有的project信息
          this.resetProjectState()
          this.resetMonitorState()
          this.$router.push({name: 'Login'})
        })
      })
    } else if (command === 'setting') {
      // this.resetPasswordFormVisible = true
      this.changePassword(this.currentUser)
    }
  }
  goToProjectList () {
    this.defaultActive = 'projectActive'
    this.$router.push({name: 'Project'})
  }
  goHome () {
    $('#menu-list').find('li').eq(0).click()
  }
  closeResetPassword () {
    this.$refs['resetPassword'].$refs['resetPasswordForm'].resetFields()
  }
  checkResetPasswordForm () {
    this.$refs['resetPassword'].$emit('resetPasswordFormValid')
  }
  resetPasswordValidSuccess (data) {
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
  }
  resetProjectForm () {
    this.$refs['projectForm'].$refs['projectForm'].resetFields()
  }
  hasSomeProjectPermission () {
    return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask, permissions.OPERATION.mask)
  }
  hasAdminProjectPermission () {
    return hasPermission(this, permissions.ADMINISTRATION.mask)
  }

  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  get hasPermissionWithoutQuery () {
    return this.hasSomeProjectPermission()
  }
  get hasSomePermissionOfProject () {
    return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
  }
  get hasAdminPermissionOfProject () {
    return this.hasAdminProjectPermission()
  }
  get briefMenu () {
    return this.$store.state.config.layoutConfig.briefMenu
  }
  get gloalProjectSelectShow () {
    return this.$store.state.config.layoutConfig.gloalProjectSelectShow
  }
  get currentPath () {
    var currentPathName = ''
    menusData.forEach((menu) => {
      if (this.defaultActive && menu.path.toLowerCase() === this.defaultActive.toLowerCase()) {
        currentPathName = menu.name
      }
    })
    if (this.defaultActive.toLowerCase() === '/messages') {
      currentPathName = 'messages'
    }
    return currentPathName || 'project'
  }
  get currentRouterNameArr () {
    const hashStr = this.$route.path
    const hashArr = hashStr.split('/').slice(1)
    return hashArr
  }
  get currentUser () {
    this.currentUserInfo = this.$store.state.user.currentUser
    let info = { ...this.currentUserInfo }
    info.password = ''
    info.confirmPassword = ''
    return info
  }
  get kapInfo () {
    return this.$store.state.system.serverAboutKap
  }
  get kapDate () {
    return this.defaultVal(this.kapInfo && this.kapInfo['kap.dates'] || null)
  }
  get kapVersion () {
    return this.defaultVal(this.kapInfo && this.kapInfo['kap.version'] || null)
  }
  get isModeler () {
    return hasRole(this, 'ROLE_MODELER')
  }
  get serverAboutKap () {
    return this.$store.state.system.serverAboutKap
  }
  get lastTime () {
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

  mounted () {
    // 接受cloud的参数
    var from = getQueryString('from')
    var lang = getQueryString('lang')
    if (from === 'cloud') {
      if (lang) {
        this.$refs.changeLangCom.$emit('changeLang', lang)
      }
      $('#fullBox').addClass('cloud-frame-page')
    }
    // cloud
    // 刷新浏览器时的路由锁定
    menusData.forEach((menu) => {
      if (this.$router.history.current.name && menu.name.toLowerCase() === this.$router.history.current.name.toLowerCase()) {
        this.$store.state.config.routerConfig.currentPathName = menu.path
        this.defaultActive = menu.path
      }
    })
  }

  @Watch('currentPathNameGet')
  onCurrentPathNameGetChange (val) {
    this.defaultActive = val
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  .full-layout{
    .grid-content.bg-purple-light {
      overflow: auto;
      height: 100%;
      .main-content {
        height: calc(~"100% - 54px");
        box-sizing: border-box;
      }
    }
    .linsencebox {
      .el-dialog{
        position: absolute;
        bottom: 6px;
        right: 6px;
        margin:0;
      }
    }
    .brief_menu.panel .panel-center{
      .left-menu {
        width: 56px;
        text-align:center;
        li {
          text-align: center;
          span {
            display: none;
          }
        }
        .el-submenu.is-active{ 
          .el-submenu__title {
            background-color: @menu-active-bgcolor;
            i,span{              
              color: @base-color-1;
            }
          }
        }
        .logo{
          width: 30px;
          height: 35px;
          margin: 13px 13px 12px;
        }
      }
      .topbar > i {
        margin-left: 76px;
      }
      .panel-content{
        left: 56px;
      }
    }

    .panel {
      position: absolute;
      top: 0px;
      bottom: 0px;
      width: 100%;

      .panel-center {
        position: absolute;
        top: 0px;
        bottom: 0px;
        overflow: hidden;

        .panel-content {
          position: absolute;
          right: 0px;
          top: 60px;
          bottom: 0;
          left: 138px;
          overflow-y: auto;
          overflow-x: hidden;
        }
        .left-menu {
          width: 138px;
          height: 100%;
          position: relative;
          z-index: 999;
          background-color: @text-title-color;
          >ul {
            height: 100%;
            background-color: @text-title-color;
            .el-menu-item {
              color: @text-placeholder-color;
              background-color: @text-title-color;
              padding-left: 12px;
            }
            li.is-active {
              color: @base-color-1;
              background-color: @menu-active-bgcolor;
            }
            li:hover {
              background-color: @menu-active-bgcolor;
            }
          }
          .el-menu {
            .el-submenu__title {
              color: @text-placeholder-color;
              background-color: @text-title-color;
            }
            .el-submenu__title:hover {
              background-color: @menu-active-bgcolor;
            }
            .el-menu-item {
              min-width: initial;
            }
            .el-menu-item-group__title {
              padding: 0px;
            }
          }
          .el-menu--collapse {
            width: 56px;
            .el-submenu .el-menu {
              margin-left: 0px;
            }
          }
          .logo {
            height: 35px;
            vertical-align: middle;
            z-index:999;
            margin: 13px 0px 12px 20px;
            cursor: pointer;
          }
          .ky-line {
            background: @text-normal-color;
          }
        }
        .topbar{
          height: 60px;
          width: 100%;
          background: @fff;
          position: absolute;
          top:0;
          border-bottom: 1px solid @text-placeholder-color;
          box-shadow: 0 1px 2px 0 #bdbdbd;
          z-index: 100;
          >i{
            margin-left: 158px;
            margin-top: 22px;
            cursor: pointer;
            float: left;
          }
          >.el-button {
            margin: 14px 0 0 8px;
          }
          .top-ul {
            margin-top: 17px;
            >li{
              vertical-align: middle;
              display: inline-block;
              margin-right: 30px;
              cursor: pointer;
            }
          }
        }
      }
    }
    
  }
  .round-icon (@width, @height) {
    width:@width;
    height: @height;
    line-height: @height;
    text-align: center;
    background-color:@error-color-1;
    display:inline-block;
    border-radius: 50%;
    color:#fff;
    margin-left:10px;
  }
  .number-icon {
    .round-icon(16px, 16px);
    font-size:12px;
  }
  .dot-icon {
    .round-icon(6px, 6px);
    margin-top:-14px;
  }
  .el-menu--collapse {
    .dot-icon {
      margin-left:0;
    }
  }
  .el-menu--popup {
    margin-left: 1px;
    padding: 0px;
    background-color: @text-title-color!important;
    .el-menu-item-group__title {
      display: none;
    }
    .el-menu-item {
      color: @text-placeholder-color;
    }
    .el-menu-item.is-active {
      color: @base-color-1;
      background-color: @menu-active-bgcolor;
    }
    .el-menu-item:hover {
      background-color: @menu-active-bgcolor;
    }
  }
</style>

