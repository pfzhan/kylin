<template>
  <div class="full-layout" id="fullBox" :class="{fullLayout:isFullScreen}">
    <el-row class="panel" :class="{'brief_menu':briefMenuGet}">
      <el-col :span="24" class="panel-center">
        <aside class="left-menu">
          <img v-show="!briefMenuGet" src="../../assets/img/big_logo.png" class="logo" @click="goHome">
          <img v-show="briefMenuGet" src="../../assets/img/small_logo.png" class="logo" @click="goHome">
          <div class="ky-line"></div>
          <el-menu :default-active="defaultActive" id="menu-list" @select="handleselect" @open="clearMenu" unique-opened router :collapse="briefMenuGet">
            <template v-for="(item,index) in menus">
              <el-menu-item :index="item.path" v-if="!item.children && showMenuByRole(item.name)" :key="index">
                <i :class="item.icon" class="ksd-fs-16"></i>
                <span slot="title">{{$t('kylinLang.menu.' + item.name)}}</span>
              </el-menu-item>
              <el-submenu :index="item.path" v-if="item.children && showMenuByRole(item.name)" :id="item.name" :key="index">
                <template slot="title">
                  <i :class="item.icon" class="ksd-fs-16 menu-icon" ></i>
                  <span>{{$t('kylinLang.menu.' + item.name)}}</span><div v-if="item.name === 'studio' && reachThreshold" class="dot-icon"></div>
                </template>
                <el-menu-item-group>
                  <el-menu-item :index="child.path" v-for="child in item.children" :key="child.path" v-if="showMenuByRole(child.name)">
                    <span style="position:relative;">
                      {{$t('kylinLang.menu.' + child.name)}}
                      <span id="favo-menu-item" v-if="item.name === 'query' && child.name === 'favorite_query'"></span>
                    </span>
                    <div class="number-icon" v-if="child.name === 'model'  && reachThreshold">1</div>
                  </el-menu-item>
                </el-menu-item-group>
              </el-submenu>
            </template>
          </el-menu>
        </aside>
        <div class="topbar">
          <i class="ksd-fs-14" :class="[!briefMenuGet ? 'el-icon-ksd-grid_01' : 'el-icon-ksd-grid_02']" @click="toggleLeftMenu"></i>
          <project_select v-on:changePro="changeProject" ref="projectSelect"></project_select>
          <el-button v-show='gloalProjectSelectShow' :title="$t('kylinLang.project.projectList')" :class="{'project-page':defaultActive==='projectActive'}" @click="goToProjectList" size="medium">
            <i class="el-icon-ksd-project_list"></i>
          </el-button>
          <el-button :title="$t('kylinLang.project.addProject')" @click="addProject" v-show="isAdmin" size="medium">
            <i class="el-icon-plus"></i>
          </el-button>

          <ul class="top-ul ksd-fright">
            <li v-if="isAdmin"><canary></canary></li>
            <li><help></help></li>
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
          <div class="grid-content bg-purple-light" id="scrollContent">
            <el-col :span="24" v-show="gloalProjectSelectShow" class="bread-box">
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
    <!-- 全局apply favorite query -->
    <el-dialog width="440px" :title="$t('kylinLang.common.notice')" class="speed_dialog" :visible="reachThresholdVisible" @close="manualClose = false" :close-on-click-modal="false">
      <el-row>
        <el-col :span="14">
          {{$t('hello', {user: currentUser.username})}}<br/>
          <p style="text-indent:25px; line-height: 26px;" v-html="$t('speedTip', {queryCount: modelSpeedEvents ,modelCount: modelSpeedModelsCount})"></p>
        </el-col>
        <el-col :span="10" class="animateImg">
          <img class="notice_img notice_img1" :class="{'rotate1': rotateVisibel}" src="../../assets/img/noticeImg1.png" height="150" width="150">
          <img class="notice_img notice_img2" :class="{'rotate2': rotateVisibel}" src="../../assets/img/noticeImg2.png" height="150" width="150">
          <img class="notice_img" src="../../assets/img/noticeImg3.png" height="150" width="150">
        </el-col>
      </el-row>
      <div slot="footer" class="dialog-footer">
        <el-button size="medium" @click="ignoreSpeed" :loading="btnLoadingCancel">{{$t('ignore')}}</el-button>
        <el-button size="medium" type="primary" plain @click="applySpeed" :loading="applyBtnLoading">{{$t('apply')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
// import { handleSuccess, handleError, kapConfirm, hasRole } from '../../util/business'
import { handleError, kapConfirm, hasRole, hasPermission } from '../../util/business'
import { objectClone, getQueryString, cacheSessionStorage, cacheLocalStorage } from '../../util/index'
import { permissions, menusData, speedInfoTimer } from '../../config'
import { mapActions, mapMutations, mapGetters } from 'vuex'
import projectSelect from '../project/project_select'
import changeLang from '../common/change_lang'
import help from '../common/help'
import canary from '../security/canary'
import resetPassword from '../security/reset_password'
import $ from 'jquery'
// import Scrollbar from 'smooth-scrollbar'
@Component({
  methods: {
    ...mapActions('ProjectEditModal', {
      callProjectEditModal: 'CALL_MODAL'
    }),
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
      getUserAccess: 'USER_ACCESS',
      applySpeedInfo: 'APPLY_SPEED_INFO',
      ignoreSpeedInfo: 'IGNORE_SPEED_INFO',
      getSpeedInfo: 'GET_SPEED_INFO'
    }),
    ...mapMutations({
      setCurUser: 'SAVE_CURRENT_LOGIN_USER',
      resetProjectState: 'RESET_PROJECT_STATE',
      resetMonitorState: 'RESET_MONITOR_STATE',
      toggleMenu: 'TOGGLE_MENU'
    }),
    ...mapActions('UserEditModal', {
      callUserEditModal: 'CALL_MODAL'
    })
  },
  components: {
    'project_select': projectSelect,
    'change_lang': changeLang,
    'reset_password': resetPassword,
    help,
    canary
  },
  computed: {
    ...mapGetters([
      'currentPathNameGet',
      'isFullScreen',
      'currentSelectedProject',
      'briefMenuGet',
      'currentProjectData',
      'isAutoProject',
      'availableMenus'
    ]),
    modelSpeedEvents () {
      return this.$store.state.model.modelSpeedEvents
    },
    reachThreshold () {
      return this.$store.state.model.reachThreshold
    },
    reachThresholdVisible () {
      return this.$store.state.model.reachThreshold && this.manualClose
    },
    modelSpeedModelsCount () {
      return this.$store.state.model.modelSpeedModelsCount
    }
  },
  locales: {
    'en': {resetPassword: 'Reset Password', confirmLoginOut: 'Are you sure to exit?', validPeriod: 'Valid Period: ', overtip1: 'This License will be expired in ', overtip2: 'days. Please contact sales support to apply for the Enterprise License.', applayLisence: 'Apply for Enterprise License', 'continueUse': 'I Know', speedTip: 'System will accelerate <span class="ky-highlight-text">{queryCount}</span> queries: this will optimize <span class="ky-highlight-text">{modelCount}</span> models! Do you want to apply it?', ignore: 'Ignore', apply: 'Apply', hello: 'Hi {user},'},
    'zh-cn': {resetPassword: '重置密码', confirmLoginOut: '确认退出吗？', validPeriod: '使用期限: ', overtip1: '当前使用的许可证将在 ', overtip2: '天后过期。欢迎联系销售支持人员申请企业版许可证。', applayLisence: '申请企业版许可证', 'continueUse': '我知道了', speedTip: '系统即将加速 <span class="ky-highlight-text">{queryCount}</span> 条查询：需要优化的模型有 <span class="ky-highlight-text">{modelCount}</span> 个！同意此次加速吗？', ignore: '忽略建议', apply: '同意', hello: '{user} 你好，'}
  }
})
export default class LayoutLeftRightTop extends Vue {
  projectSaveLoading = false
  project = {}
  isEdit = false
  FormVisible = false
  manualClose = true
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
  applyBtnLoading = false
  btnLoadingCancel = false
  rotateVisibel = false

  @Watch('reachThreshold', {immediate: true})
  onReachThreshold (val) {
    if (val) {
      setTimeout(() => {
        this.rotateVisibel = true
      })
    }
  }

  @Watch('$route.name')
  onRouterChange (newVal, val) {
    if (newVal !== val) {
      this.manualClose = true
    }
  }

  loadSpeedInfo (loadingname) {
    if (!this.currentSelectedProject) {
      return
    }
    var loadingName = loadingname || 'applyBtnLoading'
    this.getSpeedInfo(this.currentSelectedProject).then(() => {
      this[loadingName] = false
    }, (res) => {
      this[loadingName] = false
      handleError(res)
    })
  }
  applySpeed (event) {
    this.applyBtnLoading = true
    this.applySpeedInfo({size: this.modelSpeedEvents, project: this.currentSelectedProject}).then(() => {
      this.flyEvent(event)
      this.loadSpeedInfo()
    }, (res) => {
      this.applyBtnLoading = false
      handleError(res)
    })
  }

  ignoreSpeed () {
    this.btnLoadingCancel = true
    this.ignoreSpeedInfo(this.currentSelectedProject).then(() => {
      this.btnLoadingCancel = false
      this.loadSpeedInfo('btnLoadingCancel')
    }, (res) => {
      this.btnLoadingCancel = false
      handleError(res)
    })
  }
  flyer = null
  flyEvent (event) {
    var targetArea = $('#monitor')
    var targetDom = targetArea.find('.menu-icon')
    var offset = targetDom.offset()
    if (this.flyer) {
      this.flyer.remove()
    }
    this.flyer = $('<span class="fly-box"></span>')
    let leftOffset = 64
    if (this.$lang === 'en') {
      leftOffset = 74
    }
    if (this.briefMenuGet) {
      leftOffset = 20
    }
    this.flyer.fly({
      start: {
        left: event.pageX,
        top: event.pageY
      },
      end: {
        left: offset.left + leftOffset,
        top: offset.top,
        width: 4,
        height: 4
      },
      onEnd: function () {
        targetDom.addClass('rotateY')
        setTimeout(() => {
          targetDom.fadeTo('slow', 0.5, function () {
            targetDom.removeClass('rotateY')
            targetDom.fadeTo('fast', 1)
          })
          this.flyer.fadeOut(1500, () => {
            this.flyer.remove()
          })
        }, 3000)
      }
    })
  }
  clearMenu () {
    this.flyer && this.flyer.remove()
  }
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
    return this.availableMenus.includes(menuName)
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
    this.$router.push('/refresh')
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
  async addProject () {
    try {
      const data = await this.callProjectEditModal({ editType: 'new' })
      if (data) {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        cacheSessionStorage('projectName', data.name)
        cacheLocalStorage('projectName', data.name)
        this.$store.state.project.selected_project = data.name
        this.FormVisible = false
        this.projectSaveLoading = false
        this.$router.push('/studio/source')
        this._replaceRouter(this.$router.currentRoute.path)
      }
    } catch (e) {
      handleError(e)
    }
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
      cacheSessionStorage('projectName', data.name)
      cacheLocalStorage('projectName', data.name)
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
  toggleLeftMenu () {
    this.toggleMenu(!this.briefMenuGet)
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
  ST = null
  loadCircleSpeedInfo () {
    if (this.currentSelectedProject) {
      // 如果apply或者ignore接口还在进行中，先暂停轮训的请求发送
      if (this.applyBtnLoading || this.btnLoadingCancel) {
        return new Promise((resolve) => {
          resolve()
        })
      } else {
        return this.getSpeedInfo(this.currentSelectedProject)
      }
    }
  }
  circleLoadSpeedInfo () {
    clearTimeout(this.ST)
    if (this.isAutoProject) {
      this.ST = setTimeout(() => {
        this.loadCircleSpeedInfo().then(() => {
          if (this._isDestroyed) {
            return
          }
          this.circleLoadSpeedInfo()
        })
      }, speedInfoTimer)
    }
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
    if (this.isAutoProject) {
      // 获取加速信息
      this.loadSpeedInfo()
      this.circleLoadSpeedInfo()
    }
  }
  destroyed () {
    clearTimeout(this.ST)
  }
  @Watch('currentPathNameGet')
  onCurrentPathNameGetChange (val) {
    this.defaultActive = val
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #favo-menu-item {
    position:absolute;
    width:4px;
    height:4px;
    top: 0px;
    right: -5px;
    border-radius: 50%;
    background-color:@error-color-1;
    opacity: 0;
  }
  .speed_dialog {
    .animateImg {
      position: relative;
      height: 150px;
      left: 12px;
      .notice_img {
        position: absolute;
        &.rotate1 {
          -webkit-transform:rotate(360deg);
          transform:rotate(360deg);
          -webkit-transition:-webkit-transform 1s ease-in-out;
          transition:transform 1s ease-in-out;
        }
        &.rotate2 {
          -webkit-transform:rotate(-360deg);
          transform:rotate(-360deg);
          -webkit-transition:-webkit-transform 1s ease-in-out;
          transition:transform 1s ease-in-out;
        }
      }
    }
  }
  .full-layout{
    .bread-box {
      padding:20px 0;
      background: #f9fafc;
      height: 54px;
    }
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
          height: 27px;
          margin: 18px 13px 14px 12px;
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
            background: @text-title-color;
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
            width:120px;
            height: 28px;
            vertical-align: middle;
            z-index:999;
            margin: 16px 0px 15px 8px;
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
    margin-left:3px;
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

