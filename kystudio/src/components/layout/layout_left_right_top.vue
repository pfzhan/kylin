<template>
  <div class="full-layout" id="fullBox" :class="{fullLayout:isFullScreen}">
    <el-row class="panel" :class="{'brief_menu':briefMenuGet}">
      <el-col :span="24" class="panel-center">
        <aside class="left-menu">
          <img v-show="!briefMenuGet" src="../../assets/img/logo/big_logo.png" class="logo" @click="goHome">
          <img v-show="briefMenuGet" src="../../assets/img/logo/small_logo.png" class="logo" @click="goHome">
          <div class="ky-line"></div>
          <el-menu :default-active="defaultActive" id="menu-list" @select="handleselect" @open="clearMenu" router :collapse="briefMenuGet">
            <template v-for="(item,index) in menus">
              <el-menu-item :index="item.path" v-if="!item.children && showMenuByRole(item.name)" :key="index">
                <i :class="item.icon" class="ksd-fs-16"></i>
                <span slot="title" v-if="item.name === 'modelList'">
                  {{isAutoProject ? $t('kylinLang.menu.index') : $t('kylinLang.menu.modelList')}}
                </span>
                <span slot="title" v-else>
                  {{$t('kylinLang.menu.' + item.name)}}
                </span>
              </el-menu-item>
              <el-submenu :index="item.path" v-if="item.children && showMenuByRole(item.name)" :id="item.name" :key="index">
                <template slot="title">
                  <i :class="item.icon" class="ksd-fs-16 menu-icon" ></i>
                  <span>{{$t('kylinLang.menu.' + item.name)}}</span><div v-if="item.name === 'studio' && reachThresholdVisible" class="dot-icon"></div>
                </template>
                <!-- <el-menu-item-group> -->
                  <el-menu-item :index="child.path" v-for="child in item.children" :key="child.path" v-if="showMenuByRole(child.name)">
                    <template v-if="child.name === 'job'">
                      <span style="position:relative;" id="monitorJobs">
                        {{$t('kylinLang.menu.' + child.name)}}
                      </span>
                    </template>
                    <template v-else-if="child.name === 'modelList'">
                      <span style="position:relative;" id="studioModel">
                        {{isAutoProject ? $t('kylinLang.menu.index') : $t('kylinLang.menu.modelList')}}
                      </span>
                      <div class="number-icon" v-if="reachThresholdVisible">1</div>
                    </template>
                    <template v-else>
                      <span style="position:relative;">
                        {{$t('kylinLang.menu.' + child.name)}}
                        <span id="favo-menu-item" v-if="item.name === 'query' && child.name === 'acceleration'"></span>
                      </span>
                    </template>
                  </el-menu-item>
                <!-- </el-menu-item-group> -->
              </el-submenu>
            </template>
          </el-menu>
          <div :class="['diagnostic-model', {'is-hold-menu': briefMenuGet}]" v-if="showMenuByRole('diagnostic')" @click="showDiagnosticDialog">
            <el-tooltip :content="$t('diagnosis')" effect="dark" placement="top">
              <span>
                <i class="el-icon-ksd-ostin_diagnose"/>
                <span class="text" v-if="!briefMenuGet">{{$t('diagnosis')}}</span>
              </span>
            </el-tooltip>
          </div>
        </aside>
        <div class="topbar">
          <div class="nav-icon">
            <common-tip :content="$t('holdNaviBar')" placement="bottom-start" v-if="!briefMenuGet">
              <i class="ksd-fs-14 el-icon-ksd-grid_01" @click="toggleLeftMenu"></i>
            </common-tip>
            <common-tip :content="$t('unholdNaviBar')" placement="bottom-start" v-else>
              <i class="ksd-fs-14 el-icon-ksd-grid_02" @click="toggleLeftMenu"></i>
            </common-tip>
          </div>
          <template v-if="!isAdminView">
            <project_select v-on:changePro="changeProject" ref="projectSelect"></project_select>
            <common-tip :content="$t('kylinLang.project.addProject')" placement="bottom-start">
              <el-button class="add-project-btn" v-guide.addProjectBtn :type="highlightType" plain @click="addProject" v-show="isAdmin" size="small">
                <i class="el-icon-ksd-add_2"></i>
              </el-button>
            </common-tip>
          </template>

          <ul class="top-ul ksd-fright">
            <li v-if="showMenuByRole('admin')">
              <el-button
                size="small"
                plain
                class="entry-admin"
                :class="isAdminView ? 'active' : null"
                @click="handleSwitchAdmin">
                <span>{{$t('kylinLang.menu.admin')}}</span>
              </el-button>
            </li>
            <li><help></help></li>
            <li><change_lang ref="changeLangCom"></change_lang></li>
            <li>
              <el-dropdown @command="handleCommand" class="user-msg-dropdown">
                <span class="el-dropdown-link">
                  <i class="el-icon-ksd-user ksd-mr-5 ksd-fs-16"></i><span class="ksd-fs-12 limit-user-name">{{currentUserInfo && currentUserInfo.username}}</span><i class="el-icon-caret-bottom"></i>
                </span>
                <el-dropdown-menu slot="dropdown">
                  <div class="user-name">{{ currentUserInfo && currentUserInfo.username }}</div>
                  <el-dropdown-item command="setting">{{$t('kylinLang.common.changePassword')}}</el-dropdown-item>
                  <el-dropdown-item command="loginout">{{$t('kylinLang.common.logout')}}</el-dropdown-item>
                </el-dropdown-menu>
              </el-dropdown>
            </li>
          </ul>
        </div>
        <div class="panel-content" id="scrollBox" >
          <div class="grid-content bg-purple-light" id="scrollContent">
            <!-- <el-col :span="24" v-show="gloalProjectSelectShow" class="bread-box"> -->
              <!-- 面包屑在dashboard页面不显示 -->
              <!-- <el-breadcrumb separator="/" class="ksd-ml-30">
                <el-breadcrumb-item>
                  <span>{{$t('kylinLang.menu.' + currentRouterNameArr[0])}}</span>
                </el-breadcrumb-item>
                <el-breadcrumb-item v-if="currentRouterNameArr[1]" :to="{ path: '/' + currentRouterNameArr[0] + '/' + currentRouterNameArr[1]}">
                  <span>{{$t('kylinLang.menu.' + currentRouterNameArr[1])}}</span>
                </el-breadcrumb-item>
                <el-breadcrumb-item v-if="currentRouterNameArr[2]" >
                  {{currentRouterNameArr[2]}}
                </el-breadcrumb-item>
              </el-breadcrumb> -->
            <!-- </el-col> -->
            <el-col :span="24" class="main-content">
              <transition :name="isAnimation ? 'slide' : null" v-bind:css="isAnimation">
                <router-view v-on:addProject="addProject"></router-view>
              </transition>
            </el-col>
          </div>
        </div>
        <div class="global-mask" v-if="isGlobalMaskShow">
          <div class="background"></div>
          <div class="notify-contect font-medium">{{notifyContect}}</div>
        </div>
      </el-col>
    </el-row>
    <el-dialog class="linsencebox"
      :title="kapVersion"
      width="480px"
      limited-area
      :visible.sync="lisenceDialogVisible"
      :close-on-press-escape="false"
      :modal="false">
      <p><span>{{$t('validPeriod')}}</span>{{kapDate}}<!-- <span>2012<i>/1/2</i></span><span>－</span><span>2012<i>/1/2</i></span> --></p>
      <p class="ksd-pt-10">{{$t('overtip1')}}<span class="hastime">{{lastTime}} </span>{{$t('overtip2')}}</p>
      <span slot="footer" class="dialog-footer">
        <el-button plain @click="getLicense">{{$t('applayLisence')}}</el-button>
        <el-button @click="lisenceDialogVisible = false">{{$t('continueUse')}}</el-button>
      </span>
    </el-dialog>
    <!-- 全局apply favorite query -->
    <el-dialog width="480px" :title="$t('accelerateTips')" class="speed_dialog" limited-area :visible="reachThresholdVisible" @close="manualClose = false" :close-on-click-modal="false">
      <el-row>
        <el-col :span="14">
          {{$t('hello', {user: currentUser.username})}}<br/>
          <p style="text-indent:25px; line-height: 26px;" v-html="$t('speedTip', {queryCount: modelSpeedEvents ,modelCount: modelSpeedModelsCount, speedModel: speedModel})"></p>
        </el-col>
        <el-col :span="10" class="animateImg">
          <img class="notice_img notice_img1" :class="{'rotate1': rotateVisibel}" src="../../assets/img/noticeImg1.png" height="150" width="150">
          <img class="notice_img notice_img2" :class="{'rotate2': rotateVisibel}" src="../../assets/img/noticeImg2.png" height="150" width="150">
          <img class="notice_img" src="../../assets/img/noticeImg3.png" height="150" width="150">
        </el-col>
      </el-row>
      <div slot="footer" class="dialog-footer">
        <el-button plain size="medium" @click="ignoreSpeed" :loading="btnLoadingCancel">{{$t('ignore')}}</el-button>
        <el-button size="medium" @click="applySpeed" :loading="applyBtnLoading">{{$t('apply')}}</el-button>
      </div>
    </el-dialog>

    <!-- 全局弹窗 带detail -->
    <kap-detail-dialog-modal></kap-detail-dialog-modal>
    <diagnostic v-if="showDiagnostic" @close="showDiagnostic = false"/>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
// import { handleSuccess, handleError, kapConfirm, hasRole } from '../../util/business'
import { handleError, kapConfirm, hasRole, hasPermission } from '../../util/business'
import { getQueryString, cacheSessionStorage, cacheLocalStorage, delayMs } from '../../util/index'
import { permissions, menusData, speedInfoTimer } from '../../config'
import { mapState, mapActions, mapMutations, mapGetters } from 'vuex'
import projectSelect from '../project/project_select'
import changeLang from '../common/change_lang'
import help from '../common/help'
import KapDetailDialogModal from '../common/GlobalDialog/dialog/detail_dialog'
import Diagnostic from '../admin/Diagnostic/index'
import $ from 'jquery'
import ElementUI from 'kyligence-ui'
let MessageBox = ElementUI.MessageBox
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
      resetPassword: 'RESET_PASSWORD',
      getUserAccess: 'USER_ACCESS',
      getAboutKap: 'GET_ABOUTKAP',
      applySpeedInfo: 'APPLY_SPEED_INFO',
      ignoreSpeedInfo: 'IGNORE_SPEED_INFO',
      getSpeedInfo: 'GET_SPEED_INFO'
    }),
    ...mapMutations({
      setCurUser: 'SAVE_CURRENT_LOGIN_USER',
      resetProjectState: 'RESET_PROJECT_STATE',
      toggleMenu: 'TOGGLE_MENU',
      cacheHistory: 'CACHE_HISTORY',
      saveTabs: 'SET_QUERY_TABS',
      resetSpeedInfo: 'CACHE_SPEED_INFO',
      setProject: 'SET_PROJECT'
    }),
    ...mapActions('UserEditModal', {
      callUserEditModal: 'CALL_MODAL'
    })
  },
  components: {
    'project_select': projectSelect,
    'change_lang': changeLang,
    help,
    KapDetailDialogModal,
    Diagnostic
  },
  computed: {
    ...mapState({
      cachedHistory: state => state.config.cachedHistory,
      isSemiAutomatic: state => state.project.isSemiAutomatic,
      licenseDates: state => state.system.serverAboutKap
    }),
    ...mapGetters([
      'currentPathNameGet',
      'isFullScreen',
      'currentSelectedProject',
      'briefMenuGet',
      'currentProjectData',
      'availableMenus',
      'isAutoProject',
      'isGuideMode'
    ]),
    modelSpeedEvents () {
      return this.$store.state.model.modelSpeedEvents
    },
    reachThreshold () {
      return this.$store.state.model.reachThreshold
    },
    reachThresholdVisible () {
      return this.$store.state.model.reachThreshold && this.$store.state.project.projectAutoApplyConfig && this.manualClose
    },
    modelSpeedModelsCount () {
      return this.$store.state.model.modelSpeedModelsCount
    },
    speedModel () {
      if (this.isAutoProject) {
        return this.$t('indexs')
      } else {
        return this.$t('models')
      }
    }
  },
  locales: {
    'en': {
      resetPassword: 'Reset Password',
      confirmLoginOut: 'Are you sure to exit?',
      validPeriod: 'Valid Period: ',
      overtip1: 'This License will be expired in ',
      overtip2: 'days. Please contact sales support to apply for the Enterprise License.',
      applayLisence: 'Apply for Enterprise License',
      'continueUse': 'I Know',
      speedTip: 'System will accelerate <span class="ky-highlight-text">{queryCount}</span> queries: this will optimize <span class="ky-highlight-text">{modelCount}</span> {speedModel}! Do you want to apply it?',
      ignore: 'Ignore',
      apply: 'Apply',
      hello: 'Hi {user},',
      leaveAdmin: 'Going to leave the administration mode.',
      enterAdmin: 'Hi, welcome to the administration mode.',
      accelerateTips: 'Accelerating Tips',
      noProject: 'No project now. Please create one project via the top bar.',
      indexs: 'index group(s)',
      models: 'model(s)',
      holdNaviBar: 'Fold Navigation Bar',
      unholdNaviBar: 'Unfold Navigation Bar',
      diagnosis: 'Diagnosis'
    },
    'zh-cn': {
      resetPassword: '重置密码',
      confirmLoginOut: '确认退出吗？',
      validPeriod: '使用期限: ',
      overtip1: '当前使用的许可证将在 ',
      overtip2: '天后过期。欢迎联系销售支持人员申请企业版许可证。',
      applayLisence: '申请企业版许可证',
      'continueUse': '我知道了',
      speedTip: '系统即将加速 <span class="ky-highlight-text">{queryCount}</span> 条查询：需要优化的{speedModel}有 <span class="ky-highlight-text">{modelCount}</span> 个！同意此次加速吗？',
      ignore: '忽略建议',
      apply: '同意',
      hello: '{user} 你好，',
      leaveAdmin: '您即将离开系统管理。',
      enterAdmin: '您好，欢迎进入系统管理。',
      accelerateTips: '加速建议 ',
      noProject: '无项目。请在顶栏新建一个项目。',
      indexs: '索引组',
      models: '模型',
      holdNaviBar: '收起导航栏',
      unholdNaviBar: '展开导航栏',
      diagnosis: '诊断'
    }
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
  isAnimation = false
  isGlobalMaskShow = false
  showDiagnostic = false

  get isAdminView () {
    const adminRegex = /^\/admin/
    return adminRegex.test(this.$route.fullPath)
  }

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
    // 新手引导模式不用反复弹提示
    if (!this.isGuideMode) {
      this.noProjectTips()
    }
  }
  setGlobalMask (notifyContect) {
    this.isGlobalMaskShow = true
    this.notifyContect = this.$t(notifyContect, this.currentUserInfo)
  }
  hideGlobalMask () {
    this.isGlobalMaskShow = false
    this.notifyContect = ''
  }
  async handleSwitchAdmin () {
    this.setGlobalMask(this.isAdminView ? 'leaveAdmin' : 'enterAdmin')
    await delayMs(1700)
    if (this.isAdminView) {
      const nextLocation = this.cachedHistory ? this.cachedHistory : '/'
      this.isAnimation = true
      this.$router.push(nextLocation)
      setTimeout(() => {
        this.isAnimation = false
      })
    } else {
      this.cacheHistory(this.$route.fullPath)
      this.isAnimation = true
      this.$router.push('/admin/project')
      setTimeout(() => {
        this.isAnimation = false
      })
    }
    this.hideGlobalMask()
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
    this.ignoreSpeedInfo({ignoreSize: this.modelSpeedEvents, project: this.currentSelectedProject}).then(() => {
      this.btnLoadingCancel = false
      this.resetSpeedInfo({reachThreshold: false, queryCount: 0, modelCount: 0})
      this.loadSpeedInfo('btnLoadingCancel')
    }, (res) => {
      this.btnLoadingCancel = false
      handleError(res)
    })
  }
  flyer = null
  flyEvent (event) {
    let navOpen = (element) => {
      let targetDom = ''
      let rotateIcon = false
      const dom = {
        monitor: {parent: $('#monitor').find('.menu-icon'), children: $('#monitorJobs')},
        studio: {parent: $('#studio').find('.menu-icon'), children: $('#studioModel')}
      }
      document.querySelector(element).classList.value.indexOf('is-opened') >= 0 ? (targetDom = dom[element.replace(/^[.|#]/, '')].children) : (targetDom = dom[element.replace(/^[.|#]/, '')].parent, rotateIcon = true)
      return { targetDom, offset: targetDom.offset(), rotateIcon, flyer: true }
    }
    const currentPattern = !this.isAutoProject && this.isSemiAutomatic ? navOpen('#studio') : navOpen('#monitor')
    const flyer = $('<span class="fly-box"></span>')
    let leftOffset = 80
    if (this.$lang === 'en') {
      leftOffset = 74
    }
    if (this.briefMenuGet) {
      leftOffset = 20
    }
    flyer.fly({
      start: {
        left: event.pageX,
        top: event.pageY
      },
      end: {
        left: currentPattern.offset.left + leftOffset,
        top: currentPattern.offset.top,
        width: 4,
        height: 4
      },
      onEnd: function () {
        if (currentPattern.rotateIcon && currentPattern.flyer) {
          currentPattern.targetDom.addClass('rotateY')
          setTimeout(() => {
            currentPattern.targetDom.fadeTo('slow', 0.5, function () {
              currentPattern.targetDom.removeClass('rotateY')
              currentPattern.targetDom.fadeTo('fast', 1)
            })
            flyer.fadeOut(1500, () => {
              flyer.remove()
            })
          }, 3000)
        } else {
          setTimeout(() => {
            flyer.fadeOut(1500, () => {
              flyer.remove()
            })
          }, 3000)
        }
      }
    })
  }
  clearMenu () {
    this.flyer && this.flyer.remove()
  }
  created () {
    // this.reloadRouter()
    this.defaultActive = this.$route.path || '/dashboard'
    this.getAboutKap(() => {}, (res) => {
      handleError(res)
    })
  }
  showMenuByRole (menuName) {
    return this.availableMenus.includes(menuName.toLowerCase())
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
  _replaceRouter (currentPathName, currentPath) {
    this.$router.replace('/refresh')
    // 切换项目时重置speedInfo，避免前后两个项目混淆
    this.resetSpeedInfo({reachThreshold: false, queryCount: 0, modelCount: 0})
    this.$nextTick(() => {
      if (currentPathName === 'Job' && !this.hasPermissionWithoutQuery && !this.isAdmin) {
        this.$router.replace({name: 'Dashboard', params: { refresh: true }})
      } else {
        this.$router.replace({name: currentPathName, params: { refresh: true }})
        currentPath && (this.defaultActive = currentPath)
      }
    })
  }
  _isAjaxProjectAcess (allProject, curProjectName, currentPathName, currentPath) {
    let curProjectUserAccess = this.getUserAccess({project: curProjectName})
    Promise.all([curProjectUserAccess]).then(() => {
      this._replaceRouter(currentPathName, currentPath)
    })
  }
  changeProject (val) {
    this.setProject(val)
    var currentPathName = this.$router.currentRoute.name
    var currentPath = this.$router.currentRoute.path
    this._isAjaxProjectAcess(this.$store.state.project.allProject, val, currentPathName, currentPath)
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
        this._replaceRouter('Source', '/studio/source')
      }
    } catch (e) {
      handleError(e)
    }
  }
  Save () {
    this.$refs.projectForm.$emit('projectFormValid')
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
  loginOutFunc () {
    if (this.$route.name === 'ModelEdit') {
      MessageBox.confirm(window.kapVm.$t('kylinLang.common.willGo'), window.kapVm.$t('kylinLang.common.notice'), {
        confirmButtonText: window.kapVm.$t('kylinLang.common.exit'),
        cancelButtonText: window.kapVm.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(() => {
        this.logoutConfirm().then(() => {
          this.loginOut().then(() => {
            localStorage.setItem('buyit', false)
            // reset 所有的project信息
            this.resetProjectState()
            this.resetQueryTabs()
            this.$router.push({name: 'Login', params: { ignoreIntercept: true }})
          })
        })
      })
    } else {
      this.logoutConfirm().then(() => {
        this.loginOut().then(() => {
          localStorage.setItem('buyit', false)
          // reset 所有的project信息
          this.resetProjectState()
          this.resetQueryTabs()
          this.$router.push({name: 'Login'})
        })
      })
    }
  }
  resetQueryTabs () {
    this.saveTabs({tabs: null})
  }
  handleCommand (command) {
    if (command === 'loginout') {
      this.loginOutFunc()
    } else if (command === 'setting') {
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
    return this.defaultVal(this.kapInfo && this.kapInfo['ke.dates'] || null)
  }
  get kapVersion () {
    return this.defaultVal(this.kapInfo && this.kapInfo['ke.version'] || null)
  }
  get isModeler () {
    return hasRole(this, 'ROLE_MODELER')
  }
  get serverAboutKap () {
    return this.$store.state.system.serverAboutKap
  }
  get lastTime () {
    var date = this.serverAboutKap && this.serverAboutKap['ke.dates'] || ''
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
      // 如果apply或者ignore接口或者立即加速的接口还在进行中，先暂停轮训的请求发送
      // 加速配置关闭时暂停轮询
      if (!this.isAutoProject || this.applyBtnLoading || this.btnLoadingCancel || this.$store.state.model.circleSpeedInfoLock || !this.$store.state.project.projectAutoApplyConfig) {
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
    this.ST = setTimeout(() => {
      if (this._isDestroyed) {
        return
      }
      this.loadCircleSpeedInfo().then(() => {
        if (this._isDestroyed) {
          return
        }
        this.circleLoadSpeedInfo()
      })
    }, speedInfoTimer)
  }
  get highlightType () {
    return !this.$store.state.project.selected_project && !this.$store.state.project.allProject.length ? 'primary' : ''
  }
  noProjectTips () {
    if (this.highlightType && this.licenseDates['ke.dates']) {
      MessageBox.alert(this.$t('noProject'), this.$t('kylinLang.common.notice'), {
        confirmButtonText: this.$t('kylinLang.common.ok'),
        type: 'info'
      })
    }
  }
  mounted () {
    // 接受cloud的参数
    var from = getQueryString('from')
    var uimode = getQueryString('uimode') // 界面展示模式
    if (from === 'cloud' || uimode === 'nomenu') { // 保留cloud，兼容云老参数
      $('#fullBox').addClass('cloud-frame-page')
    }
    if (this.isAutoProject) {
      // 获取加速信息
      this.loadSpeedInfo()
      this.circleLoadSpeedInfo()
    }
    // 新手引导模式不用反复弹提示
    if (!this.isGuideMode) {
      this.noProjectTips()
    }
  }
  destroyed () {
    clearTimeout(this.ST)
  }
  @Watch('currentPathNameGet')
  onCurrentPathNameGetChange (val) {
    this.defaultActive = val
  }
  showDiagnosticDialog () {
    this.showDiagnostic = true
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
        height: 100%;
        box-sizing: border-box;
        background: white;
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
          margin: 14px 12px 10px 12px;
        }
      }
      .topbar .nav-icon {
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
          top: 52px;
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
          background-color: #102d41;
          // >ul {
          //   height: 100%;
          //   background-color: @text-title-color;
          //   .el-menu-item {
          //     color: @text-placeholder-color;
          //     background-color: @text-title-color;
          //     padding-left: 12px;
          //   }
          //   li.is-active {
          //     color: @base-color-1;
          //     background-color: @menu-active-bgcolor;
          //   }
          //   li:hover {
          //     background-color: @menu-active-bgcolor;
          //   }
          // }
          // .el-menu {
          //   // background: @text-title-color;
          //   .el-submenu__title {
          //     color: @text-placeholder-color;
          //     background-color: @text-title-color;
          //   }
          //   .el-submenu__title:hover {
          //     background-color: @menu-active-bgcolor;
          //   }
          //   .el-menu-item {
          //     min-width: initial;
          //   }
          //   .el-menu-item-group__title {
          //     padding: 0px;
          //   }
          // }
          // .el-menu--collapse {
          //   width: 56px;
          //   .el-submenu .el-menu {
          //     margin-left: 0px;
          //   }
          // }
          .logo {
            width:120px;
            height: 28px;
            vertical-align: middle;
            z-index:999;
            margin: 12px 0px 11px 9px;
            cursor: pointer;
          }
          .ky-line {
            background: @text-normal-color;
          }
        }
        .topbar{
          height: 51px;
          width: 100%;
          background: @fff;
          position: absolute;
          top:0;
          border-bottom: 1px solid @line-split-color;
          box-shadow: 0 1px 2px 0 @line-split-color;
          z-index: 100;
          .nav-icon {
            margin-left: 158px;
            margin-top: 20px;
            height: 14px;
            line-height: 14px;
            cursor: pointer;
            float: left;
          }
          .add-project-btn {
            margin: 14px 0 0 10px;
            padding:5px;
          }
          .top-ul {
            font-size:0;
            margin-top: 14px;
            >li{
              vertical-align: middle;
              display: inline-block;
              margin-right: 20px;
              cursor: pointer;
            }
            .el-dropdown-link {
              font-size:12px;
            }
            .el-dropdown-link:hover {
              color: @base-color;
              * {
                color: @base-color;
              }
            }
            .user-msg-dropdown {
              height: 24px;
              .el-dropdown-link {
                height: 100%;
                display: inline-block;
                line-height: 24px;
                .el-icon-ksd-user {
                  float: left;
                  line-height: 24px;
                }
                .el-icon-caret-bottom {
                  float: right;
                  line-height: 24px;
                }
                .limit-user-name {
                  line-height: 24px;
                }
              }
            }
          }
        }
      }
    }
    .entry-admin.active {
      box-shadow: inset 1px 1px 2px 0 #ADC2D0;
      border-color: @base-color-11;
      color: @base-color-11;
    }
    .global-mask {
      position: absolute;
      top: 0;
      right: 0;
      bottom: 0;
      left: 0;
      z-index: 999999;
      .background {
        width: 100%;
        height: 100%;
        opacity: 0.7;
        background-color: @000;
      }
      .notify-contect {
        position: absolute;
        top: 30%;
        left: 50%;
        transform: translateX(-50%);
        height: 50px;
        min-width: 300px;
        white-space: nowrap;
        padding: 0 40px;
        line-height: 50px;
        background: @base-color;
        color: @fff;
        text-align: center;
        font-size: 16px;
      }
    }
  }
  .limit-user-name {
    height: 20px;
    display: inline-block;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 120px;
    line-height: 29px;
  }
  .user-name {
    padding: 0 8px 8px 8px;
    line-height: 1;
    border-bottom: 1px solid @line-split-color;
    box-sizing: border-box;
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
    > [role='menuitem']:not(:last-child) {
      margin-bottom: 1px;
    }
  }
  .el-menu--popup {
    margin-left: 1px;
    padding: 0px;
    background-color: @text-title-color!important;
    &.el-menu {
      min-width: 100px;
    }
    .el-menu-item-group__title {
      display: none;
    }
    .el-menu-item {
      color: @text-placeholder-color;
      height: 32px;
      line-height: 32px;
      // &:not(:last-child) {
      //   margin-bottom: 1px;
      // }
    }
    .el-menu-item.is-active {
      color: @base-color-1;
      background-color: @menu-active-bgcolor;
    }
    .el-menu-item:hover {
      background-color: @menu-active-bgcolor;
    }
  }
  .diagnostic-model {
    width: calc(~'100% - 44px');
    height: 24px;
    border-radius: 12px;
    border: solid 1px @line-border-color;
    margin-left: 22px;
    box-sizing: border-box;
    position: absolute;
    bottom: 40px;
    color: @line-border-color;
    font-size: 12px;
    text-align: center;
    line-height: 22px;
    cursor: pointer;
    &.is-hold-menu {
      width: calc(~'100% - 31px');
      margin-left: 15px;
    }
    &:hover {
      color: @base-color;
      border: solid 1px @base-color;
    }
  }
</style>
