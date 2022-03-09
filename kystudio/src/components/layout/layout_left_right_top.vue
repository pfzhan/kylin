<template>
  <div class="full-layout" id="fullBox" :class="{fullLayout:isFullScreen,isModelList:($route.name === 'ModelList'), 'cloud-frame-page': this.$store.state.config.platform === 'iframe'}">
    <el-row class="panel" :class="{'brief_menu':briefMenuGet}">
      <el-col :span="24" class="panel-center">
        <aside class="left-menu">
          <img v-show="!briefMenuGet" src="../../assets/img/logo_KE.png" class="logo" @click="goHome">
          <img v-show="briefMenuGet" src="../../assets/img/logo/logo_small_white.png" class="logo" @click="goHome">
          <!-- <div class="ky-line"></div> -->
          <el-menu :default-active="defaultActive" id="menu-list" @select="handleselect" @open="clearMenu" router :collapse="briefMenuGet">
            <template v-for="(item,index) in menus">
              <el-menu-item :index="item.path" v-if="!item.children && showMenuByRole(item.name)" :key="index">
                <!-- <i :class="item.icon" class="ksd-fs-16"></i> -->
                <el-icon :name="item.icon" type="mult"></el-icon><span
                slot="title" v-if="item.name === 'modelList'">{{isAutoProject ? $t('kylinLang.menu.index') : $t('kylinLang.menu.modelList')}}</span><span
                slot="title" v-else>{{$t('kylinLang.menu.' + item.name)}}</span>
              </el-menu-item>
              <el-submenu :index="item.path" v-if="item.children && showMenuByRole(item.name)" :id="item.name" :key="index">
                <template slot="title">
                  <!-- <i :class="item.icon" class="ksd-fs-16 menu-icon" ></i> -->
                  <el-icon :name="item.icon" type="mult"></el-icon><span>{{$t('kylinLang.menu.' + item.name)}}</span><div v-if="item.name === 'studio' && reachThresholdVisible" class="dot-icon"></div>
                </template>
                <!-- <el-menu-item-group> -->
                  <el-menu-item :index="child.path" :class="{'ksd-pl-45': !briefMenuGet}" v-for="child in item.children" :key="child.path" v-if="showMenuByRole(child.name)">
                    <template v-if="child.name === 'job'">
                      <span style="position:relative;" id="monitorJobs">{{$t('kylinLang.menu.' + child.name)}}</span>
                    </template>
                    <template v-else-if="child.name === 'modelList'">
                      <span style="position:relative;" id="studioModel">{{isAutoProject ? $t('kylinLang.menu.index') : $t('kylinLang.menu.modelList')}}</span>
                      <div class="number-icon" v-if="reachThresholdVisible">1</div>
                    </template>
                    <template v-else>
                      <span style="position:relative;"> {{$t('kylinLang.menu.' + child.name)}}
                        <span id="favo-menu-item" v-if="item.name === 'query' && child.name === 'acceleration'"></span>
                      </span>
                    </template>
                  </el-menu-item>
                <!-- </el-menu-item-group> -->
              </el-submenu>
            </template>
          </el-menu>
          <div :class="['diagnostic-model', {'is-hold-menu': briefMenuGet}]" v-if="showMenuByRole('diagnostic')" @click="showDiagnosticDialog">
            <el-tooltip :content="$t('diagnosis')" effect="dark" placement="right" popper-class="diagnosis-tip" :disabled="!briefMenuGet">
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
              <el-button type="primary" text icon-button-mini icon="el-ksd-icon-nav_fold_22" @click="toggleLeftMenu"></el-button>
            </common-tip>
            <common-tip :content="$t('unholdNaviBar')" placement="bottom-start" v-else>
              <el-button type="primary" text icon-button-mini icon="el-ksd-icon-nav_unfold_22" @click="toggleLeftMenu"></el-button>
            </common-tip>
          </div>
          <template v-if="!isAdminView">
            <project_select v-on:changePro="changeProject" ref="projectSelect"></project_select>
            <common-tip :content="canAddProject ? $t('kylinLang.project.addProject') : $t('disableAddProject')" placement="bottom-start">
              <el-button class="add-project-btn" v-guide.addProjectBtn type="primary" text icon-button-mini icon="el-ksd-icon-add_22" @click="addProject" v-show="isAdmin" :disabled="!canAddProject">
              </el-button>
            </common-tip>
          </template>

          <ul class="top-ul ksd-fright">
            <li v-if="currentSelectedProject">
              <div class="quota-top-bar">
                <el-popover ref="quotaPopover" width="290" popper-class="quota-popover" v-model="showQuota">
                  <div class="quota-popover-layout">
                    <div class="contain ksd-fs-12">
                      <p>
                        <span>{{$t('useageMana')}}</span>
                        <span  v-if="quotaInfo.storage_quota_size !== -1" :class="['quota-status', getQuotaColor]">
                          {{useageRatio*100 | fixed(2)}}% ({{quotaInfo.total_storage_size | dataSize}}/{{quotaInfo.storage_quota_size | dataSize}})
                        </span>
                        <span v-else>-</span>
                      </p>
                      <common-tip placement="right" :content="$t('settingTips')" v-show="dashboardActions.includes('viewSetting')">
                        <span class="setting-icon-place">
                          <i class="el-icon-ksd-setting" @click="gotoSetting"></i>
                        </span>
                      </common-tip>
                    </div>
                    <p class="ksd-fs-12 ksd-mt-5">
                      <span>{{$t('trash')}}<common-tip :content="$t('tarshTips')" placement="top"><i class="el-icon-ksd-info ksd-fs-12 ksd-ml-5 trash-tips-icon"></i></common-tip>{{$t('kylinLang.common.colon')}}</span><span v-if="quotaInfo.garbage_storage_size !== -1"><span>
                        {{quotaInfo.garbage_storage_size | dataSize}}
                        </span><common-tip placement="right" :content="$t('clear')" v-if="!$store.state.project.isSemiAutomatic&&dashboardActions.includes('clearStorage')"><!-- 半自动挡时隐藏清理按钮 -->
                        <i class="el-icon-ksd-clear ksd-ml-10 clear-btn"
                        :class="{'is_no_quota': useageRatio >= 0.9, 'is-disabled': !quotaInfo.garbage_storage_size || quotaInfo.garbage_storage_size === -1}"
                      @click="clearStorage"></i>
                      </common-tip></span>
                      <span v-else>-</span>
                    </p>
                  </div>
                </el-popover>
                <p class="quota-info" v-popover:quotaPopover @click="showQuota = !showQuota">
                  <span :class="['flag', getQuotaColor]"></span>
                  <span class="quota-title">{{$t('storageQuota')}}</span>
                </p>
              </div>
            </li>
            <li class="capacity-li">
              <capacity/>
            </li>
            <li v-if="showMenuByRole('admin')" style="margin-right: 1px;">
              <el-tooltip :content="$t('kylinLang.menu.admin')" placement="bottom">
                <el-button
                  type="primary"
                  text
                  class="entry-admin"
                  icon-button
                  icon="el-ksd-icon-system_config_22"
                  :class="isAdminView ? 'active' : null"
                  @click="handleSwitchAdmin">
                </el-button>
              </el-tooltip>
            </li>
            <li style="margin-right: 1px;"><help></help></li>
            <li class="ksd-mr-10"><change_lang ref="changeLangCom"></change_lang></li>
            <li>
              <el-dropdown @command="handleCommand" class="user-msg-dropdown">
                <!-- <span class="el-dropdown-link">
                  <i class="el-icon-ksd-user ksd-mr-5 ksd-fs-16"></i>
                  <span class="ksd-fs-12 limit-user-name">{{currentUserInfo && currentUserInfo.username}}</span><i class="el-icon-caret-bottom"></i>
                </span> -->
                <el-button type="primary" text iconr="el-ksd-icon-arrow_down_22"><span class="limit-user-name">{{currentUserInfo && currentUserInfo.username}}</span></el-button>
                <el-dropdown-menu slot="dropdown">
                  <div class="user-name">{{ currentUserInfo && currentUserInfo.username }}</div>
                  <el-dropdown-item :disabled="!isTestingSecurityProfile" command="setting">{{$t('kylinLang.common.changePassword')}}</el-dropdown-item>
                  <el-dropdown-item command="loginout">{{$t('kylinLang.common.logout')}}</el-dropdown-item>
                </el-dropdown-menu>
              </el-dropdown>
            </li>
          </ul>
        </div>
        <div class="panel-content" id="scrollBox" :class="{'ksd-pt-38': isShowAlter}">
          <div class="alter-block" v-if="isShowAlter">
            <el-alert :type="globalAlterTips.flag === 0 ? 'error' : 'warning'" :closable="globalAlterTips.flag !== 0" show-icon>
              <span slot="title">{{globalAlterTips.text}} <a href="javascript:void(0);" @click="jumpToDetails" v-if="globalAlterTips.detailPath&&$route.name!=='SystemCapacity'">{{$t('viewDetails')}}</a></span>
            </el-alert>
          </div>
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
                <router-view v-on:addProject="addProject" v-if="isShowRouterView"></router-view>
                <div v-else class="blank-content">
                  <i class="el-icon-ksd-alert_1"></i>
                  <p class="text">
                    {{$t('noAuthorityText')}}
                  </p>
                </div>
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
        <el-button @click="concatSales">{{$t('applayLisence')}}</el-button>
        <el-button type="primary" @click="lisenceDialogVisible = false">{{$t('continueUse')}}</el-button>
      </span>
    </el-dialog>
    <!-- 这个是服务即将过期的弹窗 -->
     <el-dialog class="linsencebox"
      :title="kapVersion"
      width="480px"
      limited-area
      :visible.sync="licenseServiceDialogVisible"
      :close-on-press-escape="false"
      :modal="false">
      <p><span>{{$t('serviceEndDate')}}</span>{{KESeriveDate}}<!-- <span>2012<i>/1/2</i></span><span>－</span><span>2012<i>/1/2</i></span> --></p>
      <p class="ksd-pt-10">{{$t('serviceOvertip1')}}<span class="hastime">{{lastServiceEndTime}} </span>{{$t('serviceOvertip2')}}</p>
      <span slot="footer" class="dialog-footer">
        <el-button @click="concatSales">{{$t('contactSales')}}</el-button>
        <el-button type="primary" @click="licenseServiceDialogVisible = false">{{$t('gotit')}}</el-button>
      </span>
    </el-dialog>
    <!-- 全局apply favorite query -->
    <el-dialog width="480px" :title="$t('accelerateTips')" class="speed_dialog" :visible="reachThresholdVisible" @close="manualClose = false" :close-on-click-modal="false">
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
        <el-button size="medium" @click="ignoreSpeed" :loading="btnLoadingCancel">{{$t('ignore')}}</el-button>
        <el-button type="primary" size="medium" @click="applySpeed" :loading="applyBtnLoading">{{$t('apply')}}</el-button>
      </div>
    </el-dialog>

    <!-- 全局弹窗 带detail -->
    <kap-detail-dialog-modal></kap-detail-dialog-modal>
    <diagnostic v-if="showDiagnostic" @close="showDiagnostic = false"/>
    <!-- 模型引导 -->
    <GuideModal />
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
// import { handleSuccess, handleError, kapConfirm, hasRole } from '../../util/business'
import { handleError, kapConfirm, hasRole, hasPermission } from '../../util/business'
import { cacheSessionStorage, cacheLocalStorage, delayMs, handleSuccessAsync, handleSuccess } from '../../util/index'
import { permissions, menusData, speedInfoTimer, pageRefTags } from '../../config'
import { mapState, mapActions, mapMutations, mapGetters } from 'vuex'
import projectSelect from '../project/project_select'
import changeLang from '../common/change_lang'
import help from '../common/help'
import KapDetailDialogModal from '../common/GlobalDialog/dialog/detail_dialog'
import Diagnostic from '../admin/Diagnostic/index'
import Capacity from '../admin/SystemCapacity/CapacityTopBar'
import $ from 'jquery'
import moment from 'moment'
import ElementUI from 'kyligence-ui'
import GuideModal from '../studio/StudioModel/ModelList/GuideModal/GuideModal.vue'
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
      getSpeedInfo: 'GET_SPEED_INFO',
      getQuotaInfo: 'GET_QUOTA_INFO',
      clearTrash: 'CLEAR_TRASH'
    }),
    ...mapMutations({
      setCurUser: 'SAVE_CURRENT_LOGIN_USER',
      resetProjectState: 'RESET_PROJECT_STATE',
      toggleMenu: 'TOGGLE_MENU',
      cacheHistory: 'CACHE_HISTORY',
      saveTabs: 'SET_QUERY_TABS',
      resetSpeedInfo: 'CACHE_SPEED_INFO',
      setProject: 'SET_PROJECT',
      setGlobalAlter: 'SET_GLOBAL_ALTER'
    }),
    ...mapActions('UserEditModal', {
      callUserEditModal: 'CALL_MODAL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  components: {
    'project_select': projectSelect,
    'change_lang': changeLang,
    help,
    KapDetailDialogModal,
    Diagnostic,
    Capacity,
    GuideModal
  },
  computed: {
    ...mapState({
      cachedHistory: state => state.config.cachedHistory,
      isSemiAutomatic: state => state.project.isSemiAutomatic,
      licenseDates: state => state.system.serverAboutKap,
      currentUser: state => state.user.currentUser,
      showRevertPasswordDialog: state => state.system.showRevertPasswordDialog,
      capacityAlert: state => state.capacity.capacityAlert
    }),
    ...mapGetters([
      'currentPathNameGet',
      'isFullScreen',
      'currentSelectedProject',
      'briefMenuGet',
      'currentProjectData',
      'availableMenus',
      'isAutoProject',
      'isGuideMode',
      'isOnlyQueryNode',
      'dashboardActions',
      'isTestingSecurityProfile'
    ]),
    modelSpeedEvents () {
      return this.$store.state.model.modelSpeedEvents
    },
    reachThreshold () {
      return this.$store.state.model.reachThreshold
    },
    reachThresholdVisible () {
      return this.$store.state.model.reachThreshold && this.$store.state.project.projectAutoApplyConfig && this.manualClose && this.$store.state.config.platform !== 'iframe'
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
    },
    canAddProject () {
      // 模型编辑页面的时候，新增项目的按钮不可点
      return this.$route.name !== 'ModelEdit'
    },
    isShowAlter () {
      const isGlobalAlter = this.$store.state.capacity.maintenance_mode || this.capacityAlert
      if (this.$store.state.capacity.maintenance_mode) {
        this.globalAlterTips = { text: this.$t('systemUprade'), flag: 0 }
      } else if (this.capacityAlert) {
        this.globalAlterTips = { ...this.capacityAlert, text: this.$t(`kylinLang.capacity.${this.capacityAlert.text}`, this.capacityAlert.query ? this.capacityAlert.query : {}), detailPath: this.capacityAlert.detailPath }
      }
      this.setGlobalAlter(isGlobalAlter)
      return isGlobalAlter
    }
  },
  locales: {
    'en': {
      contactSales: 'Concat Sales',
      gotit: 'Got It',
      serviceOvertip1: 'The technical support service will expire in ',
      serviceOvertip2: ' days. After the expiration date, you couldn\'t use the support service and the ticket system. Please contact the sales to extend your service time if needed.',
      serviceEndDate: 'Service End Time: ',
      storageQuota: 'Storage Quota',
      settingTips: 'Configure',
      useageMana: 'Used Storage: ',
      trash: 'Low Usage Storage',
      tarshTips: 'Low usage storage refers to the obsolete files generated after the system has been running for a period of time. For more details, please refer to <a href="https://docs.kyligence.io/books/v4.5/en/Operation-and-Maintenance-Guide/junk_file_clean.en.html" target="_blank">user manual</a>.',
      clear: 'Clear',
      resetPassword: 'Reset Password',
      confirmLoginOut: 'Are you sure you want to log out?',
      validPeriod: 'Valid Period: ',
      overtip1: 'This License will be expired in ',
      overtip2: 'days. Please contact sales support to apply for the Enterprise License.',
      applayLisence: 'Contact Sales',
      'continueUse': 'Got It',
      speedTip: 'System will accelerate <span class="ky-highlight-text">{queryCount}</span> queries. <span class="ky-highlight-text">{modelCount}</span> {speedModel} would be optimized. Do you want to apply it?',
      ignore: 'Ignore',
      apply: 'Apply',
      hello: 'Hi {user},',
      leaveAdmin: 'Going to leave the administration mode.',
      enterAdmin: 'Hi, welcome to the administration mode.',
      accelerateTips: 'Accelerating Tips',
      noProject: 'No project now. Please create one project via the top bar.',
      indexs: 'index group(s)',
      models: 'model(s)',
      holdNaviBar: 'Collapse',
      unholdNaviBar: 'Expand',
      diagnosis: 'Diagnosis',
      disableAddProject: 'Can not create project in edit mode',
      systemUprade: 'System is currently undergoing maintenance. Metadata related operations are temporarily unavailable.',
      onlyQueryNode: 'There\'s no active job node now. Metadata related operations are temporarily unavailable.',
      viewDetails: 'View details',
      quotaSetting: 'Storage Setting',
      noAuthorityText: 'No project found. Please contact your administrator to create a project.'
    },
    'zh-cn': {
      contactSales: '联系销售',
      gotit: '知道了',
      serviceOvertip1: '技术支持服务将在 ',
      serviceOvertip2: ' 天后到期。过期后您将无法使用该产品的技术支持服务，相应的工单系统权限也将关闭。请联系销售支持人员申请延期。',
      serviceEndDate: '服务截止日期：',
      storageQuota: '存储配额',
      settingTips: '设置',
      useageMana: '已使用：',
      trash: '低效存储',
      tarshTips: '低效存储指的是系统运行一段时间后产生的垃圾文件，详情请<a href="https://docs.kyligence.io/books/v4.5/zh-cn/Operation-and-Maintenance-Guide/junk_file_clean.cn.html" target="_blank">查看手册</a>。',
      clear: '清除',
      resetPassword: '重置密码',
      confirmLoginOut: '确认要注销吗？',
      validPeriod: '使用期限：',
      overtip1: '当前使用的许可证将在 ',
      overtip2: '天后过期。欢迎联系销售支持人员申请企业版许可证。',
      applayLisence: '联系销售',
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
      holdNaviBar: '收起',
      unholdNaviBar: '展开',
      diagnosis: '诊断',
      disableAddProject: '编辑模式下不可新建项目',
      systemUprade: '系统已进入维护模式，元数据相关操作暂不可用。',
      onlyQueryNode: '系统中暂无活跃的任务节点，元数据相关操作暂不可用。',
      viewDetails: '查看详情',
      quotaSetting: '存储配额设置',
      noAuthorityText: '当前暂无项目。请联系管理员新建项目。'
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
  licenseServiceDialogVisible = false
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
  showChangePassword = false
  globalAlterTips = {}
  quotaInfo = {
    storage_quota_size: -1,
    total_storage_size: -1,
    garbage_storage_size: -1
  }
  useageRatio = 0
  showQuota = false

  gotoSetting () {
    this.showQuota = false
    this.$router.push('/setting')
  }

  async loadQuotaInfo () {
    const res = await this.getQuotaInfo({project: this.currentSelectedProject})
    const resData = await handleSuccessAsync(res)
    this.quotaInfo = resData
    this.useageRatio = (resData.total_storage_size / resData.storage_quota_size).toFixed(4)
  }

  get getQuotaColor () {
    if (+this.useageRatio >= 1) {
      return 'is-danger'
    } else if (+this.useageRatio >= 0.8 && +this.useageRatio < 1) {
      return 'is-warning'
    } else {
      return 'is-success'
    }
  }

  clearStorage () {
    if (!this.currentSelectedProject || !this.quotaInfo.garbage_storage_size || this.quotaInfo.garbage_storage_size === -1) {
      return
    }
    this.clearTrash({project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, () => {
        this.loadQuotaInfo()
      })
    }, (res) => {
      handleError(res)
    })
  }

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
    // if (!this.isGuideMode) {
    //   this.noProjectTips()
    // }
    setTimeout(() => {
      this.changeRouteEvent('route')
    }, 500)
  }
  setGlobalMask (notifyContect) {
    this.isGlobalMaskShow = true
    this.notifyContect = this.$t(notifyContect, this.currentUserInfo)
  }
  hideGlobalMask () {
    this.isGlobalMaskShow = false
    this.notifyContect = ''
  }
  concatSales () {
    let lang = localStorage.getItem('kystudio_lang') === 'zh-cn' ? 'zh' : 'en'
    let url = `http://account.kyligence.io/#/?lang=${lang}/license`
    window.open(url)
  }
  async handleSwitchAdmin () {
    this.setGlobalMask(this.isAdminView ? 'leaveAdmin' : 'enterAdmin')
    await delayMs(1700)
    if (this.isAdminView) {
      const nextLocation = this.cachedHistory ? this.cachedHistory : '/dashboard'
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
      // handleError(res)
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

    this.changeRouteEvent('created')
    // 获取许可证接口成功之后再弹无项目弹窗
    this.getAboutKap().then(() => {
      // 新手引导模式不用反复弹提示
      if (!this.isGuideMode && !this.showChangePassword) {
        this.noProjectTips()
      }
    }).catch((res) => {
      handleError(res)
    })
    this.loadQuotaInfo()
  }
  changeRouteEvent (from) {
    if (this.showRevertPasswordDialog === 'true' && 'defaultPassword' in this.currentUser && this.currentUser.defaultPassword) {
      this.showChangePassword = true
      this.callUserEditModal({ editType: 'password', showCloseBtn: false, showCancelBtn: false, userDetail: this.$store.state.user.currentUser }).then(() => {
        this.noProjectTips()
      })
    } else if (from !== 'created') {
      this.noProjectTips()
    }
  }
  showMenuByRole (menuName) {
    let isShowSnapshot = true
    let isShowStreamingJob = true
    if (menuName === 'snapshot') {
      isShowSnapshot = this.$store.state.project.snapshot_manual_management_enabled
    }
    if (menuName === 'streamingjob') {
      isShowStreamingJob = this.$store.state.system.streamingEnabled === 'true'
    }
    return this.availableMenus.includes(menuName.toLowerCase()) && isShowSnapshot && isShowStreamingJob
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
      } else if (currentPathName === 'ModelSubPartitionValues' || currentPathName === 'ModelDetails') {
        this.$router.replace({name: 'ModelList', params: { refresh: true }})
      } else {
        this.$router.replace({name: currentPathName, params: { refresh: true }})
        currentPath && (this.defaultActive = currentPath)
      }
    })
  }
  _isAjaxProjectAcess (allProject, curProjectName, currentPathName, currentPath) {
    let curProjectUserAccess = this.getUserAccess({project: curProjectName})
    Promise.all([curProjectUserAccess]).then((res) => {
      this._replaceRouter(currentPathName, currentPath)
    })
  }
  changeProject (val) {
    if (this.$store.state.system.isEditForm) {
      // agg group 弹框打开，有修改时取消确认
      this.callGlobalDetailDialog({
        msg: this.$t('kylinLang.common.unSavedTips'),
        title: this.$t('kylinLang.common.tip'),
        dialogType: 'warning',
        showDetailBtn: false,
        needConcelReject: true,
        hideBottomLine: true,
        wid: '420px',
        submitText: this.$t('kylinLang.common.discardChanges')
      }).then(() => {
        this.setProject(val)
        var currentPathName = this.$router.currentRoute.name
        var currentPath = this.$router.currentRoute.path
        this._isAjaxProjectAcess(this.$store.state.project.allProject, val, currentPathName, currentPath)
        this.loadQuotaInfo()
      }).catch(() => {
        this.$nextTick(() => {
          let preProject = this.$store.state.project.selected_project // 恢复上一次的project
          this.$refs['projectSelect'].selected_project = preProject
        })
      })
    } else {
      this.setProject(val)
      var currentPathName = this.$router.currentRoute.name
      var currentPath = this.$router.currentRoute.path
      this._isAjaxProjectAcess(this.$store.state.project.allProject, val, currentPathName, currentPath)
      this.loadQuotaInfo()
    }
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
    return kapConfirm(this.$t('confirmLoginOut'), {
      type: 'warning',
      confirmButtonText: this.$t('kylinLang.common.logout')
    }, this.$t('kylinLang.common.logout'))
  }
  changePassword (userDetail) {
    this.callUserEditModal({ editType: 'password', userDetail })
  }
  loginOutFunc () {
    if (this.$route.name === 'ModelEdit') {
      MessageBox.confirm(window.kapVm.$t('kylinLang.common.willGo'), window.kapVm.$t('kylinLang.common.notice'), {
        confirmButtonText: window.kapVm.$t('kylinLang.common.logout'),
        cancelButtonText: window.kapVm.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(() => {
        this.logoutConfirm().then(() => {
          this.loginOut().then(() => {
            localStorage.setItem('buyit', false)
            localStorage.setItem('loginIn', false)
            this.removeAllPagerSizeCache()
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
          localStorage.setItem('loginIn', false)
          this.removeAllPagerSizeCache()
          // reset 所有的project信息
          this.resetProjectState()
          this.resetQueryTabs()
          this.$router.push({name: 'Login'})
        })
      })
    }
  }
  removeAllPagerSizeCache () {
    for (let p in pageRefTags) {
      const pager = pageRefTags[p]
      if (localStorage.getItem(pager)) {
        localStorage.removeItem(pager)
      }
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
  get isShowRouterView () {
    return this.isAdmin || (!this.isAdmin && !this.highlightType)
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
  get KESeriveDate () {
    return this.defaultVal(this.kapInfo && this.kapInfo['ke.license.serviceEnd'] || null)
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
  get lastServiceEndTime () {
    var date = this.serverAboutKap && this.serverAboutKap['ke.license.serviceEnd'] || ''
    let isEvaluation = this.serverAboutKap && this.serverAboutKap['ke.license.isEvaluation']
    if (date === 'No') {
      return 0
    } else {
      var currentDate = moment(new Date()).add(0, 'year').format('YYYY-MM-DD')
      var timeLeft = moment(date).diff(moment(currentDate), 'days')
      if (timeLeft <= 30) {
        if (!this.$store.state.config.overLock && this.$store.state.config.platform !== 'iframe' && !isEvaluation) {
          this.licenseServiceDialogVisible = true
          this.$store.state.config.overLock = true
        }
        localStorage.setItem('buyit', true)
        return timeLeft
      }
    }
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
          // 不是嵌套在其他项目中的，才出现这个弹窗提示
          if (!this.$store.state.config.overLock && this.$store.state.config.platform !== 'iframe') {
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
      // 有加速引擎页面包括智能模式，AI增强模式开启智能推荐设置（且有ACL权限的用户）
      if (!this.isStartCircleLoadSpeedInfo || this.applyBtnLoading || this.btnLoadingCancel || this.$store.state.model.circleSpeedInfoLock || !this.$store.state.project.projectAutoApplyConfig) {
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
    if (this.highlightType && this.licenseDates['ke.dates'] && this.currentUser && this.isAdmin) {
      MessageBox.alert(this.$t('noProject'), this.$t('kylinLang.common.notice'), {
        confirmButtonText: this.$t('kylinLang.common.ok'),
        type: 'info'
      })
    }
  }
  get isStartCircleLoadSpeedInfo () {
    return this.availableMenus.includes('acceleration') && (this.isAutoProject || !this.isAutoProject && this.$store.state.project.isSemiAutomatic)
  }
  mounted () {
    // 接受cloud的参数
    /* var from = getQueryString('from')
    var uimode = getQueryString('uimode') // 界面展示模式
    if (from === 'cloud' || uimode === 'nomenu') { // 保留cloud，兼容云老参数
      $('#fullBox').addClass('cloud-frame-page')
    } */
    // 有加速引擎页面包括智能模式，AI增强模式开启智能推荐设置（且有ACL权限的用户）
    // if (this.isStartCircleLoadSpeedInfo) {
    //   获取加速信息
    //   this.loadSpeedInfo()
    //   this.circleLoadSpeedInfo()
    // }
  }
  destroyed () {
  }
  @Watch('currentPathNameGet')
  onCurrentPathNameGetChange (val) {
    this.defaultActive = val
  }
  showDiagnosticDialog () {
    this.showDiagnostic = true
  }
  jumpToDetails () {
    this.globalAlterTips.detailPath && this.$router.push(this.globalAlterTips.detailPath)
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  .quota-popover {
    position: relative;
    // .el-icon-ksd-setting {
    //   position: absolute;
    //   right: 10px;
    //   &:hover {
    //     color: @base-color;
    //   }
    // }
    .quota-popover-layout {
      .contain {
        display: flex;
        justify-content: space-between;
      }
    }
    .setting-icon-place {
      i {
        &:hover {
          color: @base-color;
        }
      }
    }
    .quota-status {
      &.is-danger {
        color: @error-color-1;
      }
      &.is-warning {
        color: @warning-color-1;
      }
      &.is-success {
        color: @text-normal-color;
      }
    }
    .trash-tips-icon {
      color: @text-disabled-color;
    }
  }
  .quota-top-bar {
    position: relative;
    min-width: 65px;
    .quota-info {
      font-size: 14px;
      height: 22px;
      line-height: 22px;
      .quota-title {
        font-weight: @font-regular;
        &:hover {
          color: @base-color;
        }
      }
      .flag {
        width: 10px;
        height: 10px;
        display: inline-block;
        border-radius: 100%;
        &.is-danger {
          background-color: @error-color-1;
        }
        &.is-warning {
          background-color: @warning-color-1;
        }
        &.is-success {
          background-color: @normal-color-1;
        }
      }
    }
    .clear-btn {
      font-size: 16px;
      color: @base-color;
      &.is_no_quota {
        font-size: 18px;
      }
      &:hover {
        color: @base-color-2;
      }
      &.is-disabled {
        color: @text-disabled-color;
        cursor: not-allowed;
      }
    }
  }
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
  .alter-block {
    position: fixed;
    top: 48px;
    width: 100%;
    z-index: 1;
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
    &:not(.isModelList){
      .grid-content.bg-purple-light {
        .main-content {
          height: 100%;
        }
      }
    }
    .grid-content.bg-purple-light {
      overflow: auto;
      height: 100%;
      min-width: 960px;
      .main-content {
        box-sizing: border-box;
        background: white;
        position: relative;
        .blank-content {
            padding-top: 270px;
            color: @text-placeholder-color;
            width: 410px;
            margin: 0 auto;
            text-align: center;
            &.en {
              width: 380px;
            }
            i {
              font-size: 60px;
            }
            .text {
              font-size: 14px;
              font-weight: 400;
              line-height: 20px;
              margin-top: 15px;
              text-align: center;
            }
        }
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
        width: 64px;
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
          width: 24px;
          height: 22px;
          margin: 17px 15px 12px 15px;
        }
      }
      .topbar .nav-icon {
        margin-left: 76px;
      }
      .panel-content{
        left: 64px;
      }
    }
    &:not(.isModelList){
      .panel {
        .panel-center {
          .panel-content {
            overflow-y: auto;
          }
        }
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
          top: 48px;
          bottom: 0;
          left: 184px;
          overflow-x: auto;
        }
        .left-menu {
          width: 184px;
          height: 100%;
          position: relative;
          z-index: 999;
          background-color: @ke-color-nav;
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
            width: 100%;
            // height: 26px;
            vertical-align: middle;
            z-index:999;
            margin: 13px 0 13px 0;
            cursor: pointer;
          }
          .ky-line {
            background: #053C6C;
          }
        }
        .topbar{
          height: 47px;
          width: 100%;
          background: @ke-background-color-secondary;
          position: absolute;
          top:0;
          border-bottom: 1px solid @ke-border-divider-color;
          // box-shadow: 0 1px 2px 0 @line-split-color;
          z-index: 100;
          .nav-icon {
            margin-left: 202px;
            margin-top: 10px;
            height: 14px;
            line-height: 14px;
            cursor: pointer;
            float: left;
          }
          .add-project-btn {
            margin: 10px 0 0 0;
          }
          .top-ul {
            font-size:0;
            margin-top: 7px;
            >li{
              vertical-align: middle;
              display: inline-block;
              margin-right: 24px;
              cursor: pointer;
              &.capacity-li {
                padding-right: 18px;
                margin-right: 10px;
                border-right: 1px solid @ke-border-secondary;
              }
              &:last-child {
                margin-right: 10px;
              }
            }
            .active-nodes {
              font-size: 14px;
              height: 22px;
              line-height: 22px;
              &:hover {
                color: @base-color;
              }
              .success-text {
                color: @normal-color-1;
              }
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
              // height: 24px;
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
      // box-shadow: inset 1px 1px 2px 0 #ADC2D0;
      // border-color: @base-color-11;
      // color: @base-color-11;
      background-color: @ke-background-color-active;
      &:hover,
      &:focus {
        background-color: @ke-background-color-active;
        border-color: @ke-background-color-active;
      }

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
  .cloud-frame-page {
    .data-source-bar {
     .el-tree > .el-tree-node > .el-tree-node__content {
       display: none;
     }
      .el-tree > .el-tree-node > .el-tree-node__children > .el-tree-node {
        border-top: none;
      }
    }
    .aggregate-modal {
      top: 0px;
      width: 100%;
      .dialog-footer {
        width: calc(~'100% - 40px');
      }
    }
  }
  .limit-user-name {
    // height: 20px;
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
    overflow: hidden;
    text-overflow: ellipsis;
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
      /* margin-bottom: 1px; */
    }
  }
  .el-menu--popup {
    margin-left: 1px;
    padding: 0px;
    // background-color: @text-title-color!important;
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
    /*.el-menu-item.is-active {
      color: @base-color-1;
      background-color: @menu-active-bgcolor;
    }
    .el-menu-item:hover {
      background-color: @menu-active-bgcolor;
    }*/
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
  .diagnosis-tip {
    margin-left: 30px !important;
  }
</style>
