<template>
  <div id="homepage">
    <div class="homepage-content">
      <!-- <div class="ksd-title-label ksd-fs-24">{{$t('aiAugmented')}}</div>
      <div class="ksd-mt-10">{{$t('aiAugmentedDesc')}}</div> -->
      <div class="ksd-mb-10 combination-block clearfix">
        <div class="left-block">
          <div class="card ksd-fright clearfix">
            <div class="ksd-fleft card-img">
              <!-- <img src="../../assets/img/data_source@2x.png" alt=""> -->
              <el-icon name="el-ksd-icon-data_storge_24" type="mult"></el-icon>
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('datasource')}}</div>
              <div class="content" v-html="$t('datasourceDesc', {database_size: infoData.database_size, table_size: infoData.table_size})"></div>
              <!-- <div class="card-link ky-a-like" @click="gotoDatasource('create')">{{$t('manageDatasource')}}</div> -->
              <el-button class="card-link" type="primary" text @click="gotoDatasource('create')">{{$t('manageDatasource')}}</el-button>
            </div>
          </div>
          <div class="card ksd-fright clearfix">
            <div class="ksd-fleft card-img">
              <!-- <img src="../../assets/img/history@2x.png" alt=""> -->
              <el-icon name="el-ksd-icon-query_history_24" type="mult"></el-icon>
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('history')}}</div>
              <div class="content">
                <span v-html="$t('historyDesc', {last_week_query_count: infoData.last_week_query_count})"/>
                <el-tooltip placement="top" :content="$t('historyDescTip')">
                  <i class="el-icon-ksd-info ksd-ml-2 tips"></i>
                </el-tooltip>
              </div>
              <!-- <div class="card-link ky-a-like" @click="gotoHistory">{{$t('viewDetail')}}</div> -->
              <el-button class="card-link" type="primary" text @click="gotoHistory">{{$t('viewDetail')}}</el-button>
            </div>
          </div>
          <div class="card ksd-fright clearfix">
            <div class="ksd-fleft card-img">
              <!-- <img src="../../assets/img/data_profile@2x.png" alt=""> -->
              <el-icon name="el-ksd-icon-data_characteristics_24" type="mult"></el-icon>
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('dataProfile')}}</div>
              <div class="content">{{$t('dataProfileDesc')}}</div>
              <!-- <div class="card-link ky-a-like" @click="gotoDatasource('load')">{{$t('viewDetail')}}</div> -->
              <el-button class="card-link" type="primary" text @click="gotoDatasource('load')">{{$t('viewDetail')}}</el-button>
            </div>
          </div>
        </div>
        <div class="middle-block">
          <div class="circle-block clearfix">
            <!-- <div class="ksd-fleft line-img">
              <img src="../../assets/img/arrow_left@2x.png" height="460px" alt="">
            </div> -->
            <div class="ksd-fleft circle-img" @mouseenter="isShowGif = true" @mouseleave="isShowGif = false">
              <img class="ksd-fleft" v-if="isAcceing" src="../../assets/img/CUT2_comp.gif" width="100%" alt=""/>
              <template v-else>
                <img class="ksd-fleft" v-if="isShowGif" src="../../assets/img/CUT1_comp.gif" width="100%" alt=""/>
                <img class="ksd-fleft" v-else src="../../assets/img/CUT3.png" width="100%" alt=""/>
              </template>
              <div class="circle-content">
                <img :class="{'en-img': isOpenSemiAutomatic}" src="../../assets/img/language=EN.png" v-if="$lang === 'en'" alt="">
                <img src="../../assets/img/language=CN.png" v-else alt="">
                <div v-if="!isOpenSemiAutomatic">
                  <div class="circle-desc" :class="{'en': $lang === 'en'}">{{$t('openRecommendationDesc')}}</div>
                  <common-tip :content="$t('noPermissionTips')" v-if="!datasourceActions.includes('acceRuleSettingActions')" placement="top">
                    <div class="circle-btn disabled">
                      <!-- <i class="ksd-fs-20 el-icon-ksd-project_status"></i> -->
                      {{$t('turnOnRecommendation')}}
                    </div>
                  </common-tip>
                  <div class="circle-btn" v-else @click="turnOnRecommendation">
                    <!-- <i class="ksd-fs-20 el-icon-ksd-project_status"></i> -->
                    {{$t('turnOnRecommendation')}}
                  </div>
                  <!-- <el-button class="circle-btn" type="primary" v-else @click="turnOnRecommendation">{{$t('turnOnRecommendation')}}</el-button> -->
                </div>
                <div v-else>
                  <!-- <div class="circle-desc" :class="{'en': $lang === 'en'}" v-if="+infoData.unhandled_query_count>0" v-html="$t('optimizeDesc', {unhandled_query_count: infoData.unhandled_query_count})"></div> -->
                  <div class="circle-desc none-opt" :class="{'en': $lang === 'en'}" v-html="$t('recommendCount', {count: infoData.approved_rec_count + infoData.acceptable_rec_size})"/>
                  <common-tip :content="disabledAcceTips" v-if="(isAcce || isAcceing) && +infoData.unhandled_query_count !== 0 || !datasourceActions.includes('accelerationActions')" placement="top">
                    <div class="circle-btn disabled">
                      <!-- <i class="ksd-fs-20" :class="{'el-icon-ksd-project_status': !isAcceing, 'el-icon-loading': isAcceing}"></i> -->
                      <span v-if="isAcce || isAcceing">{{$t('optimizing')}}</span>
                      <span v-else>{{$t('optimize')}}</span>
                    </div>
                  </common-tip>
                  <div class="circle-btn" v-else @click="toAcce">
                    <!-- <i class="ksd-fs-20" :class="{'el-icon-ksd-project_status': !isAcceing, 'el-icon-loading': isAcceing}"></i> -->
                    <span v-if="isAcceing">{{$t('optimizing')}}</span>
                    <span v-else>{{$t('optimize')}}</span>
                  </div>
                </div>
                <div class="recommend-block" :class="{'recmmend-zh': $lang !== 'en'}">
                  <el-popover
                    ref="patternPopover"
                    placement="top"
                    :title="$t('recommendation')"
                    width="310"
                    popper-class="recommend-popover"
                    trigger="click">
                      <div v-html="isOpenSemiAutomatic ? $t('recommendationDsec', {
                        approved_rec_count: infoData.approved_rec_count,
                        approved_additional_rec_count: infoData.approved_additional_rec_count,
                        approved_removal_rec_count: infoData.approved_removal_rec_count
                      }) : $t('recommendationDsecByTurnOff')"></div>
                      <div class="recommend-content no-border" slot="reference">
                        <!-- <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-pattern ksd-fleft"></i></div> -->
                        <div class="ksd-fleft ksd-ml-5 re-content">
                          <div class="re-count">{{isOpenSemiAutomatic ? infoData.approved_rec_count : '-'}}</div>
                          <div class="popover-btn">{{$t('acceptedRecs')}}</div>
                        </div>
                      </div>
                  </el-popover>
                  <!-- <el-popover
                    ref="patternPopoverOff"
                    placement="top"
                    :title="$t('pattern')"
                    width="460"
                    popper-class="recommend-popover"
                    v-else
                    trigger="click">
                    <div v-html="$t('patternDsecByTurnOff')"></div>
                    <div class="recommend-content no-border clearfix" slot="reference">
                      <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-pattern ksd-fleft"></i></div>
                      <div class="ksd-fleft ksd-ml-5 re-content">
                        <div class="popover-btn">{{$t('pattern')}}</div>
                        <div class="re-count">{{infoData.rec_pattern_count}}</div>
                      </div>
                    </div>
                  </el-popover> -->
                  <el-popover
                    ref="pendingAcceptPopover"
                    placement="top"
                    :title="$t('pendingAcceptTitle')"
                    width="310"
                    popper-class="recommend-popover"
                    trigger="click">
                    <!-- <div v-html="$t('ruleDsec')"></div>
                    <div class="ksd-mt-5" style="text-align: right; margin: 0" v-if="isOpenSemiAutomatic&&datasourceActions.includes('acceRuleSettingActions')">
                      <el-button type="primary" text size="mini" @click="gotoSettingRules">{{$t('modifySettings')}}</el-button>
                    </div> -->
                    <p>{{isOpenSemiAutomatic ? $t('pendingAcceptContent', {recs: infoData.acceptable_rec_size}) : $t('recommendationDsecByPendingTurnOff')}}</p>
                    <div class="recommend-content" slot="reference">
                      <!-- <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-rules ksd-fleft"></i></div> -->
                      <div class="ksd-fleft ksd-ml-5 re-content">
                        <div class="re-count">{{isOpenSemiAutomatic ? infoData.acceptable_rec_size : '-'}}</div>
                        <div class="popover-btn">{{$t('pendingAccept')}}</div>
                      </div>
                    </div>
                  </el-popover>
                  <!-- <el-popover
                    ref="rulePopoverOff"
                    placement="top"
                    :title="$t('rule')"
                    width="460"
                    popper-class="recommend-popover"
                    v-else
                    trigger="click">
                    <div v-html="$t('ruleDsecByTurnOff')"></div>
                    <div class="recommend-content" slot="reference">
                      <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-rules ksd-fleft"></i></div>
                      <div class="ksd-fleft ksd-ml-5 re-content">
                        <div class="popover-btn">{{$t('rule')}}</div>
                        <div class="re-count">{{infoData.effective_rule_size}}</div>
                      </div>
                    </div>
                  </el-popover> -->
                  <el-popover
                    ref="rulePopover"
                    placement="top"
                    :title="$t('preference')"
                    width="260"
                    popper-class="recommend-popover"
                    trigger="click">
                    <!-- <div v-html="$t('recommendationDsec', {
                      approved_rec_count: infoData.approved_rec_count,
                      approved_additional_rec_count: infoData.approved_additional_rec_count,
                      approved_removal_rec_count: infoData.approved_removal_rec_count
                    })"></div> -->
                    <div v-html="$t('ruleDsec')"></div>
                    <div class="ksd-mt-5" style="text-align: right; margin: 0" v-if="isOpenSemiAutomatic&&datasourceActions.includes('acceRuleSettingActions')">
                      <el-button type="primary" text size="mini" @click="gotoSettingRules">{{$t('modifySettings')}}</el-button>
                    </div>
                    <div class="recommend-content" slot="reference">
                      <!-- <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-status ksd-fleft"></i></div> -->
                      <div class="ksd-fleft ksd-ml-5 re-content">
                        <div class="re-count">{{isOpenSemiAutomatic ? infoData.effective_rule_size : '-'}}</div>
                        <div class="popover-btn">{{$t('preference')}}</div>
                      </div>
                    </div>
                  </el-popover>
                  <!-- <el-popover
                    ref="recommendationPopoverOff"
                    placement="top"
                    :title="$t('recommendation')"
                    width="260"
                    popper-class="recommend-popover"
                    v-else
                    trigger="click">
                    <div v-html="$t('recommendationDsecByTurnOff')"></div>
                    <div class="recommend-content" slot="reference">
                      <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-status ksd-fleft"></i></div>
                      <div class="ksd-fleft ksd-ml-5 re-content">
                        <div class="popover-btn">{{$t('recommendation')}}</div>
                        <div class="re-count">{{infoData.approved_rec_count}}</div>
                      </div>
                    </div>
                  </el-popover> -->
                </div>
                <div :class="['ky-a-like', 'advance-setting', {'is-disabled': !(isOpenSemiAutomatic && datasourceActions.includes('modelActions') && this.currentSelectedProject)}]">
                  <span @click="showGenerateModelDialog">
                    <!-- <i class="el-icon-ksd-load"></i> -->
                    <span>{{$t('importQueries')}}</span>
                  </span>
                </div>
              </div>
            </div>
            <!-- <div class="ksd-fright line-img">
              <img src="../../assets/img/arrow_right@2x.png" height="460px" alt="">
            </div> -->
          </div>
        </div>
        <div class="right-block">
          <div class="card ksd-mr-20 clearfix">
            <div class="ksd-fleft card-img">
              <!-- <img src="../../assets/img/model@2x.png" width="44px" alt=""> -->
              <el-icon name="el-ksd-icon-model_24" type="mult"></el-icon>
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('model')}}</div>
              <div class="content">
                <div class="ksd-inline" v-if="!$store.state.project.isSemiAutomatic" v-html="$t('modelDesc', {model_size: infoData.model_size})"></div>
                <div class="ksd-inline" v-else v-html="$t('modelDesc2', {model_size: infoData.model_size, acceptable_rec_size: infoData.acceptable_rec_size})"></div>
              </div>
              <!-- <div class="card-link ky-a-like" @click="gotoModelList">{{$t('viewDetail')}}</div> -->
              <el-button class="card-link" type="primary" text @click="gotoModelList">{{$t('viewDetail')}}</el-button>
              <el-tag class="new-tag" size="mini" type="success" v-if="infoData.is_refreshed">New</el-tag>
            </div>
          </div>
          <div class="card ksd-mr-20 clearfix">
            <div class="ksd-fleft card-img">
              <!-- <img src="../../assets/img/insight@2x.png" width="44px" alt=""> -->
              <el-icon name="el-ksd-icon-query_24" type="mult"></el-icon>
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('quickInsight')}}</div>
              <div class="content">{{$t('quickInsightDesc')}}</div>
              <!-- <div class="card-link ky-a-like" @click="gotoInsight">{{$t('queryNow')}}</div> -->
              <el-button class="card-link" type="primary" text @click="gotoInsight">{{$t('queryNow')}}</el-button>
            </div>
          </div>
          <div class="card ksd-mr-20 clearfix">
            <div class="ksd-fleft card-img">
              <!-- <img src="../../assets/img/connect_to_BI@2x.png" width="44px" alt=""> -->
              <el-icon name="el-ksd-icon-connect_BI_24" type="mult"></el-icon>
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('connectToBI')}}</div>
              <div v-if="platform === 'iframe'" class="content">{{$t('connectToBIDescForKC')}}</div>
              <div class="content" v-else>{{$t('connectToBIDesc')}}</div>
              <!-- <p class="card-link ky-a-like" @click="openManualUrl" :href="$t('manualUrl')" target="_blank">
                <span v-if="platform === 'iframe'">{{$t('connectNow')}}</span>
                <span v-else>{{$t('learnMore')}}</span>
              </p> -->
              <el-button class="card-link" type="primary" text @click="openManualUrl" :href="$t('manualUrl')">{{platform === 'iframe' ? $t('connectNow') : $t('learnMore')}}</el-button>
            </div>
          </div>
        </div>
      </div>
    </div>
    <!-- sql建模添加 -->
    <UploadSqlModel v-on:reloadModelList="init"/>
    <!-- 模型添加 -->
    <ModelAddModal/>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync, handleError } from '../../util/index'
import { postCloudUrlMessage } from '../../util/business'
import UploadSqlModel from '../common/UploadSql/UploadSql.vue'
import ModelAddModal from '../studio/StudioModel/ModelList/ModelAddModal/addmodel'
import locales from './locales'

@Component({
  computed: {
    ...mapState({
      platform: state => state.config.platform
    }),
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions',
      'briefMenuGet'
    ])
  },
  components: {
    UploadSqlModel,
    ModelAddModal
  },
  methods: {
    ...mapActions({
      loadStatistics: 'LOAD_PROJECT_STATISTICS',
      getAccelerationStatus: 'GET_ACCELERATION_STATUS',
      accelerateModel: 'ACCELERATE_MODEL',
      updateProjectGeneralInfo: 'UPDATE_PROJECT_GENERAL_INFO',
      fetchProjectSettings: 'FETCH_PROJECT_SETTINGS'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapActions('UploadSqlModel', {
      showUploadSqlDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelAddModal', {
      callAddModelDialog: 'CALL_MODAL'
    }),
    ...mapActions({
      getModelByModelName: 'LOAD_MODEL_INFO'
    })
  },
  locales
})
export default class Homepage extends Vue {
  infoData = {
    acceptable_rec_size: '-',
    additional_rec_pattern_count: '-',
    approved_additional_rec_count: '-',
    approved_rec_count: '-',
    approved_removal_rec_count: '-',
    database_size: '-',
    effective_rule_size: '-',
    is_refreshed: false,
    last_week_query_count: '-',
    model_size: '-',
    rec_pattern_count: '-',
    refreshed: false,
    removal_rec_pattern_count: '-',
    table_size: '-',
    unhandled_query_count: '0',
    max_rec_show_size: '-'
  }
  isAcce = false // 初始状态后台有优化任务在跑
  isShowGif = false
  isAcceing = false // 提交一键优化，优化ing
  gotoSettingRules () {
    if (this.platform === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'kapSetting' })
    } else {
      this.$router.push({path: '/setting', query: {moveTo: 'index-suggest-setting'}})
    }
  }
  gotoDatasource (type) {
    if (this.platform === 'iframe') {
      let routeName = type === 'create' ? 'datasource-create' : 'datasource-sync'
      postCloudUrlMessage(this.$route, { name: routeName })
    } else {
      this.$router.push({path: '/studio/source'})
    }
  }
  gotoHistory () {
    if (this.platform === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'kapHistory', params: {source: 'homepage-history'} })
    } else {
      this.$router.push({name: 'QueryHistory', params: {source: 'homepage-history'}})
    }
  }
  gotoModelList () {
    if (this.platform === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'kapModel' })
    } else {
      this.$router.push({path: '/studio/model'})
    }
  }
  gotoInsight () {
    if (this.platform === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'kapInsight' })
    } else {
      this.$router.push({path: '/query/insight'})
    }
  }
  openManualUrl () {
    if (this.platform === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'dataV' })
    } else {
      window.open(this.$t('manualUrl'))
    }
  }
  get disabledAcceTips () {
    if ((this.isAcce || this.isAcceing) && this.infoData.unhandled_query_count !== '0') {
      return this.$t('isAcce')
    } else if (!this.datasourceActions.includes('accelerationActions')) {
      return this.$t('noPermissionTips')
    } else {
      return ''
    }
  }
  async turnOnRecommendation () {
    try {
      await this.callGlobalDetailDialog({
        msg: this.$t('turnOnTips'),
        title: this.$t('turnOn') + this.$t('enableSemiAutomatic'),
        dialogType: 'warning',
        isBeta: false,
        wid: '400px',
        isCenterBtn: true,
        showDetailBtn: false,
        dangerouslyUseHTMLString: true,
        needConcelReject: true,
        submitText: this.$t('confirmOpen')
      })
      await this.updateProjectGeneralInfo({
        alias: this.currentSelectedProject,
        project: this.currentSelectedProject,
        description: this.$store.state.project.projectConfig && this.$store.state.project.projectConfig.description,
        semi_automatic_mode: true
      })
      await this.fetchProjectSettings({projectName: this.currentSelectedProject})
    } catch (e) {
      handleError(e)
    }
    this.init()
  }
  showGenerateModelDialog () {
    this.showUploadSqlDialog({
      isGenerateModel: true,
      title: this.$t('importQueries')
    })
  }
  get isOpenSemiAutomatic () {
    return this.$store.state.project.isSemiAutomatic
  }
  async toAcce () {
    if (this.isAcceing || this.isAcce) return
    if (this.infoData.unhandled_query_count === '0') {
      this.$message({
        type: 'success',
        message: this.$t('noPendingRecommendations')
      })
      return
    }
    const res = await this.getModelByModelName({status: 'ONLINE,WARNING', project: this.currentSelectedProject})
    const data = await handleSuccessAsync(res)
    if (data.total_size === 0) {
      await this.callGlobalDetailDialog({
        msg: this.$t('noModelsTips'),
        title: this.$t('kylinLang.common.tip'),
        dialogType: 'warning',
        isBeta: false,
        showDetailBtn: false,
        dangerouslyUseHTMLString: true,
        needResolveCancel: true,
        wid: '600px',
        submitText: this.$t('addModel'),
        cancelText: this.$t('viewAllModels')
      }).then(e => {
        if (!e) {
          this.gotoModelList()
        } else {
          this.gotoModelList()
          this.$nextTick(() => {
            this.callAddModelDialog()
          })
        }
      })
      return
    }
    try {
      this.isAcceing = true
      const requestProject = JSON.parse(JSON.stringify(this.currentSelectedProject))
      const res = await this.accelerateModel({project: this.currentSelectedProject})
      const data = await handleSuccessAsync(res)
      // 阻止切换项目后上一个项目的提示信息显示在当前项目下
      if (requestProject !== this.currentSelectedProject) return
      if (data) {
        this.$message({
          dangerouslyUseHTMLString: true,
          type: 'success',
          customClass: 'acce-success',
          duration: 10000,
          showClose: true,
          message: (
            <div>
              <span>{this.$t('acceSuccess', {acceNum: data})}</span>
              <a href="javascript:void(0)" onClick={() => this.$router.push('/studio/model')}>{this.$t('viewDetail2')}</a>
            </div>
          )
        })
      } else {
        this.$message({
          type: 'success',
          dangerouslyUseHTMLString: true,
          duration: 10000,
          showClose: true,
          message: this.$t('acceSuccessZero')
        })
      }
      this.init()
      this.isAcceing = false
    } catch (e) {
      handleError(e)
      this.isAcceing = false
    }
  }
  async init () {
    try {
      const res = await this.loadStatistics({project: this.currentSelectedProject})
      const data = await handleSuccessAsync(res)
      for (let k in data) {
        if (data[k] !== -1) {
          if (k === 'last_week_query_count' || k === 'unhandled_query_count') {
            this.infoData[k] = Vue.filter('readableNumber')(data[k])
          } else {
            this.infoData[k] = data[k]
          }
        }
      }
      if (this.$store.state.project.isSemiAutomatic) {
        const resStatus = await this.getAccelerationStatus({project: this.currentSelectedProject})
        const statusData = await handleSuccessAsync(resStatus)
        this.isAcce = statusData
      }
    } catch (e) {
      handleError(e)
    }
  }
  handleResizeWindow () {
    const homepage = document.getElementsByClassName('combination-block')[0]
    if (homepage) {
      const homepageWidth = homepage.clientWidth
      homepage.style.fontSize = homepageWidth + 'px'
    }
  }
  mounted () {
    this.$nextTick(() => {
      window.addEventListener('resize', this.handleResizeWindow)
      this.handleResizeWindow()
    })
  }
  @Watch('briefMenuGet')
  onBriefMenuGetChange () {
    this.handleResizeWindow()
  }
  created () {
    if (this.currentSelectedProject) {
      this.init()
    }
  }
}
</script>

<style lang="less">
  @import "../../assets/styles/variables.less";
  #homepage {
    background: @base-background-color-1;
    position: absolute;
    width: 100%;
    height: 100%;
    overflow: auto;
    min-width: 1250px;
    .homepage-content {
      margin: 20px;
      box-sizing: border-box;
      overflow-y: auto;
      .combination-block {
        transform: translate(-50%, -50%);
        left: 50%;
        top: 50%;
        position: absolute;
        width: 100%;
        .left-block,
        .right-block {
          width: 25%;
          float: left;
        }
        .middle-block {
           width: 50%;
           float: left;
        }
      }
      .card {
        padding: 0.0128em;
        background-color: @fff;
        box-shadow: 0px 1px 4px rgba(63, 89, 128, 0.16);
        border-radius: 12px;
        min-width: 290px;
        width: 0.232em;
        height: 0.1152em;
        position: relative;
        box-sizing: border-box;
        margin-top: 0.028em;
        z-index: 1;
        &:first-child {
          margin-top: 0;
        }
        .card-img {
          font-size: 0.225em;
          width: 0.19915em;
          height: 0.19915em;
          background: linear-gradient(180deg, #EBF6FF 0%, rgba(240, 248, 255, 0) 100%);
          border-radius: 100%;
          position: relative;
          // img {
          //   width: 0.1549em;
          //   height: 0.1549em;
          //   float: left;
          // }
          .mutiple-color-icon {
            width: 0.1em;
            height: 0.1em;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
          }
        }
        .card-content {
          box-sizing: border-box;
          width: 0.15em;
          .ksd-title-label {
            font-size: 0.015em;
            height: 1.2em;
            line-height: 1.2em;
          }
          .content {
            color: @text-normal-color;
            font-size: 0.011em;
            margin-top: 0.8em;
            line-height: 1.4em;
            b {
              // font-weight: bold;
              font-size: 1.1em;
              color: @text-title-color;
            }
            p {
              display: inline-block;
            }
            .tips {
              color: @color-text-placeholder;
            }
          }
          .card-link {
            position: absolute;
            bottom: 1.1em;
            right: 1em;
            font-size: 0.01em;
            color: #0875DA;
            padding: .4em .7em;
          }
          .new-tag {
            position: absolute;
            top: 1em;
            right: 1em;
            font-size: 0.012em;
            height: 1.5em;
            padding: 0 0.5em;
            display: flex;
            align-items: center;
          }
        }
      }
      .circle-block {
        width: 100%;
        box-sizing: border-box;
        position: relative;
        margin-top: 0.024em;
        .line-img {
          box-sizing: border-box;
          width: calc(~'50% - 200px');
          img {
            width: 100%;
          }
        }
        .circle-img {
          width: 100%;
          height: 100%;
          box-sizing: border-box;
          position: absolute;
          .circle-content {
            width: 52%;
            height: 0.2em;
            text-align: center;
            position: absolute;
            top: 0.16em;
            left: 50%;
            transform: translate(-50%, -50%);
            img {
              width: 0.15em;
              position: absolute;
              left: 50%;
              transform: translate(-50%);
            }
            .tip_box .icon {
              font-size: 1em;
            }
            .advance-setting {
              font-size: 0.01em;
              margin-top: 6em;
              padding-top: 15px;
              color: @ke-color-primary;
              font-weight: 400;
              &.is-disabled {
                pointer-events: none;
                color: @text-disabled-color;
              }
            }
            .circle-desc {
              width: 14em;
              height: 3em;
              line-height: 1.4em;
              margin: 0 auto;
              margin-top: 3.8em;
              font-size: 0.01em;
              display: flex;
              align-items: flex-end;
              justify-content: center;
              color: @text-placeholder-color;
              span {
                font-weight: bold;
                font-size: 1.1em;
                color: @text-placeholder-color;
              }
              &.en {
                width: 17em;
              }
              &.none-opt {
                color: @text-normal-color;
                b {
                  font-size: 1.6em;
                }
              }
            }
            .circle-btn {
              margin: 0 auto;
              margin-top: 1.1em;
              color: #fff;
              width: 14.334em;
              height: 2.534em;
              line-height: 2.534em;
              font-size: 0.012em;
              background: @ke-color-primary;
              box-shadow: 0px 0px 4px 0px rgba(163, 163, 163, 50%);
              border-radius: 0.4em;
              cursor: pointer;
              position: relative;
              &:hover {
                background: @ke-color-primary-hover;
              }
              &.disabled {
                background: linear-gradient(141deg, #3EA8EF 0%, #0474C1 100%);
                box-shadow: 0px 0px 4px 0px rgba(163, 163, 163, 0.5);
                opacity: 0.49;
                cursor: default;
              }
              &::after {
                content: '';
                width: 80%;
                height: 10%;
                box-shadow: 0px 1px 32px 6px @ke-color-primary;
                opacity: 0.3;
                position: absolute;
                bottom: 0px;
                left: 10%;
              }
            }
            .recommend-block {
              position: absolute;
              left: 50%;
              transform: translate(-50%);
              width: 0.16em;
              margin-top: 0.01em;
              display: flex;
              justify-content: space-between;
              .recommend-content {
                margin-top: 0.7em;
                height: 44px;
                font-size: 0.01em;
                cursor: pointer;
                min-width: 3.5em;
                // border-left: 1px solid #EAEAEA;
                .popover-btn {
                  color: @text-disabled-color;
                }
                &.no-pointer {
                  cursor: default;
                }
                .re-content {
                  position: relative;
                  top: -0.2em;
                }
                .re-count {
                  font-weight: bold;
                  font-size: 1.1em;
                  color: @text-title-color;
                  margin-top: 0.25em;
                }
                &.no-border {
                  border-left: 0;
                }
              }
            }
          }
        }
      }
    }
  }
</style>
