<template>
  <div id="homepage">
    <div class="homepage-content">
      <div class="ksd-title-label ksd-fs-24">{{$t('aiAugmented')}}</div>
      <div class="ksd-mt-10">{{$t('aiAugmentedDesc')}}</div>
      <div class="ksd-mb-10 combination-block clearfix">
        <div class="left-block">
          <div class="card ksd-fright clearfix">
            <div class="ksd-fleft card-img">
              <img src="../../assets/img/data_source@2x.png" alt="">
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('datasource')}}</div>
              <div class="content" v-html="$t('datasourceDesc', {database_size: infoData.database_size, table_size: infoData.table_size})"></div>
              <div class="card-link ky-a-like" @click="gotoDatasource">{{$t('manageDatasource')}}</div>
            </div>
          </div>
          <div class="card ksd-fright clearfix">
            <div class="ksd-fleft card-img">
              <img src="../../assets/img/history@2x.png" alt="">
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('history')}}</div>
              <div class="content" v-html="$t('historyDesc', {last_week_query_count: infoData.last_week_query_count})"></div>
              <div class="card-link ky-a-like" @click="gotoHistory">{{$t('viewDetail')}}</div>
            </div>
          </div>
          <div class="card ksd-fright clearfix">
            <div class="ksd-fleft card-img">
              <img src="../../assets/img/data_profile@2x.png" alt="">
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('dataProfile')}}</div>
              <div class="content">{{$t('dataProfileDesc')}}</div>
              <div class="card-link ky-a-like" @click="gotoDatasource">{{$t('viewDetail')}}</div>
            </div>
          </div>
        </div>
        <div class="middle-block">
          <div class="circle-block clearfix">
            <!-- <div class="ksd-fleft line-img">
              <img src="../../assets/img/arrow_left@2x.png" height="460px" alt="">
            </div> -->
            <div class="ksd-fleft circle-img" @mouseenter="isShowGif = true" @mouseleave="isShowGif = false">
              <img class="ksd-fleft" v-if="isAcceing" src="../../assets/img/animation_loading@2.gif" width="100%" alt=""/>
              <template v-else>
                <img class="ksd-fleft" v-if="isShowGif" src="../../assets/img/circle_hover@2.gif" width="100%" alt=""/>
                <img class="ksd-fleft" v-else src="../../assets/img/circle_arrow.png" width="100%" alt=""/>
              </template>
              <div class="circle-content">
                <img :class="{'en-img': isOpenSemiAutomatic}" src="../../assets/img/AItitle_EN@2x.png" v-if="$lang === 'en'" alt="">
                <img src="../../assets/img/AItitle_CN@2x.png" v-else alt="">
                <div v-if="!isOpenSemiAutomatic">
                  <div class="circle-desc" :class="{'en': $lang === 'en'}">{{$t('openRecommendationDesc')}}</div>
                  <common-tip :content="$t('noPermissionTips')" v-if="!datasourceActions.includes('acceRuleSettingActions')" placement="top">
                    <div class="circle-btn disabled">
                      <i class="ksd-fs-20 el-icon-ksd-project_status"></i>
                      {{$t('turnOnRecommendation')}}
                    </div>
                  </common-tip>
                  <div class="circle-btn" v-else @click="turnOnRecommendation">
                    <i class="ksd-fs-20 el-icon-ksd-project_status"></i>
                    {{$t('turnOnRecommendation')}}
                  </div>
                </div>
                <div v-else>
                  <div class="circle-desc" :class="{'en': $lang === 'en'}" v-if="+infoData.unhandled_query_count>0" v-html="$t('optimizeDesc', {unhandled_query_count: infoData.unhandled_query_count})"></div>
                  <div class="circle-desc none-opt" :class="{'en': $lang === 'en'}" v-else>{{$t('noneOptimizeDesc')}}</div>
                  <common-tip :content="disabledAcceTips" v-if="(isAcce || isAcceing) && +infoData.unhandled_query_count !== 0 || !datasourceActions.includes('accelerationActions')" placement="top">
                    <div class="circle-btn disabled">
                      <i class="ksd-fs-20" :class="{'el-icon-ksd-project_status': !isAcceing, 'el-icon-loading': isAcceing}"></i>
                      <span v-if="isAcceing">{{$t('optimizing')}}</span>
                      <span v-else>{{$t('optimize')}}</span>
                    </div>
                  </common-tip>
                  <div class="circle-btn" v-else :class="{'disabled': +infoData.unhandled_query_count == 0}" @click="toAcce">
                    <i class="ksd-fs-20" :class="{'el-icon-ksd-project_status': !isAcceing, 'el-icon-loading': isAcceing}"></i>
                    <span v-if="isAcceing">{{$t('optimizing')}}</span>
                    <span v-else>{{$t('optimize')}}</span>
                  </div>
                </div>
                <div class="recommend-block clearfix" :class="{'recmmend-zh': $lang !== 'en'}">
                  <el-popover
                    ref="patternPopover"
                    placement="top"
                    :title="$t('pattern')"
                    width="460"
                    popper-class="recommend-popover"
                    v-if="isOpenSemiAutomatic"
                    trigger="click">
                      <div v-html="$t('patternDsec', {
                        rec_pattern_count: infoData.rec_pattern_count,
                        additional_rec_pattern_count: infoData.additional_rec_pattern_count,
                        removal_rec_pattern_count: infoData.removal_rec_pattern_count,
                        max_rec_show_size: infoData.max_rec_show_size
                      })"></div>
                      <div class="recommend-content no-border clearfix" slot="reference">
                        <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-pattern ksd-fleft"></i></div>
                        <div class="ksd-fleft ksd-ml-5 re-content">
                          <div class="popover-btn">{{$t('pattern')}}</div>
                          <div class="re-count">{{infoData.rec_pattern_count}}</div>
                        </div>
                      </div>
                  </el-popover>
                  <div class="recommend-content no-pointer no-border clearfix" v-else>
                    <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-pattern ksd-fleft"></i></div>
                    <div class="ksd-fleft ksd-ml-5 re-content">
                      <div class="popover-btn">{{$t('pattern')}}</div>
                      <div class="re-count">{{infoData.rec_pattern_count}}</div>
                    </div>
                  </div>
                  <el-popover
                    ref="rulePopover"
                    placement="top"
                    :title="$t('rule')"
                    width="460"
                    popper-class="recommend-popover"
                    v-if="isOpenSemiAutomatic"
                    trigger="click">
                    <div v-html="$t('ruleDsec')"></div>
                    <div class="ksd-mt-5" style="text-align: right; margin: 0" v-if="isOpenSemiAutomatic&&datasourceActions.includes('acceRuleSettingActions')">
                      <el-button type="primary" text size="mini" @click="gotoSettingRules">{{$t('modifySettings')}}</el-button>
                    </div>
                    <div class="recommend-content" slot="reference">
                      <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-rules ksd-fleft"></i></div>
                      <div class="ksd-fleft ksd-ml-5 re-content">
                        <div class="popover-btn">{{$t('rule')}}</div>
                        <div class="re-count">{{infoData.effective_rule_size}}</div>
                      </div>
                    </div>
                  </el-popover>
                  <div class="recommend-content no-pointer" v-else>
                    <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-rules ksd-fleft"></i></div>
                    <div class="ksd-fleft ksd-ml-5 re-content">
                      <div class="popover-btn">{{$t('rule')}}</div>
                      <div class="re-count">{{infoData.effective_rule_size}}</div>
                    </div>
                  </div>
                  <el-popover
                    ref="recommendationPopover"
                    placement="top"
                    :title="$t('recommendation')"
                    width="260"
                    popper-class="recommend-popover"
                    v-if="isOpenSemiAutomatic"
                    trigger="click">
                    <div v-html="$t('recommendationDsec', {
                      approved_rec_count: infoData.approved_rec_count,
                      approved_additional_rec_count: infoData.approved_additional_rec_count,
                      approved_removal_rec_count: infoData.approved_removal_rec_count
                    })"></div>
                    <div class="recommend-content" slot="reference">
                    <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-status ksd-fleft"></i></div>
                    <div class="ksd-fleft ksd-ml-5 re-content">
                      <div class="popover-btn">{{$t('recommendation')}}</div>
                      <div class="re-count">{{infoData.approved_rec_count}}</div>
                    </div>
                  </div>
                  </el-popover>
                  <div class="recommend-content no-pointer" v-else>
                    <div class="ksd-fleft ksd-ml-10"><i class="el-icon-ksd-status ksd-fleft"></i></div>
                    <div class="ksd-fleft ksd-ml-5 re-content">
                      <div class="popover-btn">{{$t('recommendation')}}</div>
                      <div class="re-count">{{infoData.approved_rec_count}}</div>
                    </div>
                  </div>
                </div>
                <div class="ky-a-like advance-setting" v-if="isOpenSemiAutomatic&&datasourceActions.includes('modelActions')&&this.currentSelectedProject">
                  <span @click="showGenerateModelDialog">
                    <i class="el-icon-ksd-load"></i>
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
              <img src="../../assets/img/model@2x.png" width="44px" alt="">
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('model')}}</div>
              <div class="content">
                <div class="ksd-inline" v-if="!$store.state.project.isSemiAutomatic" v-html="$t('modelDesc', {model_size: infoData.model_size})"></div>
                <div class="ksd-inline" v-else v-html="$t('modelDesc2', {model_size: infoData.model_size, acceptable_rec_size: infoData.acceptable_rec_size})"></div>
              </div>
              <div class="card-link ky-a-like" @click="gotoModelList">{{$t('viewDetail')}}</div>
              <el-tag class="new-tag" size="mini" type="success" v-if="infoData.is_refreshed">New</el-tag>
            </div>
          </div>
          <div class="card ksd-mr-20 clearfix">
            <div class="ksd-fleft card-img">
              <img src="../../assets/img/insight@2x.png" width="44px" alt="">
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('quickInsight')}}</div>
              <div class="content">{{$t('quickInsightDesc')}}</div>
              <div class="card-link ky-a-like" @click="gotoInsight">{{$t('queryNow')}}</div>
            </div>
          </div>
          <div class="card ksd-mr-20 clearfix">
            <div class="ksd-fleft card-img">
              <img src="../../assets/img/connect_to_BI@2x.png" width="44px" alt="">
            </div>
            <div class="ksd-fleft card-content ksd-pl-15">
              <div class="ksd-title-label">{{$t('connectToBI')}}</div>
              <div v-if="platform === 'iframe'" class="content">{{$t('connectToBIDescForKC')}}</div>
              <div class="content" v-else>{{$t('connectToBIDesc')}}</div>
              <p class="card-link ky-a-like" @click="openManualUrl" :href="$t('manualUrl')" target="_blank">
                <span v-if="platform === 'iframe'">{{$t('connectNow')}}</span>
                <span v-else>{{$t('learnMore')}}</span>
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
    <!-- 模型添加 -->
    <UploadSqlModel v-on:reloadModelList="init"/>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync, handleError } from '../../util/index'
import { postCloudUrlMessage } from '../../util/business'
import UploadSqlModel from '../common/UploadSql/UploadSql.vue'
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
    UploadSqlModel
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
    })
  },
  locales
})
export default class Homepage extends Vue {
  infoData = {
    acceptable_rec_size: '--',
    additional_rec_pattern_count: '--',
    approved_additional_rec_count: '--',
    approved_rec_count: '--',
    approved_removal_rec_count: '--',
    database_size: '--',
    effective_rule_size: '--',
    is_refreshed: false,
    last_week_query_count: '--',
    model_size: '--',
    rec_pattern_count: '--',
    refreshed: false,
    removal_rec_pattern_count: '--',
    table_size: '--',
    unhandled_query_count: '0',
    max_rec_show_size: '--'
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
  gotoDatasource () {
    if (this.platform === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'datasource-create' })
    } else {
      this.$router.push({path: '/studio/source'})
    }
  }
  gotoHistory () {
    if (this.platform === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'kapHistory' })
    } else {
      this.$router.push({path: '/query/queryhistory'})
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
        isBeta: true,
        showDetailBtn: false,
        dangerouslyUseHTMLString: true,
        needConcelReject: true,
        submitText: this.$t('confirmOpen')
      })
      await this.updateProjectGeneralInfo({
        alias: this.currentSelectedProject,
        project: this.currentSelectedProject,
        description: this.$store.state.project.projectConfig && this.$store.state.project.projectConfig.description,
        maintain_model_type: this.$store.state.project.projectConfig && this.$store.state.project.projectConfig.maintain_model_type,
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
      isGenerateModel: true
    })
  }
  get isOpenSemiAutomatic () {
    return this.$store.state.project.isSemiAutomatic
  }
  async toAcce () {
    if (this.isAcceing || this.isAcce || this.infoData.unhandled_query_count === '0') return
    try {
      this.isAcceing = true
      const res = await this.accelerateModel({project: this.currentSelectedProject})
      const data = await handleSuccessAsync(res)
      if (data) {
        this.$message({
          dangerouslyUseHTMLString: true,
          type: 'success',
          customClass: 'acce-success',
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
        padding: 0.016em;
        background-color: @fff;
        box-shadow: 0px 0px 10px 0px #D8D8D8;
        min-width: 200px;
        width: 0.23em;
        height: 0.1em;
        position: relative;
        box-sizing: border-box;
        margin-top: 0.028em;
        z-index: 1;
        &:first-child {
          margin-top: 0;
        }
        .card-img {
          font-size: 0.225em;
          width: 0.1549em;
          height: 0.1549em;
          img {
            width: 0.1549em;
            height: 0.1549em;
            float: left;
          }
        }
        .card-content {
          box-sizing: border-box;
          width: 0.16em;
          .ksd-title-label {
            font-size: 0.015em;
            height: 1.2em;
            line-height: 1.2em;
          }
          .content {
            color: @text-normal-color;
            font-size: 0.01em;
            margin-top: 0.8em;
            line-height: 1.4em;
            span {
              font-weight: 500;
              font-size: 1.1em;
              color: @text-title-color;
            }
          }
          .card-link {
            position: absolute;
            bottom: 1.1em;
            font-size: 0.01em;
          }
          .new-tag {
            position: absolute;
            top: 1em;
            right: 1em;
            font-size: 0.012em;
            height: 1.5em;
            line-height: 1.5em;
            padding: 0 0.5em;
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
              margin-top: 5em;
              padding-top: 15px;
            }
            .circle-desc {
              width: 13em;
              height: 3em;
              line-height: 1.4em;
              margin: 0 auto;
              margin-top: 3.8em;
              font-size: 0.01em;
              display: flex;
              align-items: flex-end;
              justify-content: center;
              span {
                font-weight: 500;
                font-size: 1.1em;
                color: @text-title-color;
              }
              &.en {
                width: 17em;
              }
            }
            .circle-btn {
              margin: 0 auto;
              margin-top: 1.1em;
              color: @fff;
              width: 19em;
              height: 3.2em;
              line-height: 3.2em;
              font-size: 0.012em;
              background: linear-gradient(141deg, #3EA8EF 0%, #0474C1 100%);
              box-shadow: 0px 0px 4px 0px rgba(163, 163, 163, 0.5);
              border-radius: 2em;
              cursor: pointer;
              &.disabled {
                background: linear-gradient(141deg, #3EA8EF 0%, #0474C1 100%);
                box-shadow: 0px 0px 4px 0px rgba(163, 163, 163, 0.5);
                opacity: 0.49;
                cursor: default;
              }
            }
            .recommend-block {
              position: absolute;
              left: 50%;
              transform: translate(-50%);
              width: 0.25em;
              margin-top: 0.01em;
              display: flex;
              justify-content: center;
              .recommend-content {
                margin: 0.7em;
                height: 44px;
                font-size: 0.01em;
                cursor: pointer;
                border-left: 1px solid #EAEAEA;
                &.no-pointer {
                  cursor: default;
                }
                .re-content {
                  position: relative;
                  top: -0.2em;
                }
                .re-count {
                  font-weight: 500;
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
