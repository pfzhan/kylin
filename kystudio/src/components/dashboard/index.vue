<template>
  <div id="dashboard">
    <el-row :gutter="10" class="ratio-row">
      <el-col :span="8">
        <div class="dash-card">
          <div class="cart-title clearfix">
            <span>{{$t('storageQuota')}}</span>
            <el-button plain size="mini" class="ksd-fright">{{$t('kylinLang.common.setting')}}</el-button>
          </div>
          <el-row :gutter="35" class="quota-row">
            <el-col :span="12">
              <el-popover
                ref="popover"
                placement="left-end"
                trigger="hover"
                popper-class="quota-popover"
                :disabled="!(trashRatio*170<14 || useageRatio*170<14)"
                v-model="popoverVisible">
                <p class="info-block">
                  <span class="info-title">{{$t('useageMana')}}</span>
                  <span class="useage" v-if="quotaInfo.total_storage_size>=0">{{useageRatio*100}}%</span>
                  <span v-else>--</span>
                </p>
                <p class="info-block">
                  <span  class="info-title">{{$t('trash')}}</span>
                  <span class="trash" v-if="quotaInfo.garbage_storage_size>=0">{{trashRatio*100}}%</span>
                  <span v-else>--</span>
                </p>
              </el-popover>
              <div class="quota-chart" v-popover:popover>
                <div class="unUseage-block" :style="{'height': (1-useageRatio-trashRatio)*170+'px'}">
                  <div class="text" v-if="(trashRatio*170<14 || useageRatio*170<14)">{{Math.round((1-useageRatio-trashRatio)*100)}}%</div>
                </div>
                <div class="total-use-block" :style="{'height': usedBlockHeight+'px'}">
                  <div class="useage-block" :style="{'height': (useageRatio*170/usedBlockHeight)*100+'%'}">
                    <div class="text" v-if="useageRatio*170>=14">{{Math.round(useageRatio*100)}}%</div>
                  </div>
                  <div class="trash-block" :style="{'height': (trashRatio*170/usedBlockHeight)*100+'%'}">
                    <div class="text" v-if="trashRatio*170>=14">{{Math.round(trashRatio*100)}}%</div>
                  </div>
                </div>
              </div>
            </el-col>
            <el-col :span="12">
              <div class="quota-info">
                <div class="info-title ksd-mt-10">{{$t('totalStorage')}}</div>
                <div class="total-quota">
                  <span v-if="quotaInfo.storage_quota_size>=0">
                    <span class="ksd-fs-28">{{quotaInfo.storage_quota_size | dataSize('GB', true)}}</span><span class="ksd-fs-18">G</span>
                  </span>
                  <span class="ksd-fs-28" v-else>--</span>
                </div>
                <div class="info-title ksd-mt-16">{{$t('useageMana')}}</div>
                <div class="useage">
                  <span v-if="quotaInfo.total_storage_size>=0">{{quotaInfo.total_storage_size | dataSize}}</span>
                  <span v-else>--</span>
                </div>
                <div class="info-title ksd-mt-16">{{$t('trash')}}</div>
                <div class="trash">
                  <span v-if="quotaInfo.garbage_storage_size>=0">{{quotaInfo.garbage_storage_size | dataSize}}</span>
                  <span v-else>--</span>
                  <el-button type="primary" size="mini" class="ksd-ml-10" @click="clearStorage" v-if="quotaInfo.garbage_storage_size>0">{{$t('clear')}}</el-button>
                </div>
              </div>
            </el-col>
          </el-row>
        </div>
      </el-col>
      <el-col :span="8">
        <div class="dash-card">
          <div class="cart-title clearfix">
            <span>{{$t('acceImpact')}}</span>
            <el-button plain size="mini" class="ksd-fright">{{$t('ruleSetting')}}</el-button>
          </div>
          <svg id="ruleImpact" width="100%" height="168" class="ksd-mt-20"></svg>
        </div>
      </el-col>
    </el-row>
    <hr class="divider"/>
    <div class="clearfix ksd-mb-10">
      <div class="ksd-fright">
        <span>{{$t('kylinLang.common.startTime')}}</span>
        <el-date-picker v-model="startTime" type="date" size="small" :placeholder="$t('kylinLang.common.startTime')"></el-date-picker>
        <span class="ksd-ml-10">{{$t('kylinLang.common.endTime')}}</span>
        <el-date-picker v-model="endTime" type="date" size="small" :placeholder="$t('kylinLang.common.endTime')"></el-date-picker>
      </div>
    </div>
    <el-row :gutter="10" class="count-row">
      <el-col :span="6">
        <div class="dash-card">
          <div class="cart-title">{{$t('queryCount')}}</div>
          <div class="content">
            <span class="num">55,327</span>
          </div>
          <el-button type="primary" plain size="mini">{{$t('viewDetail')}}</el-button>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="dash-card">
          <div class="cart-title">{{$t('avgQueryLatency')}}</div>
          <div class="content">
            <span class="num">0.48</span>
            <span class="unit">sec</span>
          </div>
          <el-button type="primary" plain size="mini">{{$t('viewDetail')}}</el-button>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="dash-card">
          <div class="cart-title">{{$t('jobCount')}}</div>
          <div class="content">
            <span class="num">5</span>
          </div>
          <el-button type="primary" plain size="mini">{{$t('viewDetail')}}</el-button>
        </div>
      </el-col>
      <el-col :span="6">
        <div class="dash-card">
          <div class="cart-title">{{$t('avgBulidTime')}}</div>
          <div class="content">
            <span class="num">11.81</span>
            <span class="unit">sec</span>
          </div>
          <el-button type="primary" plain size="mini">{{$t('viewDetail')}}</el-button>
        </div>
      </el-col>
    </el-row>
    <el-row :gutter="10" class="ksd-mt-10 chart-row">
      <el-col :span="12">
        <div class="dash-card">
          <div class="cart-title">{{$t('queryByCube')}}</div>
          <img src="../../assets/img/chart01.png" width="100%" height="100%"/>
        </div>
      </el-col>
      <el-col :span="12">
        <div class="dash-card">
          <div class="cart-title">{{$t('queryByDay')}}</div>
          <img src="../../assets/img/chart02.png" width="100%" height="100%"/>
        </div>
      </el-col>
    </el-row>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccess } from '../../util/business'
import { handleSuccessAsync, handleError } from '../../util/index'
import { loadLiquidFillGauge, liquidFillGaugeDefaultSettings } from '../../util/liquidFillGauge'
import $ from 'jquery'
@Component({
  methods: {
    ...mapActions({
      getRulesImpact: 'GET_RULES_IMPACT',
      getQuotaInfo: 'GET_QUOTA_INFO',
      clearTrash: 'CLEAR_TRASH'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales: {
    'en': {storageQuota: 'Storage Quota', acceImpact: 'Acceleration Impact', ruleSetting: 'Rules Setting', totalStorage: 'Total Storage', useageMana: 'Useage Manage', trash: 'Trash', clear: 'Clear', queryCount: 'Query Count', viewDetail: 'View Detail', avgQueryLatency: 'Avg. Query Latency', jobCount: 'Job Count', avgBulidTime: 'Avg Build Time Per MB', queryByCube: 'Query Count by Cube', queryByDay: 'Query Count by Day'},
    'zh-cn': {storageQuota: '储存配额', acceImpact: '加速规则影响力', ruleSetting: '规则设置', totalStorage: '总储存容量', useageMana: '占用资源管理', trash: '系统垃圾', clear: '清除垃圾', queryCount: '查询次数', viewDetail: '查看详情', avgQueryLatency: '平均查询延迟', jobCount: '任务次数', avgBulidTime: '每兆平均构建时间', queryByCube: '以Cube查询次数', queryByDay: '以天查询次数'}
  }
})
export default class Dashboard extends Vue {
  startTime = ''
  endTime = ''
  impactRatio = 0
  quotaInfo = {
    storage_quota_size: -1,
    total_storage_size: -1,
    garbage_storage_size: -1
  }
  useageRatio = 0
  trashRatio = 0
  usedBlockHeight = 0
  popoverVisible = false
  drawImpactChart () {
    $(this.$el.querySelector('#ruleImpact')).empty()
    const config1 = liquidFillGaugeDefaultSettings()
    config1.circleColor = '#15BDF1'
    config1.textColor = '#263238'
    config1.waveAnimateTime = 1000
    loadLiquidFillGauge('ruleImpact', this.impactRatio, config1)
  }
  mounted () {
    this.$nextTick(() => {
      window.onresize = () => {
        const targetDom = this.$el.querySelector('#ruleImpact')
        if (targetDom) {
          $(targetDom).empty()
          this.drawImpactChart()
        }
      }
    })
  }
  beforeDestroy () {
    window.onresize = null
  }
  clearStorage () {
    if (this.currentSelectedProject) {
      this.clearTrash({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, () => {
          this.loadQuotaInfo()
        })
      }, (res) => {
        handleError(res)
      })
    }
  }
  created () {
    this.loadRuleImpactRatio()
    this.loadQuotaInfo()
  }
  async loadQuotaInfo () {
    if (this.currentSelectedProject) {
      const res = await this.getQuotaInfo({project: this.currentSelectedProject})
      const resData = await handleSuccessAsync(res)
      this.quotaInfo = resData
      this.useageRatio = (resData.total_storage_size / resData.storage_quota_size).toFixed(2)
      this.trashRatio = (resData.garbage_storage_size / resData.storage_quota_size).toFixed(2)
      setTimeout(() => {
        this.usedBlockHeight = (this.useageRatio + this.trashRatio) * 170
      }, 0)
    }
  }
  loadRuleImpactRatio () {
    if (this.currentSelectedProject) {
      this.getRulesImpact({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.impactRatio = data.toFixed(2) * 100
          this.drawImpactChart()
        })
      }, (res) => {
        handleError(res)
      })
    }
  }
}
</script>

<style lang="less">
  @import "../../assets/styles/variables.less";
  #dashboard {
    margin: 20px;
    .dash-card {
      box-shadow: 0px 0px 4px 0px @line-border-color;
      border: 1px solid @table-stripe-color;
      background-color: @fff;
      padding: 15px;
      text-align: center;
      box-sizing: border-box;
      &:hover {
        box-shadow: 0px 0px 8px 0px @line-border-color;
      }
      .cart-title {
        color: @text-title-color;
        font-size: 14px;
        font-weight: 500;
        line-height: 14px;
        text-align: left;
      }
      .content {
        margin: 30px 0 25px auto;
        .num {
          color: @base-color;
          font-size: 36px;
          line-height: 43px;
        }
      }
      .quota-row {
        margin-top: 20px;
        .quota-chart {
          height: 170px;
          width: 90px;
          border-radius: 4px;
          border: 2px solid #15bdf1;
          box-shadow: 0px 0px 2px 0px #3AA0E5;
          float: right;
          position: relative;
          .text {
            font-size: 12px;
            line-height: 14px;
            color: @fff;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
          }
          .unUseage-block {
            background-color: @fff;
            position: absolute;
            top: 0;
            width: 100%;
            .text {
              color: @text-secondary-color;
            }
          }
          .total-use-block {
            height: 0px;
            -moz-transition: height .5s ease;
            -webkit-transition: height .5s ease;
            -o-transition: height .5s ease;
            transition: height .5s ease;
            position: absolute;
            bottom: 0;
            width: 100%;
            .useage-block {
              background-image: linear-gradient(-202deg, #6EDAAF 0%, #3BB477 100%);
              position: relative;
              width: 100%;
            }
            .trash-block {
              background-image: linear-gradient(-194deg, #FCDE54 0%, #F7BA2A 100%);
              position: relative;
              width: 100%;
            }
          }

        }
        .quota-info {
          float: left;
          text-align: left;
          .info-title {
            color: @text-normal-color;
            font-size: 12px;
            line-height: 14px;
            font-weight: 500;
          }
          .total-quota {
            font-weight: 500px;
            color: @text-title-color;
          }
          .useage {
            font-weight: 500px;
            font-size: 18px;
            color: #3bb477;
          }
          .trash {
            font-weight: 500px;
            font-size: 18px;
            color: @warning-color-1;
          }
        }
      }
    }
    .ratio-row .dash-card {
      height: 253px;
    }
    .count-row .dash-card {
      height: 176px;
    }
    .chart-row .dash-card {
      height: 355px;
    }
    .divider {
      margin: 25px 0;
      border-top: 1px solid @table-stripe-color;
    }
  }
  .quota-popover {
    min-width: 130px !important;
    .info-block {
      display: table-row;
      font-weight: 500;
      font-size: 12px;
      line-height: 18px;
      span {
        display: table-cell;
        &.info-title {
          text-align: right;
          padding-right: 5px;
          color: @text-title-color;
        }
        &.useage {
          color: #3bb477;
        }
        &.trash {
          color: @warning-color-1;
        }
      }
    }
  }
</style>
