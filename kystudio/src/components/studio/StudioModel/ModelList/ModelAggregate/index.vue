<template>
  <div class="model-aggregate ksd-mb-15" v-if="model">
    <div class="aggregate-actions" v-if="isShowAggregateAction">
      <!-- <el-button type="primary" icon="el-icon-ksd-table_refresh">
        {{$t('kylinLang.common.refresh')}}
      </el-button>
      <el-button type="primary" icon="el-icon-ksd-table_delete">
        {{$t('kylinLang.common.delete')}}
      </el-button> -->
      <el-button type="primary" size="small" v-guide.addAggBtn icon="el-icon-ksd-table_edit" @click="handleAggregateGroup" v-if="isShowEditAgg">
        {{$t('aggregateGroup')}}
      </el-button><el-button
        type="primary" size="small" :loading="buildIndexLoading" @click="buildAggIndex" v-if="isShowBulidIndex">
        {{$t('buildIndex')}}
      </el-button>
    </div>
    <div class="aggregate-view">
      <el-row :gutter="15">
        <el-col :span="15">
          <el-card class="agg-detail-card agg_index">
            <div slot="header" class="clearfix">
              <div class="left font-medium">{{$t('aggregateIndexTree')}}</div>
              <div class="right">
                <span>{{$t('aggregateAmount')}}</span>{{cuboidCount}}
                <!-- <el-input v-model.trim="cuboidCount" :readonly="true" size="small"></el-input> -->
              </div>
            </div>
            <div class="agg-counter">
              <div>
                <img src="./empty_note.jpg" />
                <span>{{$t('emptyAggregate')}}</span>
                <span>{{emptyCuboidCount}}</span>
              </div>
              <div>
                <img src="./broken_note.jpg" />
                <span>{{$t('brokenAggregate')}}</span>
                <span>{{brokenCuboidCount}}</span>
              </div>
            </div>
            <kap-empty-data v-if="cuboidCount === 0" size="small"></kap-empty-data>
            <PartitionChart
              :data="cuboids"
              :search-id="searchCuboidId"
              :background-maps="backgroundMaps"
              @on-click-node="handleClickNode"/>
          </el-card>
        </el-col>
        <el-col :span="9">
          <el-card class="agg-detail-card agg-detail">
            <div slot="header" class="clearfix">
              <div class="left font-medium fix">{{$t('aggregateDetail')}}</div>
              <div class="right fix">
                <el-input class="search-input" v-model.trim="searchCuboidId" size="mini" :placeholder="$t('searchAggregateID')" prefix-icon="el-icon-search"></el-input>
              </div>
            </div>
            <div class="detail-content">
              <template v-if="cuboidDetail.id !== ''">
                <el-table class="cuboid-info" nested :data="cuboidInfo" border stripe :show-header="false">
                  <el-table-column prop="key" class-name="font-medium" width="166px">
                    <template slot-scope="scope">
                      <div v-if="scope.row.key === 'dataRange'">
                        <div>{{$t(scope.row.key)}}</div>
                        <div class="slot" v-if="scope.row.value">slot</div>
                      </div>
                      <div v-else-if="scope.row.key === 'queryCount'" class="ky-hover-icon">
                        {{$t(scope.row.key)}}
                        <el-tooltip placement="top" :content="$t('usageTip')">
                          <i class='el-icon-ksd-what'></i>
                        </el-tooltip>
                      </div>
                      <div v-else>{{$t(scope.row.key)}}</div>
                    </template>
                  </el-table-column>
                  <el-table-column prop="value">
                    <template slot-scope="scope">
                      <template v-if="scope.row.value !== undefined">
                        <div v-if="scope.row.key === 'dataRange'">
                          <div>{{scope.row.value.startDate}} {{scope.row.value.to}}</div>
                          <div>{{scope.row.value.endDate}}</div>
                        </div>
                        <div v-else-if="scope.row.key === 'storage'">{{scope.row.value | dataSize}}</div>
                        <div v-else>{{scope.row.value}}</div>
                      </template>
                      <div v-else>{{$t('kylinLang.common.null')}}</div>
                    </template>
                  </el-table-column>
                </el-table>
                <el-table class="cuboid-content"  size="medium" nested :data="cuboidContent" border>
                  <el-table-column type="index" :label="$t('order')" width="64">
                  </el-table-column>
                  <el-table-column prop="content" :label="$t('content')">
                    <template slot-scope="scope">
                      <div class="align-left">{{scope.row.content}}</div>
                    </template>
                  </el-table-column>
                </el-table>
              </template>
            </div>
          </el-card>
        </el-col>
      </el-row>
    </div>

    <AggregateModal />
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import FlowerChart from '../../../../common/FlowerChart'
import PartitionChart from '../../../../common/PartitionChart'
import { handleSuccessAsync } from '../../../../../util'
import { handleError, transToGmtTime, transToServerGmtTime } from '../../../../../util/business'
import { speedProjectTypes } from '../../../../../config'
import { BuildIndexStatus } from '../../../../../config/model'
import AggregateModal from './AggregateModal/index.vue'
import { formatFlowerJson, getCuboidCounts, getStatusCuboidCounts, backgroundMaps } from './handler'

@Component({
  props: {
    model: {
      type: Object
    },
    projectName: {
      type: String
    },
    isShowAggregateAction: {
      type: Boolean,
      default: true
    },
    isShowEditAgg: {
      type: Boolean,
      default: true
    },
    isShowBulidIndex: {
      type: Boolean,
      default: true
    }
  },
  computed: {
    ...mapGetters([
      'currentProjectData'
    ])
  },
  methods: {
    ...mapActions('AggregateModal', {
      callAggregateModal: 'CALL_MODAL'
    }),
    ...mapActions({
      fetchModelAggregates: 'FETCH_AGGREGATES',
      fetchCuboids: 'FETCH_CUBOIDS',
      fetchCuboid: 'FETCH_CUBOID',
      buildIndex: 'BUILD_INDEX'
    })
  },
  components: {
    FlowerChart,
    PartitionChart,
    AggregateModal
  },
  locales
})
export default class ModelAggregate extends Vue {
  cuboidCount = 0
  emptyCuboidCount = 0
  brokenCuboidCount = 0
  cuboids = []
  cuboidData = {}
  searchCuboidId = ''
  backgroundMaps = backgroundMaps
  buildIndexLoading = false
  handleBuildIndexTip (data) {
    let tipMsg = ''
    if (data.type === BuildIndexStatus.NORM_BUILD) {
      tipMsg = this.$t('kylinLang.model.buildIndexSuccess')
      this.$message({message: tipMsg, type: 'success'})
      return
    }
    if (data.type === BuildIndexStatus.NO_LAYOUT) {
      tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.aggregateGroupIndex')})
    } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
      tipMsg += this.$t('kylinLang.model.buildIndexFail1', {modelName: this.model.name})
    }
    this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
  }
  async buildAggIndex () {
    try {
      this.buildIndexLoading = true
      let res = await this.buildIndex({
        project: this.projectName,
        model_id: this.model.uuid
      })
      let data = await handleSuccessAsync(res)
      this.handleBuildIndexTip(data)
    } catch (e) {
      handleError(e)
    } finally {
      this.buildIndexLoading = false
    }
  }
  get cuboidInfo () {
    return Object.entries(this.cuboidDetail)
      .filter(([key]) => !['dimensions', 'measures'].includes(key))
      .map(([key, value]) => ({ key, value }))
  }
  get cuboidContent () {
    return [
      ...this.cuboidDetail.dimensions.map(dimension => ({ content: dimension })),
      ...this.cuboidDetail.measures.map(measure => ({ content: measure }))
    ]
  }
  get isSpeedProject () {
    return speedProjectTypes.includes(this.currentProjectData.maintain_model_type)
  }
  get cuboidDetail () {
    const id = this.cuboidData.id
    const dimensions = this.cuboidData.dimensions_res || []
    const measures = this.cuboidData.measures_res || []
    const startDate = transToServerGmtTime(this.cuboidData.start_time)
    const endDate = transToServerGmtTime(this.cuboidData.end_time)
    const storage = this.cuboidData.storage_size
    const queryCount = this.cuboidData.query_hit_count || 0
    const modifiedTime = transToGmtTime(this.cuboidData.last_modify_time)
    const dataRange = (this.cuboidData.start_time && this.cuboidData.end_time) ? { startDate, to: this.$t('to'), endDate } : undefined
    return { modifiedTime, storage, dimensions, measures, id, dataRange, queryCount }
  }
  async handleClickNode (node) {
    const res = await this.fetchCuboid({
      projectName: this.projectName,
      modelId: this.model.uuid,
      cuboidId: node.cuboid.id
    })
    this.cuboidData = await handleSuccessAsync(res)
  }
  async freshCuboids () {
    const res = await this.fetchCuboids({
      projectName: this.projectName,
      modelId: this.model.uuid
    })
    const data = await handleSuccessAsync(res)
    this.cuboids = formatFlowerJson(data)
    this.cuboidCount = getCuboidCounts(data)
    this.emptyCuboidCount = getStatusCuboidCounts(data, 'EMPTY')
    this.brokenCuboidCount = getStatusCuboidCounts(data, 'BROKEN')
  }
  async mounted () {
    await this.freshCuboids()
  }
  async handleAggregateGroup () {
    const { projectName, model } = this
    const isSubmit = await this.callAggregateModal({ editType: 'edit', model, projectName })
    isSubmit && await this.freshCuboids()
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.model-aggregate {
  .aggregate-actions {
    margin-bottom: 10px;
  }
  .agg-amount-block {
    position: absolute;
    right: 0;
    .el-input {
      width: 120px;
    }
  }
  .agg-counter {
    position: absolute;
    top: 15px;
    right: 20px;
    white-space: nowrap;
    font-size: 12px;
    * {
      vertical-align: middle;
    }
    img {
      width: 18px;
      height: 18px;
    }
    div:not(:last-child) {
      margin-bottom: 5px;
    }
  }
  .cuboid-info {
    margin-bottom: 10px;
    .is-right {
      border-right: none;
    }
    .slot {
      opacity: 0;
    }
  }
  .align-left {
    text-align: left;
  }
  .agg-detail-card {
    height: 496px;
    &.agg_index .el-card__body {
      overflow: hidden;
    }
    &.agg-detail {
      .el-card__header {
        padding-top:6px;
        padding-bottom:6px;
      }
    }
    .el-card__body {
      overflow: auto;
      height: 460px;
      width: 100%;
      position: relative;
      box-sizing: border-box;
      .detail-content {
        .el-row {
          margin-bottom: 10px;
          .dim-item {
            margin-bottom: 5px;
          }
        }
      }
    }
    .left {
      display: block;
      float: left;
      &.fix {
        width: 130px;
      }
    }
    .right {
      display: block;
      float: right;
      white-space: nowrap;
      font-size: 14px;
      &.fix {
        width: calc(~'100% - 130px');
        max-width: 200px;
        .el-input.search-input {
          width: 100%;
        }
      }
      .el-input {
        width: 100px;
      }
    }
    .label {
      text-align: right;
    }
  }
}
</style>
