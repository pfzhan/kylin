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
      </el-button><el-button v-if="!isAutoProject" size="small" @click="openAggAdvancedModal()">{{$t('aggIndexAdvancedTitle')}}</el-button>
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
              :search-id="filterArgs.content"
              :background-maps="backgroundMaps"
              @on-click-node="handleClickNode"/>
          </el-card>
        </el-col>
        <el-col :span="9">
          <el-card class="agg-detail-card agg-detail">
            <div slot="header" class="clearfix">
              <div class="left font-medium fix">{{$t('aggregateDetail')}}</div>
              <div class="right fix">
                <el-input class="search-input" v-model.trim="filterArgs.content" size="mini" :placeholder="$t('searchAggregateID')" prefix-icon="el-icon-search" @input="searchAggs"></el-input>
              </div>
            </div>
            <div class="detail-content">
              <div class="ksd-mb-10 ksd-fs-12" v-if="dataRange">
                {{$t('dataRange')}}: {{dataRange}}
              </div>
              <el-table :data="indexDatas" nested border size="medium" @sort-change="onSortChange" :default-sort = "{prop: 'last_modify_time', order: 'descending'}">
                <el-table-column prop="id" show-overflow-tooltip :label="$t('id')" width="70"></el-table-column>
                <el-table-column prop="storage_size" sortable="custom" show-overflow-tooltip align="right" :label="$t('storage')">
                  <template slot-scope="scope">
                    {{scope.row.storage_size | dataSize}}
                  </template>
                </el-table-column>
                <el-table-column prop="index_type" show-overflow-tooltip :label="$t('source')" width="150">
                  <template slot-scope="scope">
                    <span v-if="scope.row.index_type === 'MANUAL'">{{$t('aggregateGroupType')}}</span>
                    <span v-if="scope.row.index_type === 'AUTO'">{{$t('recommendation')}}</span>
                  </template>
                </el-table-column>
                <el-table-column prop="query_hit_count" sortable="custom" show-overflow-tooltip align="right" :label="$t('queryCount')"></el-table-column>
                <el-table-column :label="$t('kylinLang.common.action')" width="65">
                  <template slot-scope="scope">
                    <common-tip :content="$t('viewDetail')">
                      <i class="el-icon-ksd-desc" @click="showDetail(scope.row)"></i>
                    </common-tip>
                  </template>
                </el-table-column>
              </el-table>
              <kap-pager class="ksd-center ksd-mtb-10" ref="indexPager" layout="total, prev, pager, next, jumper" :totalSize="totalSize"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
            </div>
          </el-card>
        </el-col>
      </el-row>
    </div>

    <el-dialog class="lincense-result-box"
      :title="$t('aggregateDetail')"
      width="480px"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :visible.sync="aggDetailShow">
      <div class="ksd-mb-10 ksd-fs-12">{{$t('modifiedTime')}}: {{cuboidDetail.modifiedTime}}</div>
      <el-table class="cuboid-content" :data="cuboidContent" border>
        <el-table-column type="index" :label="$t('order')" width="64">
        </el-table-column>
        <el-table-column prop="content" :label="$t('content')">
          <template slot-scope="scope">
            <div class="align-left">{{scope.row.content}}</div>
          </template>
        </el-table-column>
        <el-table-column prop="type" :label="$t('kylinLang.query.type')" width="90">
          <template slot-scope="scope">
            <div class="align-left">{{$t('kylinLang.cube.' + scope.row.type)}}</div>
          </template>
        </el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer">
        <el-button type="default" size="medium" @click="aggDetailShow=false">{{$t('kylinLang.common.close')}}</el-button>
      </div>
    </el-dialog>

    <AggregateModal />
    <AggAdvancedModal v-on:refreshCuboids="refreshCuboidsAfterSubmitSetting" />
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import FlowerChart from '../../../../common/FlowerChart'
import PartitionChart from '../../../../common/PartitionChart'
import { handleSuccessAsync, objectClone } from '../../../../../util'
import { handleError, transToGmtTime, transToServerGmtTime } from '../../../../../util/business'
import { speedProjectTypes } from '../../../../../config'
import { BuildIndexStatus } from '../../../../../config/model'
import AggregateModal from './AggregateModal/index.vue'
import AggAdvancedModal from './AggAdvancedModal/index.vue'
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
      'currentProjectData',
      'isAutoProject'
    ])
  },
  methods: {
    ...mapActions('AggregateModal', {
      callAggregateModal: 'CALL_MODAL'
    }),
    ...mapActions('AggAdvancedModal', {
      callAggAdvancedModal: 'CALL_MODAL'
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
    AggregateModal,
    AggAdvancedModal
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
  indexDatas = []
  dataRange = ''
  totalSize = 0
  filterArgs = {
    pageOffset: 0,
    pageSize: 10,
    content: '',
    sortBy: '',
    reverse: ''
  }
  ST = null
  aggDetailShow = false
  // 打开高级设置
  openAggAdvancedModal () {
    this.callAggAdvancedModal({
      model: objectClone(this.model),
      aggIndexAdvancedDesc: null
    })
  }

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
  showDetail (row) {
    this.cuboidData = row
    this.aggDetailShow = true
  }
  onSortChange ({ column, prop, order }) {
    this.filterArgs.sortBy = prop
    this.filterArgs.reverse = !(order === 'ascending')
    this.loadAggIndices()
  }
  pageCurrentChange (size, count) {
    this.filterArgs.pageOffset = size
    this.filterArgs.pageSize = count
    this.loadAggIndices()
  }
  searchAggs () {
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      this.filterArgs.pageOffset = 0
      this.loadAggIndices()
    }, 500)
  }
  get cuboidInfo () {
    return Object.entries(this.cuboidDetail)
      .filter(([key]) => !['dimensions', 'measures'].includes(key))
      .map(([key, value]) => ({ key, value }))
  }
  get cuboidContent () {
    return [
      ...this.cuboidDetail.dimensions.map(dimension => ({ content: dimension, type: 'dimension' })),
      ...this.cuboidDetail.measures.map(measure => ({ content: measure, type: 'measure' }))
    ]
  }
  get isSpeedProject () {
    return speedProjectTypes.includes(this.currentProjectData.maintain_model_type)
  }
  get cuboidDetail () {
    const dimensions = this.cuboidData.dimensions || []
    const measures = this.cuboidData.measures || []
    const modifiedTime = transToGmtTime(this.cuboidData.last_modify_time)
    return { modifiedTime, dimensions, measures }
  }
  async handleClickNode (node) {
    this.filterArgs.content = node.cuboid.id
    this.loadAggIndices()
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
  async loadAggIndices () {
    const res = await this.fetchModelAggregates(Object.assign({
      project: this.projectName,
      model: this.model.uuid
    }, this.filterArgs))
    const data = await handleSuccessAsync(res)
    this.indexDatas = data.indices
    this.dataRange = (data.start_time && data.end_time) ? transToServerGmtTime(data.start_time) + this.$t('to') + transToServerGmtTime(data.end_time) : undefined
    this.totalSize = data.size
  }
  async mounted () {
    await this.freshCuboids()
    await this.loadAggIndices()
  }
  async refreshCuboidsAfterSubmitSetting () {
    await this.freshCuboids()
    await this.loadAggIndices()
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
  .tabel-scroll {
    overflow: hidden;
    height: 400px;
  }
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
  .el-icon-ksd-desc {
    &:hover {
      color: @base-color;
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
