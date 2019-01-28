<template>
  <div class="model-aggregate" v-if="model">
    <div class="aggregate-actions" v-if="isShowAggregateAction">
      <!-- <el-button type="primary" icon="el-icon-ksd-table_refresh">
        {{$t('kylinLang.common.refresh')}}
      </el-button>
      <el-button type="primary" icon="el-icon-ksd-table_delete">
        {{$t('kylinLang.common.delete')}}
      </el-button> -->
      <el-button type="primary" v-guide.addAggBtn icon="el-icon-ksd-table_edit" @click="handleAggregateGroup" v-if="availableAggregateActions.includes('viewAggGroup')">
        {{$t('aggregateGroup')}}
      </el-button>
    </div>
    <div class="aggregate-view">
      <el-row :gutter="20">
        <el-col :span="15">
          <el-card class="agg-detail-card agg_index">
            <div slot="header" class="clearfix">
              <div class="left font-medium">{{$t('aggregateIndexTree')}}</div>
              <div class="right">
                <span>{{$t('aggregateAmount')}}</span>
                <el-input v-model.trim="cuboidCount" :readonly="true" size="small"></el-input>
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
            <PartitionChart
              :data="cuboids"
              :search-id="searchCuboidId"
              :background-maps="backgroundMaps"
              @on-click-node="handleClickNode"/>
          </el-card>
        </el-col>
        <el-col :span="9">
          <el-card class="agg-detail-card">
            <div slot="header" class="clearfix">
              <div class="left font-medium fix">{{$t('aggregateDetail')}}</div>
              <div class="right fix">
                <el-input class="search-input" v-model.trim="searchCuboidId" size="small" :placeholder="$t('searchAggregateID')" prefix-icon="el-icon-search"></el-input>
              </div>
            </div>
            <div class="detail-content">
              <template v-if="cuboidDetail.id !== ''">
                <el-table class="cuboid-info" :data="cuboidInfo" border stripe :show-header="false">
                  <el-table-column prop="key" align="right" width="166px">
                    <template slot-scope="scope">
                      <div v-if="scope.row.key === 'dataRange'">
                        <div>{{$t(scope.row.key)}}</div>
                        <div class="slot" v-if="scope.row.value">slot</div>
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
                <el-table class="cuboid-content" :data="cuboidContent" border max-height="335">
                  <el-table-column type="index" :label="$t('order')" width="64" align="center">
                  </el-table-column>
                  <el-table-column prop="content" :label="$t('content')" align="center">
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
import dayjs from 'dayjs'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import FlowerChart from '../../../../common/FlowerChart'
import PartitionChart from '../../../../common/PartitionChart'
import { handleSuccessAsync } from '../../../../../util'
import { speedProjectTypes } from '../../../../../config'
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
    }
  },
  computed: {
    ...mapGetters([
      'currentProjectData',
      'availableAggregateActions'
    ])
  },
  methods: {
    ...mapActions('AggregateModal', {
      callAggregateModal: 'CALL_MODAL'
    }),
    ...mapActions({
      fetchModelAggregates: 'FETCH_AGGREGATES',
      fetchCuboids: 'FETCH_CUBOIDS',
      fetchCuboid: 'FETCH_CUBOID'
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
    const startDate = dayjs(this.cuboidData.start_time).format('YYYY-MM-DD HH:mm:ss')
    const endDate = dayjs(this.cuboidData.end_time).format('YYYY-MM-DD HH:mm:ss')
    const storage = this.cuboidData.storage_size
    const queryCount = this.cuboidData.amount || undefined
    const dataRange = (this.cuboidData.start_time && this.cuboidData.end_time) ? { startDate, to: this.$t('to'), endDate } : undefined
    return { id, dimensions, measures, dataRange, storage, queryCount }
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
  padding: 20px 0;
  .aggregate-actions {
    margin-bottom: 15px;
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
    top: 10px;
    right: 10px;
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
  .cubois-chart-block {
    border: 1px solid @line-border-color;
    height: 638px;
    position: relative;
    overflow: hidden;
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
    padding-left: 10px;
  }
  .agg-detail-card {
    height: 638px;
    box-shadow: none;
    .el-card__body {
      overflow: auto;
      padding: 10px;
      height: 583px;
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
