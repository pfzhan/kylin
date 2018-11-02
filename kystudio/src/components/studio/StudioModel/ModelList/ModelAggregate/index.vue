<template>
  <div class="model-aggregate" v-if="model">
    <div class="aggregate-actions">
      <!-- <el-button type="primary" icon="el-icon-ksd-table_refresh">
        {{$t('kylinLang.common.refresh')}}
      </el-button>
      <el-button type="primary" icon="el-icon-ksd-table_delete">
        {{$t('kylinLang.common.delete')}}
      </el-button> -->
      <el-button type="primary" icon="el-icon-ksd-backup" @click="handleAggregateGroup">
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
                <el-input v-model.trim="cuboidCount" size="small"></el-input>
              </div>
            </div>
            <PartitionChart :data="cuboids" @on-click-node="handleClickNode" :search-id="searchCuboidId" />
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
                <el-row :gutter="5">
                  <el-col :span="10" class="label">ID:</el-col>
                  <el-col :span="14">{{cuboidDetail.id}}</el-col>
                </el-row>
                <el-row :gutter="5">
                  <el-col :span="10" class="label">{{$t('dimensionAndOrder')}}:</el-col>
                  <el-col :span="14"><div v-for="item in cuboidDetail.dim" :key="item" class="dim-item">{{item}}</div></el-col>
                </el-row>
                <el-row :gutter="5">
                  <el-col :span="10" class="label">{{$t('dataSize')}}:</el-col>
                  <el-col :span="14">{{cuboidDetail.dataSize}}</el-col>
                </el-row>
                <el-row :gutter="5">
                  <el-col :span="10" class="label">{{$t('dataRange')}}:</el-col>
                  <el-col :span="14">{{cuboidDetail.dateFrom}} To {{cuboidDetail.dateTo}}</el-col>
                </el-row>
                <el-row :gutter="5">
                  <el-col :span="10" class="label">{{$t('servedQueryAmount')}}:</el-col>
                  <el-col :span="14">{{cuboidDetail.amount}} Query</el-col>
                </el-row>
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
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import { formatFlowerJson, getCuboidCounts } from './handle'
import FlowerChart from '../../../../common/FlowerChart'
import PartitionChart from '../../../../common/PartitionChart'
import { handleSuccessAsync, transToGmtTime } from '../../../../../util'
import AggregateModal from './AggregateModal/index.vue'

@Component({
  props: {
    model: {
      type: Object
    },
    projectName: {
      type: String
    }
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
  cuboids = []
  cuboidDetail = {
    id: '',
    dim: [],
    dataSize: 0,
    dateFrom: 0,
    dateTo: 0,
    amount: 0
  }
  searchCuboidId = ''

  async handleClickNode (node) {
    const cuboidId = node.cuboid.id
    const res = await this.fetchCuboid({
      projectName: this.projectName,
      modelName: this.model.name,
      cuboidId
    })
    const cuboid = await handleSuccessAsync(res)
    this.cuboidDetail.id = cuboid.id
    this.cuboidDetail.dim = cuboid.dimensions_res
    this.cuboidDetail.dataSize = cuboid.storage_size < 1024 ? `${cuboid.storage_size}KB` : `${(cuboid.storage_size / 1024).toFixed(2)}MB`
    this.cuboidDetail.dateFrom = transToGmtTime(cuboid.start_time)
    this.cuboidDetail.dateTo = transToGmtTime(cuboid.end_time)
    if (this.cuboidDetail.dateFrom) {
      this.cuboidDetail.dateFrom = this.cuboidDetail.dateFrom.split(' GMT')[0]
    }
    if (this.cuboidDetail.dateTo) {
      this.cuboidDetail.dateTo = this.cuboidDetail.dateTo.split(' GMT')[0]
    }
  }
  async freshCuboids () {
    const projectName = this.projectName
    const modelName = this.model.name
    const res = await this.fetchCuboids({ modelName, projectName })
    const data = await handleSuccessAsync(res)
    this.cuboids = formatFlowerJson(data)
    this.cuboidCount = getCuboidCounts(data)
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
  .cubois-chart-block {
    border: 1px solid @line-border-color;
    height: 638px;
    position: relative;
    overflow: hidden;
  }
  .agg-detail-card {
    height: 638px;
    box-shadow: none;
    // .el-card__header {
    //   background-color: @grey-3;
    //   color: @text-title-color;
    //   font-size: 16px;
    //   padding: 7px 9px 7px 17px;
    // }
    // &.agg_index {
    //   .el-card__header {
    //     padding: 7px 9px 7px 17px;
    //   }
    // }
    .el-card__body {
      overflow: auto;
      padding: 10px;
      height: 583px;
      width: 100%;
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
