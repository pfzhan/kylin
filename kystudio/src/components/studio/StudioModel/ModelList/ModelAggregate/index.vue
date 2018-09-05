<template>
  <div class="model-aggregate" v-if="model">
    <div class="aggregate-actions">
      <el-button icon="el-icon-ksd-table_refresh">
        {{$t('kylinLang.common.refresh')}}
      </el-button>
      <el-button icon="el-icon-ksd-table_delete">
        {{$t('kylinLang.common.delete')}}
      </el-button>
      <el-button icon="el-icon-ksd-backup" @click="newAggregateGroup">
        {{$t('aggregateGroup')}}
      </el-button>
    </div>
    <div class="aggregate-view">
      <el-row :gutter="20">
        <el-col :span="16">
          <div class="cubois-chart-block">
            <div class="ksd-mt-10 ksd-mr-10 ksd-fright agg-amount-block">
              <span>Aggregate Amount</span>
              <el-input v-model.trim="aggAmount" size="small"></el-input>
            </div>
            <FlowerChart :data="aggregates" />
          </div>
        </el-col>
        <el-col :span="8">
          <el-card class="agg-detail-card">
            <div slot="header" class="clearfix">
              <span>Aggregate Detail</span>
            </div>
            <div class="detail-content">
              <el-row :gutter="5"><el-col :span="11" class="label">ID:</el-col><el-col :span="13">{{aggDetail.id}}</el-col></el-row>
              <el-row :gutter="5">
                <el-col :span="11" class="label">Dimension and Order:</el-col>
                <el-col :span="13"><div v-for="item in aggDetail.dim" :key="item" class="dim-item">{{item}}</div></el-col>
              </el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Data Size:</el-col><el-col :span="13">{{aggDetail.dataSize}}</el-col></el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Data Range:</el-col><el-col :span="13">{{aggDetail.dateFrom | gmtTime}} To {{aggDetail.dateTo | gmtTime}}</el-col></el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Served Query amount:</el-col><el-col :span="13">{{aggDetail.amount}} Query</el-col></el-row>
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
import { getPlaintTreeNode } from './handle'
import FlowerChart from '../../../../common/FlowerChart'
import { handleSuccessAsync } from '../../../../../util'
import { aggregateGroups } from './mock'
import AggregateModal from './AggregateModal/index.vue'

@Component({
  props: {
    model: {
      type: Object
    }
  },
  methods: {
    ...mapActions('AggregateModal', {
      callAggregateModal: 'CALL_MODAL'
    }),
    ...mapActions({
      getModelAggregateIndex: 'GET_MODEL_AGGREGATE_INDEX'
    })
  },
  components: {
    FlowerChart,
    AggregateModal
  },
  locales
})
export default class ModelAggregate extends Vue {
  aggAmount = 0
  aggregates = {}
  aggDetail = {
    id: '2234kdrkg343532342jk',
    dim: ['Dimension_1', 'Dimension_2', 'Sum ( Price )', 'Dimension_5', 'TopN ( Seller_ID ) group by Sum ( Price )'],
    dataSize: '256MB',
    dateFrom: 1524829437628,
    dateTo: 1524829437628,
    amount: 12
  }

  async mounted () {
    const res = await this.getModelAggregateIndex()
    const data = await handleSuccessAsync(res)
    this.aggAmount = data.size
    this.aggregates = getPlaintTreeNode(data.data)
  }
  async newAggregateGroup () {
    await this.callAggregateModal({ editType: 'new', model: { ...this.model, aggregateGroups } })
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
    .el-card__header {
      background-color: @grey-3;
      color: @text-title-color;
      font-size: 16px;
    }
    .el-card__body {
      padding: 10px;
      .detail-content {
        .el-row {
          margin-bottom: 10px;
          .dim-item {
            margin-bottom: 5px;
          }
        }
      }
    }
    .label {
      text-align: right;
    }
  }
}
</style>
