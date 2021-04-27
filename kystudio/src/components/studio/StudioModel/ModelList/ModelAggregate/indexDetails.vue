<template>
  <el-dialog class="indexes-result-box"
    :title="indexDetailTitle"
    width="480px"
    :limited-area="true"
    :append-to-body="true"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="resetDetail"
    :visible="true">
    <div class="ksd-mb-10 ksd-fs-12 indexes-content-details">
      <span>{{$t(detailType === 'aggDetail' ? 'customAggIndex' : 'customTableIndex')}}
        <span v-if="detailType === 'aggDetail' && cuboidDetail.cuboidContent">{{$t('indexContentTips', {dimensionNum: cuboidDetail.cuboidContent.filter(it => it.type === 'dimension').length, measureNum: cuboidDetail.cuboidContent.filter(it => it.type === 'measure').length})}}</span>
        <span v-else-if="detailType !== 'aggDetail' && cuboidData.col_order">{{$t('tableIndexContentTip', {num: cuboidData.col_order.length})}}</span>
      </span>
      <span><el-tooltip :content="$t('modifiedTime')" placement="top"><i class="el-icon-ksd-type_time"></i></el-tooltip> {{cuboidDetail.modifiedTime || showTableIndexDetail.modifiedTime}}</span>
    </div>
    <el-table class="cuboid-content" :data="cuboidDetail.cuboidContent" size="small" border v-if="detailType === 'aggDetail'">
      <el-table-column type="index" :label="$t('order')" width="64">
      </el-table-column>
      <el-table-column prop="content" show-overflow-tooltip :label="$t('content')">
        <template slot-scope="scope">
          <span>{{scope.row.content}}</span>
        </template>
      </el-table-column>
      <el-table-column prop="type" :label="$t('kylinLang.query.type')" width="90">
        <template slot-scope="scope">
          <span>{{$t('kylinLang.cube.' + scope.row.type)}}</span>
        </template>
      </el-table-column>
      <el-table-column prop="cardinality" :label="$t('cardinality')" info-icon="el-icon-ksd-what" :info-tooltip="$t('cardinalityColumnTips')" sortable>
        <template slot-scope="scope">
          <span v-if="scope.row.cardinality">{{scope.row.cardinality}}</span>
          <span v-else><i class="no-data_placeholder">NULL</i></span>
        </template>
      </el-table-column>
      <el-table-column
        label="Shard by"
        align="center"
        width="80">
        <template slot-scope="scope">
            <i class="el-icon-ksd-good_health ky-success" v-show="scope.row.isSharedBy"></i>
        </template>
      </el-table-column>
    </el-table>
    <div v-else>
        <el-table
        size="small"
        :data="showTableIndexDetail.renderData"
        border class="table-index-detail">
        <!-- <el-table-column
          :label="$t('ID')"
          prop="id"
          width="64">
        </el-table-column> -->
        <el-table-column
          show-overflow-tooltip
          :label="$t('column')"
          prop="column">
        </el-table-column>
        <el-table-column
        :label="$t('sort')"
        prop="sort"
        width="70"
        align="center">
        <template slot-scope="scope">
          <span class="ky-dot-tag" v-show="scope.row.sort">{{scope.row.sort}}</span>
        </template>
          </el-table-column>
        <el-table-column
        label="Shard by"
        align="center"
        width="80">
          <template slot-scope="scope">
              <i class="el-icon-ksd-good_health ky-success" v-show="scope.row.shared"></i>
          </template>
          </el-table-column>
        </el-table>
        <kap-pager layout="prev, pager, next" :background="false" class="ksd-mt-10 ksd-center" ref="pager" :refTag="pageRefTags.IndexDetailPager" :perpage_size="currentCount" :curPage="currentPage+1" :totalSize="totalTableIndexColumnSize"  v-on:handleCurrentChange='currentChange'></kap-pager>
      </div>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="closeIndexDetailsDialog">{{$t('kylinLang.common.close')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import locales from './locales'
import { Component } from 'vue-property-decorator'
import { pageRefTags } from 'config'
import { transToGmtTime } from '../../../../../util/business'

@Component({
  props: {
    indexDetailTitle: {
      type: String,
      default: ''
    },
    detailType: {
      type: String,
      default: ''
    },
    cuboidData: {
      type: Object,
      default () {
        return {}
      }
    }
  },
  locales
})
export default class indexDetails extends Vue {
  pageRefTags = pageRefTags
  currentPage = 0
  currentCount = +localStorage.getItem(this.pageRefTags.IndexDetailPager) || 10
  totalTableIndexColumnSize = 0

  get cuboidDetail () {
    if (!this.cuboidData || !this.cuboidData.col_order || this.detailType === 'tabelIndexDetail') {
      return []
    }
    const modifiedTime = transToGmtTime(this.cuboidData.last_modified)
    const cuboidContent = this.cuboidData.col_order.map(col => ({ content: col.key, type: col.value === 'measure' ? 'measure' : 'dimension', cardinality: col.cardinality, isSharedBy: this.cuboidData.shard_by_columns.includes(col.key) }))
    return { modifiedTime, cuboidContent }
  }

  get showTableIndexDetail () {
    if (!this.cuboidData || !this.cuboidData.col_order || this.detailType === 'aggDetail') {
      return []
    }
    let tableIndexList = this.cuboidData.col_order.slice(this.currentCount * this.currentPage, this.currentCount * (this.currentPage + 1))
    this.totalTableIndexColumnSize = this.cuboidData.col_order.length
    let renderData = tableIndexList.map((item, i) => {
      let newitem = {
        id: this.currentCount * this.currentPage + i + 1,
        column: item.key,
        sort: this.cuboidData.sort_by_columns.indexOf(item.key) + 1 || '',
        shared: this.cuboidData.shard_by_columns.includes(item.key)
      }
      return newitem
    })
    const modifiedTime = transToGmtTime(this.cuboidData.last_modified)
    return { renderData, modifiedTime }
  }

  currentChange (size, count) {
    this.currentPage = size
    this.currentCount = count
  }

  resetDetail () {
    this.currentPage = 0
    this.currentCount = 10
    this.totalTableIndexColumnSize = 0
    this.$emit('close')
  }

  closeIndexDetailsDialog () {
    // this.$emit('close')
    this.resetDetail()
  }
}
</script>

<style lang="less">
.table-index-detail {
  .ky-dot-tag {
    line-height: 15px !important;
  }
}
</style>
