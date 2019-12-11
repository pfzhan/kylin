<template>
  <div class="model-aggregate ksd-mb-15" v-if="model">
    <div class="aggregate-actions" v-if="isShowAggregateAction">
      <el-button-group>
        <el-button type="primary" plain size="small" v-guide.addAggBtn icon="el-icon-ksd-add_2" @click="handleAggregateGroup" v-if="isShowEditAgg">
          {{$t('aggregateGroup')}}
        </el-button>
        <el-button v-if="!isAutoProject" type="primary" plain size="small" @click="openAggAdvancedModal()">{{$t('aggIndexAdvancedTitle')}}</el-button>
      </el-button-group><el-button
        type="primary" plain size="small" class="ksd-ml-10" icon="el-icon-ksd-add_2" v-if="isShowTableIndexActions" v-visible="!isHideEdit" @click="editTableIndex()">{{$t('tableIndex')}}
      </el-button><el-button
        type="primary" plain size="small" class="ksd-ml-10" :loading="buildIndexLoading" @click="buildAggIndex" v-if="isShowBulidIndex&&cuboidCount">
        {{$t('buildIndex')}}
      </el-button><common-tip :content="$t('noIndexTips')" v-if="isShowBulidIndex&&!cuboidCount"><el-button
        type="primary" plain size="small" disabled class="ksd-ml-10" :loading="buildIndexLoading" @click="buildAggIndex" v-if="isShowBulidIndex">
        {{$t('buildIndex')}}
      </el-button></common-tip>
    </div>
    <div class="aggregate-view">
      <el-row :gutter="15">
        <el-col :span="12">
          <el-card class="agg-detail-card agg_index">
            <div slot="header" class="clearfix">
              <div class="left font-medium">
                <span>{{$t('aggregateIndexTree')}}</span>
                <el-tooltip :content="$t('treemapTips')" placement="left">
                  <i class="el-icon-ksd-what"></i>
                </el-tooltip>
              </div>
              <div class="right">
                <span>{{$t('aggregateAmount')}}</span>{{cuboidCount}}
                <!-- <el-input v-model.trim="cuboidCount" :readonly="true" size="small"></el-input> -->
              </div>
            </div>
            <div class="agg-counter">
              <div>
                <!-- <img src="./empty_note.jpg" /> -->
                <span>{{$t('emptyAggregate')}}</span>
                <span>{{emptyCuboidCount}}</span>
              </div>
              <!-- <div>
                <img src="./broken_note.jpg" />
                <span>{{$t('brokenAggregate')}}</span>
                <span>{{brokenCuboidCount}}</span>
              </div> -->
            </div>
            <kap-empty-data v-if="cuboidCount === 0 || noDataNum === 0" size="small"></kap-empty-data>
            <TreemapChart
              v-else
              :data="cuboids"
              :search-id="filterArgs.key"
              @searchId="handleClickNode"/>
          </el-card>
        </el-col>
        <el-col :span="12">
          <el-card class="agg-detail-card agg-detail">
            <div slot="header" class="clearfix">
              <div class="left font-medium fix">{{$t('aggregateDetail')}}</div>
              <div class="right fix">
                <el-input class="search-input" v-model.trim="filterArgs.key" size="mini" :placeholder="$t('searchAggregateID')" prefix-icon="el-icon-search" @input="searchAggs"></el-input>
              </div>
            </div>
            <div class="detail-content">
              <div class="ksd-mb-10 ksd-fs-12" v-if="isFullLoaded">
                {{$t('dataRange')}}: {{$t('kylinLang.dataSource.full')}}
              </div>
              <div class="ksd-mb-10 ksd-fs-12" v-if="dataRange&&!isFullLoaded">
                {{$t('dataRange')}}: {{dataRange}}
              </div>
              <el-table
                nested
                border
                :data="indexDatas"
                class="indexes-table"
                size="medium"
                @sort-change="onSortChange"
                :row-class-name="tableRowClassName">
                <el-table-column prop="id" show-overflow-tooltip :label="$t('id')" width="100"></el-table-column>
                <el-table-column prop="data_size" width="100" sortable="custom" show-overflow-tooltip align="right" :label="$t('storage')">
                  <template slot-scope="scope">
                    {{scope.row.data_size | dataSize}}
                  </template>
                </el-table-column>
                <el-table-column prop="usage" width="100" sortable="custom" show-overflow-tooltip align="right" :label="$t('queryCount')"></el-table-column>
                <el-table-column prop="source" show-overflow-tooltip :renderHeader="renderColumn">
                  <template slot-scope="scope">
                    <span>{{$t(scope.row.source)}}</span>
                  </template>
                </el-table-column>
                <el-table-column prop="status" show-overflow-tooltip :renderHeader="renderColumn2" width="100">
                  <template slot-scope="scope">
                    <span>{{$t(scope.row.status)}}</span>
                  </template>
                </el-table-column>
                <el-table-column :label="$t('kylinLang.common.action')" width="83">
                  <template slot-scope="scope">
                    <common-tip :content="$t('viewDetail')">
                      <i class="el-icon-ksd-desc" @click="showDetail(scope.row)"></i>
                    </common-tip>
                    <common-tip :content="$t('editIndex')">
                      <i class="el-icon-ksd-table_edit ksd-ml-5" v-if="scope.row.source === 'MANUAL_TABLE'" @click="editTableIndex(scope.row)"></i>
                    </common-tip>
                    <common-tip :content="$t('delIndex')">
                      <i class="el-icon-ksd-table_delete ksd-ml-5" @click="removeIndex(scope.row)"></i>
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
      :title="indexDetailTitle"
      width="480px"
      :limited-area="true"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="resetDetail"
      :visible.sync="indexDetailShow">
      <div class="ksd-mb-10 ksd-fs-12">{{$t('modifiedTime')}}: {{cuboidDetail.modifiedTime || showTableIndexDetail.modifiedTime}}</div>
      <el-table class="cuboid-content" :data="cuboidDetail.cuboidContent" border v-if="detailType === 'aggDetail'">
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
      </el-table>
      <div v-else>
          <el-table
          size="medium"
          :data="showTableIndexDetail.renderData"
          border class="table-index-detail">
          <el-table-column
            :label="$t('ID')"
            prop="id"
            width="64">
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('column')"
            prop="column">
          </el-table-column>
          <el-table-column
          :label="$t('sort')"
          prop="sort"
          width="60"
          align="center">
          <template slot-scope="scope">
            <span class="ky-dot-tag" v-show="scope.row.sort">{{scope.row.sort}}</span>
          </template>
            </el-table-column>
          <el-table-column
          label="Shard"
          align="center"
          width="70">
            <template slot-scope="scope">
                <i class="el-icon-ksd-good_health ky-success" v-show="scope.row.shared"></i>
            </template>
            </el-table-column>
          </el-table>
          <kap-pager layout="prev, pager, next" :background="false" class="ksd-mt-10 ksd-center" ref="pager" :perpage_size="currentCount" :totalSize="totalTableIndexColumnSize"  v-on:handleCurrentChange='currentChange'></kap-pager>
        </div>
      <div slot="footer" class="dialog-footer">
        <el-button type="default" size="medium" @click="indexDetailShow=false">{{$t('kylinLang.common.close')}}</el-button>
      </div>
    </el-dialog>

    <AggregateModal/>
    <TableIndexEdit/>
    <AggAdvancedModal v-on:refreshIndexGraph="refreshIndexGraphAfterSubmitSetting" />
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import FlowerChart from '../../../../common/FlowerChart'
import TreemapChart from '../../../../common/TreemapChart'
import { handleSuccessAsync, objectClone } from '../../../../../util'
import { handleError, transToGmtTime, kapConfirm, transToServerGmtTime } from '../../../../../util/business'
import { speedProjectTypes } from '../../../../../config'
import { BuildIndexStatus } from '../../../../../config/model'
import AggregateModal from './AggregateModal/index.vue'
import AggAdvancedModal from './AggAdvancedModal/index.vue'
import TableIndexEdit from '../../TableIndexEdit/tableindex_edit'
import { formatGraphData } from './handler'
import NModel from '../../ModelEdit/model.js'

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
    },
    isShowTableIndexActions: {
      type: Boolean,
      default: true
    },
    isHideEdit: {
      type: Boolean,
      default: false
    }
  },
  computed: {
    ...mapGetters([
      'currentProjectData',
      'isAutoProject'
    ]),
    modelInstance () {
      this.model.project = this.currentProjectData.name
      return new NModel(this.model)
    }
  },
  methods: {
    ...mapActions('AggregateModal', {
      callAggregateModal: 'CALL_MODAL'
    }),
    ...mapActions('AggAdvancedModal', {
      callAggAdvancedModal: 'CALL_MODAL'
    }),
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    }),
    ...mapActions({
      fetchIndexGraph: 'FETCH_INDEX_GRAPH',
      buildIndex: 'BUILD_INDEX',
      loadAllIndex: 'LOAD_ALL_INDEX',
      deleteIndex: 'DELETE_INDEX'
    })
  },
  components: {
    FlowerChart,
    TreemapChart,
    AggregateModal,
    AggAdvancedModal,
    TableIndexEdit
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
  buildIndexLoading = false
  indexDatas = []
  dataRange = ''
  totalSize = 0
  filterArgs = {
    page_offset: 0,
    page_size: 10,
    key: '',
    sort_by: '',
    reverse: '',
    sources: [],
    status: []
  }
  ST = null
  indexDetailShow = false
  tableIndexBaseList = []
  realFilteArr = ['AUTO_AGG', 'MANUAL_AGG', 'AUTO_TABLE', 'MANUAL_TABLE']
  statusArr = ['EMPTY', 'AVAILABLE', 'TO_BE_DELETED', 'BUILDING']
  detailType = ''
  currentPage = 0
  currentCount = 10
  totalTableIndexColumnSize = 0
  isFullLoaded = false
  indexDetailTitle = ''
  // 打开高级设置
  openAggAdvancedModal () {
    this.callAggAdvancedModal({
      model: objectClone(this.model),
      aggIndexAdvancedDesc: null
    })
  }

  tableRowClassName ({row, rowIndex}) {
    if (row.status === 'EMPTY' || row.status === 'BUILDING') {
      return 'empty-index'
    }
    return ''
  }

  handleBuildIndexTip (data) {
    let tipMsg = ''
    if (data.type === BuildIndexStatus.NORM_BUILD) {
      tipMsg = this.$t('kylinLang.model.buildIndexSuccess')
      this.$message({message: tipMsg, type: 'success'})
      return
    }
    if (data.type === BuildIndexStatus.NO_LAYOUT) {
      tipMsg = this.$t('kylinLang.model.buildIndexFail2', {indexType: this.$t('kylinLang.model.index')})
    } else if (data.type === BuildIndexStatus.NO_SEGMENT) {
      tipMsg += this.$t('kylinLang.model.buildIndexFail1', {modelName: this.model.name})
    }
    this.$confirm(tipMsg, this.$t('kylinLang.common.notice'), {showCancelButton: false, type: 'warning', dangerouslyUseHTMLString: true})
  }
  editTableIndex (indexDesc) {
    this.showTableIndexEditModal({
      modelInstance: this.modelInstance,
      tableIndexDesc: indexDesc || {name: 'TableIndex_1'}
    }).then((res) => {
      if (res.isSubmit) {
        this.refreshIndexGraphAfterSubmitSetting()
      }
    })
  }
  renderColumn (h) {
    let items = []
    for (let i = 0; i < this.realFilteArr.length; i++) {
      items.push(<el-checkbox label={this.realFilteArr[i]} key={this.realFilteArr[i]}>{this.$t(this.realFilteArr[i])}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('source')}</span>
      <el-popover
        ref="sourceFilterPopover"
        placement="bottom-start"
        popperClass="source-filter">
        <el-checkbox-group class="filter-groups" value={this.filterArgs.sources} onInput={val => (this.filterArgs.sources = val)} onChange={this.loadAggIndices}>
          {items}
        </el-checkbox-group>
        <i class={this.filterArgs.sources.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumn2 (h) {
    let items = []
    for (let i = 0; i < this.statusArr.length; i++) {
      items.push(<el-checkbox label={this.statusArr[i]} key={this.statusArr[i]}>{this.$t(this.statusArr[i])}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.common.status')}</span>
      <el-popover
        ref="sourceFilterPopover"
        placement="bottom-start"
        popperClass="source-filter">
        <el-checkbox-group class="filter-groups" value={this.filterArgs.status} onInput={val => (this.filterArgs.status = val)} onChange={this.loadAggIndices}>
          {items}
        </el-checkbox-group>
        <i class={this.filterArgs.status.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
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
  resetDetail () {
    this.currentPage = 0
    this.currentCount = 10
    this.totalTableIndexColumnSize = 0
  }
  currentChange (size, count) {
    this.currentPage = size
    this.currentCount = count
  }
  showDetail (row) {
    this.cuboidData = row
    this.detailType = row.source.indexOf('AGG') >= 0 ? 'aggDetail' : 'tabelIndexDetail'
    this.indexDetailTitle = row.source.indexOf('AGG') >= 0 ? this.$t('aggDetailTitle') : this.$t('tabelDetailTitle')
    this.indexDetailShow = true
  }
  async removeIndex (row) {
    try {
      await kapConfirm(this.$t('delIndexTip'), null, this.$t('delIndex'))
      await this.deleteIndex({project: this.projectName, model: this.model.uuid, id: row.id})
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.refreshIndexGraphAfterSubmitSetting()
    } catch (e) {
      handleError(e)
    }
  }
  onSortChange ({ column, prop, order }) {
    this.filterArgs.sort_by = prop
    this.filterArgs.reverse = !(order === 'ascending')
    this.loadAggIndices()
  }
  pageCurrentChange (size, count) {
    this.filterArgs.page_offset = size
    this.filterArgs.page_size = count
    this.loadAggIndices()
  }
  searchAggs () {
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      this.filterArgs.page_offset = 0
      this.loadAggIndices()
    }, 500)
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
    const modifiedTime = transToGmtTime(this.cuboidData.last_modified_time)
    return { renderData, modifiedTime }
  }
  get isSpeedProject () {
    return speedProjectTypes.includes(this.currentProjectData.maintain_model_type)
  }
  get cuboidDetail () {
    if (!this.cuboidData || !this.cuboidData.col_order || this.detailType === 'tabelIndexDetail') {
      return []
    }
    const modifiedTime = transToGmtTime(this.cuboidData.last_modified_time)
    const cuboidContent = this.cuboidData.col_order.map(col => ({ content: col.key, type: col.value === 'measure' ? 'measure' : 'dimension' }))
    return { modifiedTime, cuboidContent }
  }
  async handleClickNode (id) {
    this.filterArgs.key = id
    this.loadAggIndices()
  }
  async freshIndexGraph () {
    try {
      const res = await this.fetchIndexGraph({
        project: this.projectName,
        model: this.model.uuid
      })
      const data = await handleSuccessAsync(res)
      this.dataRange = (data.start_time && data.end_time) ? transToServerGmtTime(data.start_time) + this.$t('to') + transToServerGmtTime(data.end_time) : undefined
      this.isFullLoaded = data.is_full_loaded
      this.cuboids = formatGraphData(data)
      this.cuboidCount = data.total_indexes
      this.emptyCuboidCount = data.empty_indexes
    } catch (e) {
      handleError(e)
    }
  }
  get noDataNum () {
    let nodeNum = 0
    this.cuboids.forEach((n) => {
      nodeNum = nodeNum + n.children.length
    })
    return nodeNum
  }
  async loadAggIndices () {
    try {
      const res = await this.loadAllIndex(Object.assign({
        project: this.projectName,
        model: this.model.uuid
      }, this.filterArgs))
      const data = await handleSuccessAsync(res)
      this.indexDatas = data.value
      this.totalSize = data.total_size
    } catch (e) {
      handleError(e)
    }
  }
  async mounted () {
    await this.freshIndexGraph()
    await this.loadAggIndices()
  }
  async refreshIndexGraphAfterSubmitSetting () {
    await this.freshIndexGraph()
    await this.loadAggIndices()
  }
  async handleAggregateGroup () {
    const { projectName, model } = this
    const isSubmit = await this.callAggregateModal({ editType: 'edit', model, projectName })
    isSubmit && await this.refreshIndexGraphAfterSubmitSetting()
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.model-aggregate {
  .el-button-group .el-button--primary:last-child {
    border-left-color: @base-color;
  }
  .indexes-table {
    .empty-index {
      background: @warning-color-2;
    }
    .el-popover.source-filter {
      min-width: 130px;
      box-sizing: border-box;
    }
    .el-icon-ksd-filter {
      position: relative;
      left: 5px;
      &.isFilter,
      &:hover {
        color: @base-color;
      }
    }
  }
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
        max-width: 250px;
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
