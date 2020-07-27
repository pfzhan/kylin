<template>
  <div class="model-aggregate ksd-mb-15" v-if="model" v-loading="isLoading">
    <div class="aggregate-view">
      <div class="aggregate-tree-map" :style="{width: `${moveEvent.w}%`}">
        <el-card class="agg-detail-card agg_index">
          <div slot="header" class="clearfix">
            <div class="left font-medium">
              <span>{{$t('aggregateIndexTree')}}</span>
              <el-tooltip :content="$t('treemapTips')" placement="left">
                <i class="el-icon-ksd-what"></i>
              </el-tooltip>
            </div>
          </div>
          <div class="agg-counter ksd-fs-12">
            <div>
              <span>{{$t('aggregateAmount')}}</span>
              <span>{{cuboidCount}}</span>
              <span class="divide"></span>
              <span>{{$t('emptyAggregate')}}</span>
              <span>{{emptyCuboidCount}}</span>
            </div>
          </div>
          <kap-empty-data v-if="cuboidCount === 0 || noDataNum === 0" size="small"></kap-empty-data>
          <TreemapChart
            v-else
            :data="cuboids"
            :search-id="filterArgs.key"
            :idTag="model.uuid"
            ref="indexTreeMap"
            @searchId="handleClickNode"/>
        </el-card>
        <div class="drag-bar ky-drag-layout-bar" @mousedown="handlerDownEvent">||</div>
      </div>
      <div class="index-group" :style="{width: `calc(${100 - moveEvent.w}% - 10px)`}">
        <div class="btn-groups" v-if="isShowAggregateAction">
          <el-button-group>
            <el-button plain size="mini" icon="el-icon-ksd-login_intro" @click="switchIndexValue = 0" :class="{'active': switchIndexValue === 0}">{{$t('indexListBtn')}}</el-button>
            <el-button plain size="mini" icon="el-icon-ksd-status" v-if="$store.state.project.isSemiAutomatic && datasourceActions.includes('accelerationActions')" @click="switchIndexValue = 1" :class="{'active': switchIndexValue === 1}">{{$t('recommendationsBtn')}}</el-button>
          </el-button-group>
        </div>
        <el-card class="agg-detail-card agg-detail" v-if="switchIndexValue === 0">
          <div slot="header" class="clearfix">
            <div class="left font-medium fix">{{$t('aggregateDetail')}}</div>
            <!-- <el-dropdown class="right ksd-ml-10" v-if="isShowAggregateAction&&isShowIndexActions">
              <el-button icon="el-icon-ksd-add_2" type="primary" plain size="small">{{$t('index')}}</el-button>
              <el-dropdown-menu slot="dropdown">
                <el-dropdown-item @click.native="handleAggregateGroup" v-if="isShowEditAgg">{{$t('aggregateGroup')}}</el-dropdown-item>
                <el-dropdown-item v-if="isShowTableIndexActions&&!isHideEdit" @click.native="confrimEditTableIndex()">{{$t('tableIndex')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
            <div class="right fix">
              <el-input class="search-input" v-model.trim="filterArgs.key" size="small" :placeholder="$t('searchAggregateID')" prefix-icon="el-icon-search" v-global-key-event.enter.debounce="searchAggs" @clear="searchAggs()"></el-input>
            </div> -->
          </div>
          <div class="detail-content" v-loading="indexLoading">
            <div class="clearfix">
              <div class="ksd-mb-10 ksd-fs-12 ksd-fleft">
                {{$t('dataRange')}}<el-tooltip :content="$t('dataRangeTips')" placement="top">
                  <i class="el-icon-ksd-what"></i>
                </el-tooltip>: {{getDataRange}}
              </div>
              <div v-if="isShowAggregateAction&&isHaveComplementSegs" @click="complementedIndexes('allIndexes')" class="text-btn-like ksd-fleft ksd-ml-5">
                <el-tooltip :content="$t('viewIncomplete')" effect="dark" placement="top">
                  <i class="el-icon-ksd-complete"></i>
                </el-tooltip>
              </div>
            </div>
            <div class="clearfix" v-if="isShowAggregateAction">
              <el-button icon="el-icon-ksd-icon_build-index" :disabled="!checkedList.length" v-if="datasourceActions.includes('buildIndex')" class="ksd-mb-10 ksd-fleft" size="small" @click="complementedIndexes('batchIndexes')">{{$t('buildIndex')}}</el-button>
              <el-dropdown
                split-button
                plain
                class="split-button ksd-mb-10 ksd-ml-10 ksd-fleft"
                :class="{'is-disabled': !checkedList.length}"
                size="small"
                placement="bottom-start"
                :loading="removeLoading"
                v-if="datasourceActions.includes('delAggIdx')"
                @click="removeIndexes">
                {{$t('kylinLang.common.delete')}}
                <el-dropdown-menu slot="dropdown" class="model-actions-dropdown">
                  <el-dropdown-item
                    :disabled="!checkedList.length"
                    @click="complementedIndexes('deleteIndexes')">
                    {{$t('deletePart')}}
                  </el-dropdown-item>
                </el-dropdown-menu>
              </el-dropdown>
              <!-- <el-button
              icon="el-icon-ksd-table_delete" :disabled="!checkedList.length" v-if="datasourceActions.includes('delAggIdx')" class="ksd-mb-10 ksd-ml-10" size="small" :loading="removeLoading" @click="removeIndexes()">{{$t('kylinLang.common.delete')}}</el-button> -->
              <el-dropdown class="right ksd-ml-10" v-if="isShowAggregateAction&&isShowIndexActions">
                <el-button icon="el-icon-ksd-add_2" type="primary" plain size="small">{{$t('index')}}</el-button>
                <el-dropdown-menu slot="dropdown">
                  <el-dropdown-item @click.native="handleAggregateGroup" v-if="isShowEditAgg">{{$t('aggregateGroup')}}</el-dropdown-item>
                  <el-dropdown-item v-if="isShowTableIndexActions&&!isHideEdit" @click.native="confrimEditTableIndex()">{{$t('tableIndex')}}</el-dropdown-item>
                </el-dropdown-menu>
              </el-dropdown>
              <div class="right fix">
                <el-input class="search-input" v-model.trim="filterArgs.key" size="small" :placeholder="$t('searchAggregateID')" prefix-icon="el-icon-search" v-global-key-event.enter.debounce="searchAggs" @clear="searchAggs()"></el-input>
              </div>
            </div>
            <div class="filter-tags-agg" v-show="filterTags.length">
              <div class="filter-tags-layout"><el-tag size="mini" closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{`${$t(item.source)}：${$t(item.label)}`}}</el-tag></div>
              <span class="clear-all-filters" @click="clearAllTags">{{$t('clearAll')}}</span>
            </div>
            <el-table
              nested
              border
              :data="indexDatas"
              class="indexes-table"
              size="medium"
              :empty-text="emptyText"
              @sort-change="onSortChange"
              @selection-change="handleSelectionChange"
              :row-class-name="tableRowClassName">
              <el-table-column type="selection" width="44"></el-table-column>
              <el-table-column prop="id" show-overflow-tooltip :label="$t('id')" width="100"></el-table-column>
              <el-table-column prop="data_size" width="100" sortable="custom" show-overflow-tooltip align="right" :label="$t('storage')">
                <template slot-scope="scope">
                  {{scope.row.data_size | dataSize}}
                </template>
              </el-table-column>
              <el-table-column prop="usage" width="100" sortable="custom" show-overflow-tooltip align="right" :label="$t('queryCount')"></el-table-column>
              <el-table-column prop="source" show-overflow-tooltip :filters="realFilteArr.map(item => ({text: $t(item), value: item}))" :filtered-value="filterArgs.sources" :label="$t('source')" filter-icon="el-icon-ksd-filter" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'sources')">
                <template slot-scope="scope">
                  <span>{{$t(scope.row.source)}}</span>
                </template>
              </el-table-column>
              <el-table-column prop="status" show-overflow-tooltip :filters="statusArr.map(item => ({text: $t(item), value: item}))" :filtered-value="filterArgs.status" :label="$t('kylinLang.common.status')" filter-icon="el-icon-ksd-filter" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'status')" width="100">
                <template slot-scope="scope">
                  <span>{{$t(scope.row.status)}}</span>
                </template>
              </el-table-column>
              <el-table-column :label="$t('kylinLang.common.action')" fixed="right" width="83">
                <template slot-scope="scope">
                  <common-tip :content="$t('viewDetail')">
                    <i class="el-icon-ksd-desc" @click="showDetail(scope.row)"></i>
                  </common-tip>
                  <common-tip :content="$t('buildIndex')" v-if="isShowAggregateAction&&datasourceActions.includes('buildIndex')">
                    <i class="el-icon-ksd-icon_build-index ksd-ml-5" @click="complementedIndexes('', scope.row.id)"></i>
                  </common-tip>
                  <common-tip :content="$t('editIndex')" v-if="isShowAggregateAction&&datasourceActions.includes('editAggGroup')">
                    <i class="el-icon-ksd-table_edit ksd-ml-5" v-if="scope.row.source === 'MANUAL_TABLE'" @click="confrimEditTableIndex(scope.row)"></i>
                  </common-tip>
                  <!-- <common-tip :content="$t('kylinLang.common.moreActions')">
                    <el-dropdown @command="(command) => {handleCommand(command, scope.row)}" trigger="click" >
                      <span class="el-dropdown-link" >
                          <i class="el-icon-ksd-table_others ksd-ml-5 ksd-fs-14"></i>
                      </span>
                      <el-dropdown-menu slot="dropdown">
                        <el-dropdown-item command="edit" v-if="datasourceActions.includes('editAggGroup')&&scope.row.source === 'MANUAL_TABLE'">{{$t('kylinLang.common.edit')}}</el-dropdown-item>
                        <el-dropdown-item command="delete" v-if="datasourceActions.includes('delAggIdx')">{{$t('kylinLang.common.delete')}}</el-dropdown-item>
                      </el-dropdown-menu>
                    </el-dropdown>
                  </common-tip> -->
                  <!-- <common-tip :content="$t('delIndex')" v-if="datasourceActions.includes('delAggIdx')">
                    <i class="el-icon-ksd-table_delete ksd-ml-5" @click="removeIndex(scope.row)"></i>
                  </common-tip> -->
                </template>
              </el-table-column>
            </el-table>
            <kap-pager class="ksd-center ksd-mtb-10" ref="indexPager" :totalSize="totalSize" :curPage="filterArgs.page_offset+1" v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
          </div>
        </el-card>
        <recommendations :modelDesc="model" @accept="acceptRecommend" v-else/>
      </div>
    </div>

    <el-dialog class="lincense-result-box"
      :title="indexDetailTitle"
      width="480px"
      :limited-area="true"
      :append-to-body="true"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="resetDetail"
      :visible.sync="indexDetailShow">
      <div class="ksd-mb-10 ksd-fs-12">{{$t('modifiedTime')}}: {{cuboidDetail.modifiedTime || showTableIndexDetail.modifiedTime}}</div>
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
      </el-table>
      <div v-else>
          <el-table
          size="small"
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
          width="70"
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
          <kap-pager layout="prev, pager, next" :background="false" class="ksd-mt-10 ksd-center" ref="pager" :perpage_size="currentCount" :curPage="currentPage+1" :totalSize="totalTableIndexColumnSize"  v-on:handleCurrentChange='currentChange'></kap-pager>
        </div>
      <div slot="footer" class="dialog-footer">
        <el-button plain size="medium" @click="indexDetailShow=false">{{$t('kylinLang.common.close')}}</el-button>
      </div>
    </el-dialog>

    <!-- <ConfirmSegment/> -->
    <!-- <TableIndexEdit/> -->
    <!-- <AggregateModal/> -->
    <!-- <AggAdvancedModal v-on:refreshIndexGraph="refreshIndexGraphAfterSubmitSetting" /> -->
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import FlowerChart from '../../../../common/FlowerChart'
import TreemapChart from '../../../../common/TreemapChart'
import { handleSuccessAsync } from '../../../../../util'
import { handleError, transToGmtTime, kapConfirm, transToServerGmtTime } from '../../../../../util/business'
import { speedProjectTypes } from '../../../../../config'
import { BuildIndexStatus } from '../../../../../config/model'
// import ConfirmSegment from '../ConfirmSegment/ConfirmSegment.vue'
// import AggregateModal from './AggregateModal/index.vue'
// import AggAdvancedModal from './AggAdvancedModal/index.vue'
// import TableIndexEdit from '../../TableIndexEdit/tableindex_edit'
import { formatGraphData } from './handler'
import NModel from '../../ModelEdit/model.js'
import Recommendations from './sub/recommendations'

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
      'isAutoProject',
      'datasourceActions',
      'isOnlyQueryNode'
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
    ...mapActions('ConfirmSegment', {
      callConfirmSegmentModal: 'CALL_MODAL'
    }),
    // ...mapActions('AggAdvancedModal', {
    //   callAggAdvancedModal: 'CALL_MODAL'
    // }),
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    }),
    ...mapActions({
      fetchIndexGraph: 'FETCH_INDEX_GRAPH',
      buildIndex: 'BUILD_INDEX',
      loadAllIndex: 'LOAD_ALL_INDEX',
      deleteIndex: 'DELETE_INDEX',
      deleteIndexes: 'DELETE_INDEXES',
      autoFixSegmentHoles: 'AUTO_FIX_SEGMENT_HOLES'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  components: {
    FlowerChart,
    TreemapChart,
    Recommendations
    // ConfirmSegment
    // AggregateModal
    // AggAdvancedModal,
    // TableIndexEdit
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
  indexLoading = false
  isLoading = false
  indexDatas = []
  dataRange = null
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
  filterTags = []
  checkedList = []
  removeLoading = false
  moveEvent = {
    w: 50,
    curW: 50,
    limit: {
      width: [10, 80]
    }
  }
  switchIndexValue = 0
  isHaveComplementSegs = false
  // 打开高级设置
  // openAggAdvancedModal () {
  //   this.callAggAdvancedModal({
  //     model: objectClone(this.model),
  //     aggIndexAdvancedDesc: null
  //   })
  // }

  async complementedIndexes (indexType, id) {
    let title = this.$t('buildIndex')
    let subTitle = this.$t('subTitle')
    let submitText = this.$t('buildIndex')
    let isRemoveIndex = false
    let indexes = []
    if (indexType === 'allIndexes') {
      title = this.$t('viewIncompleteTitle')
      subTitle = this.$t('incompleteSubTitle')
    } else if (indexType === 'batchIndexes') {
      title = this.$t('buildIndex')
      subTitle = this.$t('batchBuildSubTitle', {number: this.checkedList.length})
      indexes = this.checkedList.map((i) => {
        return i.id
      })
    } else if (indexType === 'deleteIndexes') {
      title = this.$t('deleteIndex')
      subTitle = this.$t('deleteTips', {number: this.checkedList.length})
      submitText = this.$t('kylinLang.common.delete')
      isRemoveIndex = true
      indexes = this.checkedList.map((i) => {
        return i.id
      })
    } else {
      indexes.push(id)
    }
    await this.callConfirmSegmentModal({
      title: title,
      subTitle: subTitle,
      indexes: indexes,
      submitText: submitText,
      isRemoveIndex: isRemoveIndex,
      model: this.model
    })
    this.refreshIndexGraphAfterSubmitSetting()
  }

  // handleCommand (command, row) {
  //   if (command === 'edit') {
  //     this.confrimEditTableIndex(row)
  //   } else if (command === 'delete') {
  //     this.removeIndexs(row.id)
  //   }
  // }

  handleSelectionChange (val) {
    this.checkedList = val
  }

  async removeIndexes () {
    if (!this.checkedList.length) return
    const layout_ids = this.checkedList.map((index) => {
      return index.id
    }).join(',')
    try {
      await kapConfirm(this.$t('delIndexesTips', {indexNum: this.checkedList.length}), {confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('delIndex'))
      this.removeLoading = true
      await this.deleteIndexes({project: this.projectName, model: this.model.uuid, layout_ids: layout_ids})
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.removeLoading = false
      this.refreshIndexGraphAfterSubmitSetting()
      this.$emit('loadModels')
    } catch (e) {
      handleError(e)
      this.removeLoading = false
    }
  }

  tableRowClassName ({row, rowIndex}) {
    if (row.status === 'EMPTY' || row.status === 'BUILDING') {
      return 'empty-index'
    }
    return ''
  }

  get emptyText () {
    return this.filterArgs.key || this.filterArgs.sources.length || this.filterArgs.status.length ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  get isShowIndexActions () {
    const { isShowEditAgg, isShowTableIndexActions, isHideEdit } = this

    return isShowEditAgg || (isShowTableIndexActions && !isHideEdit)
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
  confrimEditTableIndex (indexDesc) {
    if (this.$store.state.capacity.maintenance_mode || this.isOnlyQueryNode) {
      let msg = ''
      if (this.$store.state.capacity.maintenance_mode) {
        msg = this.$t('kylinLang.common.systemUpgradeTips')
      } else if (this.isOnlyQueryNode) {
        msg = this.$t('kylinLang.common.noAllNodeTips')
      }
      kapConfirm(msg, {cancelButtonText: this.$t('kylinLang.common.continueOperate'), confirmButtonText: this.$t('kylinLang.common.tryLater'), type: 'warning', showClose: false, closeOnClickModal: false, closeOnPressEscape: false}, this.$t('kylinLang.common.tip')).then().catch(async () => {
        this.editTableIndex(indexDesc)
      })
    } else {
      this.editTableIndex(indexDesc)
    }
  }
  editTableIndex (indexDesc) {
    this.showTableIndexEditModal({
      modelInstance: this.modelInstance,
      tableIndexDesc: indexDesc || {name: 'TableIndex_1'}
    }).then((res) => {
      if (res.isSubmit) {
        this.refreshIndexGraphAfterSubmitSetting()
        this.$emit('loadModels')
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
        <el-checkbox-group class="filter-groups" value={this.filterArgs.sources} onInput={val => (this.filterArgs.sources = val)} onChange={this.filterSouces}>
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
        <el-checkbox-group class="filter-groups" value={this.filterArgs.status} onInput={val => (this.filterArgs.status = val)} onChange={this.filterSouces}>
          {items}
        </el-checkbox-group>
        <i class={this.filterArgs.status.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  async buildAggIndex () {
    if (this.model.segment_holes.length) {
      const segmentHoles = this.model.segment_holes
      try {
        const tableData = []
        let selectSegmentHoles = []
        segmentHoles.forEach((seg) => {
          const obj = {}
          obj['start'] = transToServerGmtTime(seg.date_range_start)
          obj['end'] = transToServerGmtTime(seg.date_range_end)
          obj['date_range_start'] = seg.date_range_start
          obj['date_range_end'] = seg.date_range_end
          tableData.push(obj)
        })
        await this.callGlobalDetailDialog({
          msg: this.$t('segmentHoletips', {modelName: this.model.name}),
          title: this.$t('fixSegmentTitle'),
          detailTableData: tableData,
          detailColumns: [
            {column: 'start', label: this.$t('kylinLang.common.startTime')},
            {column: 'end', label: this.$t('kylinLang.common.endTime')}
          ],
          isShowSelection: true,
          dialogType: 'warning',
          showDetailBtn: false,
          needResolveCancel: true,
          cancelText: this.$t('ignore'),
          submitText: this.$t('fixAndBuild'),
          customCallback: async (segments) => {
            selectSegmentHoles = segments.map((seg) => {
              return {start: seg.date_range_start, end: seg.date_range_end}
            })
            await this.autoFixSegmentHoles({project: this.projectName, model_id: this.model.uuid, segment_holes: selectSegmentHoles})
            this.confirmBuild()
          }
        })
        this.confirmBuild()
      } catch (e) {
        e !== 'cancel' && handleError(e)
      }
    } else {
      await kapConfirm(this.$t('bulidTips', {modelName: this.model.name}), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: this.$t('buildIndex'), type: 'warning'})
      this.confirmBuild()
    }
  }
  async confirmBuild () {
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
    let idStr = (row.id !== undefined) && (row.id !== null) && (row.id !== '') ? ' [' + row.id + ']' : ''
    this.detailType = row.source.indexOf('AGG') >= 0 ? 'aggDetail' : 'tabelIndexDetail'
    this.indexDetailTitle = row.source.indexOf('AGG') >= 0 ? this.$t('aggDetailTitle') + idStr : this.$t('tabelDetailTitle') + idStr
    this.indexDetailShow = true
  }
  async removeIndex (row) {
    try {
      await kapConfirm(this.$t('delIndexTip'), null, this.$t('delIndex'))
      await this.deleteIndex({project: this.projectName, model: this.model.uuid, id: row.id})
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.refreshIndexGraphAfterSubmitSetting()
      this.$emit('loadModels')
    } catch (e) {
      handleError(e)
    }
  }
  onSortChange ({ column, prop, order }) {
    this.filterArgs.sort_by = prop
    this.filterArgs.reverse = !(order === 'ascending')
    this.pageCurrentChange(0, this.filterArgs.page_size)
  }
  async pageCurrentChange (size, count) {
    this.filterArgs.page_offset = size
    this.filterArgs.page_size = count
    this.indexLoading = true
    await this.loadAggIndices()
    this.indexLoading = false
  }
  async filterSouces () {
    this.filterArgs.page_offset = 0
    this.indexLoading = true
    await this.loadAggIndices()
    this.indexLoading = false
  }
  async searchAggs () {
    this.filterArgs.page_offset = 0
    this.indexLoading = true
    await this.loadAggIndices()
    this.indexLoading = false
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
    this.switchIndexValue = 0
    this.filterArgs.key = id
    this.indexLoading = true
    await this.loadAggIndices()
    this.indexLoading = false
  }
  async freshIndexGraph () {
    try {
      const res = await this.fetchIndexGraph({
        project: this.projectName,
        model: this.model.uuid
      })
      const data = await handleSuccessAsync(res)
      this.dataRange = [data.start_time, data.end_time]
      this.isHaveComplementSegs = data.segment_to_complement_count > 0
      this.isFullLoaded = data.is_full_loaded
      this.cuboids = formatGraphData(data)
      this.cuboidCount = data.total_indexes
      this.emptyCuboidCount = data.empty_indexes
    } catch (e) {
      handleError(e)
    }
  }
  get getDataRange () {
    if (this.dataRange) {
      if (this.isFullLoaded && this.dataRange[0] === 0 && this.dataRange[1] === 9223372036854776000) {
        return this.$t('kylinLang.dataSource.full')
      } else if (!this.dataRange[0] && !this.dataRange[1]) {
        return this.$t('noDataRange')
      } else {
        return transToServerGmtTime(this.dataRange[0]) + this.$t('to') + transToServerGmtTime(this.dataRange[1])
      }
    } else {
      return ''
    }
    // return this.dataRange ? transToServerGmtTime(this.dataRange[0]) + this.$t('to') + transToServerGmtTime(this.dataRange[1]) : ''
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
      // this.indexLoading = true
      const res = await this.loadAllIndex(Object.assign({
        project: this.projectName,
        model: this.model.uuid
      }, this.filterArgs))
      const data = await handleSuccessAsync(res)
      this.indexDatas = data.value
      this.totalSize = data.total_size
      // this.indexLoading = false
    } catch (e) {
      handleError(e)
      // this.indexLoading = false
    }
  }
  async mounted () {
    this.isLoading = true
    await this.freshIndexGraph()
    await this.loadAggIndices()
    this.isLoading = false
  }
  async refreshIndexGraphAfterSubmitSetting () {
    this.isLoading = true
    await this.freshIndexGraph()
    await this.loadAggIndices()
    this.isLoading = false
  }
  async handleAggregateGroup () {
    if (this.$store.state.capacity.maintenance_mode || this.isOnlyQueryNode) {
      let msg = ''
      if (this.$store.state.capacity.maintenance_mode) {
        msg = this.$t('kylinLang.common.systemUpgradeTips')
      } else if (this.isOnlyQueryNode) {
        msg = this.$t('kylinLang.common.noAllNodeTips')
      }
      kapConfirm(msg, {cancelButtonText: this.$t('kylinLang.common.continueOperate'), confirmButtonText: this.$t('kylinLang.common.tryLater'), type: 'warning', showClose: false, closeOnClickModal: false, closeOnPressEscape: false}, this.$t('kylinLang.common.tip')).then().catch(async () => {
        const { projectName, model } = this
        const isSubmit = await this.callAggregateModal({ editType: 'new', model, projectName })
        isSubmit && await this.refreshIndexGraphAfterSubmitSetting()
        isSubmit && await this.$emit('loadModels')
      })
    } else {
      const { projectName, model } = this
      const isSubmit = await this.callAggregateModal({ editType: 'new', model, projectName })
      isSubmit && await this.refreshIndexGraphAfterSubmitSetting()
      isSubmit && await this.$emit('loadModels')
    }
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      sources: 'source',
      status: 'kylinLang.common.status'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        this.filterTags.push({label: item, source: maps[type], key: type})
      }
    })
    this.filterArgs[type] = val
    this.filterSouces()
  }
  // 删除单个筛选条件
  handleClose (tag) {
    const index = this.filterArgs[tag.key].indexOf(tag.label)
    index > -1 && this.filterArgs[tag.key].splice(index, 1)
    this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    this.filterSouces()
  }
  // 清除所有筛选条件
  clearAllTags () {
    this.filterArgs.sources.splice(0, this.filterArgs.sources.length)
    this.filterArgs.status.splice(0, this.filterArgs.status.length)
    this.filterArgs.page_offset = 0
    this.filterTags = []
    this.filterSouces()
  }

  handlerDownEvent (e) {
    this.moveEvent.downX = e.clientX
    document.addEventListener('mousemove', this.handlerMoveEvent)
    document.addEventListener('mouseup', this.handlerUpEvent)
  }

  handlerMoveEvent (e) {
    let tarX = e.clientX
    let viewParent = this.$el.querySelector('.aggregate-view').getBoundingClientRect()
    this.moveEvent.w = (this.moveEvent.curW / 100 * viewParent.width + tarX - this.moveEvent.downX) / viewParent.width * 100
    if (this.moveEvent.limit.width.length) {
      const [min, max] = this.moveEvent.limit.width
      min && this.moveEvent.w < min && (this.moveEvent.w = min)
      max && this.moveEvent.w > max && (this.moveEvent.w = max)
    }
    this.$refs.indexTreeMap && this.$refs.indexTreeMap.myChart.resize()
  }

  handlerUpEvent () {
    this.moveEvent.curW = this.moveEvent.w
    document.removeEventListener('mousemove', this.handlerMoveEvent)
    document.removeEventListener('mouseup', this.handlerUpEvent)
  }

  // 优化建议通过后刷新索引列表
  async acceptRecommend () {
    await this.loadAggIndices()
    this.model.total_indexes = this.totalSize
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';

.model-aggregate {
  .aggregate-view {
    background-color: @fff;
    // padding: 10px;
    border: 1px solid @line-border-color4;
    // display: flex;
    .aggregate-tree-map {
      position: relative;
      display: inline-block;
      vertical-align: top;
    }
    .index-group {
      padding: 15px 20px;
      box-sizing: border-box;
      position: relative;
      display: inline-block;
      vertical-align: top;
      .btn-groups {
        position: absolute;
        right: 20px;
        .active {
          color: @text-normal-color;
          background:rgba(244,244,244,1);
          border:1px solid rgba(204,204,204,1);
          box-shadow: inset 1px 1px 2px 0 @line-border-color3;
        }
      }
    }
  }
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
      top: 2px;
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
    * {
      vertical-align: middle;
    }
    .divide {
      border-left: 1px solid @line-border-color;
      margin: 0 5px;
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
  .drag-bar {
    right: 0;
    top: 50%;
    transform: translate(50%, -50%);
    cursor: ew-resize;
  }
  .agg-detail-card {
    // height: 496px;
    border: none;
    background: none;
    .el-card__header {
      background: none;
      border-bottom: none;
      height: 24px;
      font-size: 14px;
      padding: 0px;
      margin-bottom: 5px;
    }
    &.agg_index {
      border-right: 1px solid @line-border-color;
      padding-right: 20px;
      padding: 15px 20px;
      .el-card__body {
        overflow: hidden;
      }
    }
    // &.agg-detail {
    //   .el-card__header {
    //     padding-top:6px;
    //     padding-bottom:6px;
    //   }
    // }
    .el-card__body {
      overflow: auto;
      height: 460px;
      width: 100%;
      position: relative;
      box-sizing: border-box;
      padding: 0px !important;
      .detail-content {
        .text-btn-like {
          color: @base-color;
          cursor: pointer;
          font-size: 12px;
        }
        .el-row {
          margin-bottom: 10px;
          .dim-item {
            margin-bottom: 5px;
          }
        }
        .split-button {
          &.is-disabled {
            .el-button-group > .el-button {
              background-color: @background-disabled-color;
              color: @text-disabled-color;
              cursor: not-allowed;
              background-image: none;
              border-color: @line-border-color3;
            }
          }
        }
      }
    }
    .left {
      display: block;
      float: left;
      position: relative;
      // top: 8px;
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
  .filter-tags-agg {
    margin-bottom: 10px;
    padding: 0 5px 4px 3px;
    box-sizing: border-box;
    position: relative;
    font-size: 12px;
    background: @background-disabled-color;
    .filter-tags-layout {
      max-width: calc(~'100% - 80px');
      display: inline-block;
      // line-height: 30px;
    }
    .el-tag {
      margin-left: 5px;
      margin-top: 4px;
    }
    .clear-all-filters {
      position: absolute;
      top: 5px;
      right: 8px;
      font-size: 12px;
      color: @base-color;
      cursor: pointer;
    }
  }
  .cell.highlight {
    .el-icon-ksd-filter {
      color: @base-color;
    }
  }
  .el-icon-ksd-filter {
    position: relative;
    font-size: 17px;
    top: 2px;
    left: 5px;
    &:hover,
    &.filter-open {
      color: @base-color;
    }
  }
}
</style>
