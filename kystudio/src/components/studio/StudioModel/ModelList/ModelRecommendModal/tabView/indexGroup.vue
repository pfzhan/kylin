<template>
  <div>
    <!-- 实际信息展示区 -->
    <el-table
      key="indexList"
      ref="indexList"
      class="index-table"
      border
      :empty-text="emptyText"
      :data="tableShowList"
      style="width: 100%"
      @select-all="selectionAllChange"
      @select="selectionChange"
      :expand-row-keys="expandRows"
      :row-key="rowKey"
      @expand-change="expandChange">
      <el-table-column
        type="selection"
        width="44">
      </el-table-column>
      <el-table-column width="44" type="expand">
        <template slot-scope="scope">
          <div v-loading="curRowExpandLoading[scope.row.id]" v-if="!scope.row.type.split('_').includes('TABLE')">
            <!-- 表中表搜索区 -->
            <p class="clearfix">
            <el-input
                class="ksd-fright"
                :placeholder="$t('kylinLang.common.pleaseFilter')"
                prefix-icon="el-ksd-icon-search_22"
                v-model.trim="curTableInTableKeyword[scope.row.item_id]"
                @keyup.native="handleTableInTableFilter(scope.row)"
                @clear="handleTableInTableFilter(scope.row)"
                style="width:300px">
            </el-input>
            </p>
            <el-table
              size="small"
              class="ksd-mt-10"
              :data="scope.row.contentShow"
              nested
              border
              style="width: 100%">
            <el-table-column
                label="Order"
                type="index"
                width="80">
            </el-table-column>
            <el-table-column
                label="Content"
                prop="content">
            </el-table-column>
            </el-table>
            <!-- 表中表分页 -->
            <kap-pager
            :key="scope.row.id"
            :refTag="pageRefTags.indexGroupPager"
            class="ksd-center ksd-mt-10" ref="pager"
            :per-page-size="perPageSize"
            :totalSize="scope.row.size"
            :curPage="currentPage+1"
            :layout="'prev, pager, next'"
            :background="false"
            @handleCurrentChange="(currentPage) => { handleCurrentChange(currentPage, scope.row) }">
            </kap-pager>
          </div>
          <div v-loading="curRowExpandLoading[scope.row.id]" v-else>
            <!-- 表中表搜索区 -->
            <p class="clearfix">
                <el-input
                class="ksd-fright"
                :placeholder="$t('kylinLang.common.pleaseFilter')"
                prefix-icon="el-ksd-icon-search_22"
                v-model.trim="curTableInTableKeyword[scope.row.item_id]"
                @keyup.native="handleTableInTableFilter(scope.row)"
                @clear="handleTableInTableFilter(scope.row)"
                style="width:300px">
                </el-input>
            </p>
            <el-table
                size="small"
                class="ksd-mt-10"
                :data="scope.row.contentShow"
                nested
                border
                style="width: 100%">
                <el-table-column
                label="Order"
                type="index"
                width="80">
                </el-table-column>
                <el-table-column
                label="Column"
                prop="column">
                </el-table-column>
                <el-table-column
                label="Shard"
                prop="shared"
                width="80"
                align="center">
                <template slot-scope="scope">
                    <i class="el-icon-ksd-good_health ky-success ksd-fs-16" v-if="scope.row.shared"></i>
                </template>
                </el-table-column>
            </el-table>
            <!-- 表中表分页 -->
            <kap-pager
                class="ksd-center ksd-mt-10" ref="pager"
                :refTag="pageRefTags.indexGroupContentShowPager"
                :per-page-size="perPageSize"
                :totalSize="scope.row.size"
                :curPage="currentPage+1"
                :layout="'prev, pager, next'"
                :background="false"
                @handleCurrentChange="(currentPage) => { handleCurrentChange(currentPage, scope.row) }">
            </kap-pager>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        width="160"
        :label="$t('th_recommendType')"
        :filters="typeList.map(item => ({text: $t(item), value: item}))"
        :filtered-value="checkedStatus"
        filter-icon="el-icon-ksd-filter"
        :show-multiple-footer="false"
        :filter-change="(v) => filterType(v, 'checkedStatus')"
        prop="type"
        show-overflow-tooltip>
        <template slot-scope="scope">
          {{$t(scope.row.type)}}
        </template>
      </el-table-column>
      <el-table-column
        width="120"
        prop="id"
        label="Index ID">
        <template slot-scope="scope">
          <span v-if="scope.row.type !== 'ADD_AGG' && scope.row.type !== 'ADD_TABLE'">{{$t(scope.row.id)}}</span>
        </template>
      </el-table-column>
      <el-table-column
        width="110"
        :label="$t('th_source')"
        :filters="source.map(item => ({text: $t(item), value: item}))"
        :filtered-value="sourceCheckedStatus"
        filter-icon="el-icon-ksd-filter"
        :show-multiple-footer="false"
        :filter-change="(v) => filterType(v, 'sourceCheckedStatus')">
        <template slot-scope="scope">
          {{$t(scope.row.source)}}
        </template>
      </el-table-column>
      <el-table-column
        width="110"
        prop="data_size"
        :label="$t('th_dataSize')"
        sortable>
        <template slot-scope="scope">
          {{scope.row.data_size || 0 | dataSize}}
        </template>
      </el-table-column>
      <el-table-column
        width="105"
        prop="usage"
        :label="$t('th_useCount')"
        sortable>
      </el-table-column>
      <el-table-column
        width="175"
        prop="create_time"
        :label="$t('th_updateDate')"
        sortable
        show-overflow-tooltip>
        <template slot-scope="scope">
          {{transToGmtTime(scope.row.create_time)}}
        </template>
      </el-table-column>
      <el-table-column
        :width="noteWidth"
        :label="$t('th_note')">
        <div slot-scope="scope" class="col-tab-note">
          <template v-if="('remove_reason' in scope.row.info)">
            <el-tooltip class="item" effect="dark" :content="removeReasonTip(scope)" placement="top">
              <el-tag class="th-note-tag" size="small" type="warning">{{$t(scope.row.info.remove_reason)}}</el-tag>
            </el-tooltip>
          </template>
        </div> 
      </el-table-column>
    </el-table>
  </div>
</template>
<script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import { renderTableColumnSelected } from '../handler'
  import { pageRefTags, pageCount } from 'config'
  // import vuex from '../../../../store'
  import { handleError, transToGmtTime } from 'util/business'
  import { handleSuccessAsync } from 'util'
  import { objectClone } from 'util/index'
  import locales from '../locales'

  @Component({
    props: {
      list: {
        type: Array,
        default: () => []
      },
      modelDesc: {
        type: Object,
        default: null
      }
    },
    computed: {
      ...mapGetters([
        'currentSelectedProject',
        'currentProjectData'
      ]),
      ...mapState({
      })
    },
    methods: {
      transToGmtTime: transToGmtTime,
      ...mapActions({
        getIndexContentList: 'GET_INDEX_CONTENTLIST',
        fetchProjectSettings: 'FETCH_PROJECT_SETTINGS'
      }),
      ...mapMutations({
      }),
      rowKey (row) {
        return row.item_id
      }
    },
    locales
  })

  export default class IndexGroup extends Vue {
    pageRefTags = pageRefTags
    curTableInTableKeyword = {}
    checkedStatus = []
    sourceCheckedStatus = []
    tableShowList = []
    ST = null
    expandRows = []
    curRowExpandLoading = {}
    perPageSize = +localStorage.getItem(this.pageRefTags.indexGroupPager) || pageCount
    currentPage = 0
    isShow = true
    lowFrequency = {
      frequency_time_window: '',
      low_frequency_threshold: 0
    }
    typeList = ['ADD_AGG', 'REMOVE_AGG', 'ADD_TABLE', 'REMOVE_TABLE']
    source = ['imported', 'query_history']

    get noteWidth () {
      const len = []
      this.list.forEach(item => 'remove_reason' in item.info && (Array.isArray(item.info.remove_reason.length) ? len.push(item.info.remove_reason.length) : 1))
      return len.length ? (Math.max.apply(null, len) > 3 ? 3 * 130 : Math.max.apply(null, len) * 130) : 130
    }

    get emptyText () {
      return this.sourceCheckedStatus.length || this.checkedStatus.length ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
    }

    created () {
      this.projectSetting()
    }

    mounted () {
      this.tableShowList = this.list
      renderTableColumnSelected(this.tableShowList, this, 'indexList')
    }

    // 获取低效配置的相关数据
    async projectSetting () {
      try {
        const response = await this.fetchProjectSettings({projectName: this.currentProjectData.name})
        const result = await handleSuccessAsync(response)
        this.lowFrequency = {...this.lowFrequency, ...{frequency_time_window: result.frequency_time_window, low_frequency_threshold: result.low_frequency_threshold}}
      } catch (e) {
        handleError(e)
      }
    }

    async loadIndexList (row, page) {
      try {
        this.curRowExpandLoading[row.id] = true
        let params = {
          project: this.currentSelectedProject,
          model: this.modelDesc.uuid,
          item_id: row.item_id,
          page_offset: page.curpage, // 每次展开默认在第一页
          page_size: this.perPageSize,
          content: this.curTableInTableKeyword[row.item_id]
        }
        const response = await this.getIndexContentList(params)
        const result = await handleSuccessAsync(response)
        row.size = result.size
        if (row.type.split('_').includes('TABLE')) {
          row.contentShow = result.columns_and_measures.map(res => ({column: res, shared: result.shard_by_columns.includes(res)}))
        } else {
          row.contentShow = result.columns_and_measures.map(re => ({content: re}))
        }
        this.curRowExpandLoading[row.id] = false
      } catch (e) {
        row.contentShow = []
        row.size = 0
        this.curRowExpandLoading[row.id] = false
        handleError(e)
      }
    }

    expandChange (row, expandedRows) {
      let inExpand = expandedRows.filter((item) => {
        return item.item_id === row.item_id
      })
      if (inExpand.length > 0) { // 说明是展开，需要发接口
        this.loadIndexList(row, {curpage: 0})
      }
    }

    // 通知外部，选中/取消选中所有的行，更变list的isSelect的值
    selectionAllChange (selection) {
      let selectAllType = selection.length > 0 ? 'all' : 'null'
      this.$emit('indexSelectedChange', {type: 'index', selectType: selectAllType})
    }

    // 单元行的选中/取消
    selectionChange (selection, row) {
      row.isSelected = !row.isSelected
      this.$emit('indexSelectedChange', {type: 'index', selectType: 'single', data: row})
    }

    handleTableInTableFilter (row) {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.currentPage = 0
        this.loadIndexList(row, {curpage: 0})
      }, 100)
    }

    handleCurrentChange (currentPage, row) {
      this.currentPage = currentPage
      this.loadIndexList(row, {curpage: currentPage})
    }

    // 筛选建议类型和来源
    filterType (v, type) {
      this[type] = v
      if (this.sourceCheckedStatus.length && this.checkedStatus.length) {
        this.tableShowList = this.list.filter(item => (item.source && this.sourceCheckedStatus.includes(item.source)) && (item.type && this.checkedStatus.includes(item.type)))
      } else if (this.sourceCheckedStatus.length || this.checkedStatus.length) {
        this.tableShowList = this.list.filter(item => (item.source && this.sourceCheckedStatus.includes(item.source)) || (item.type && this.checkedStatus.includes(item.type)))
      } else {
        this.tableShowList = objectClone(this.list)
      }
      renderTableColumnSelected(this.tableShowList, this, 'indexList')
    }

    // 删除索引备注hover提示
    removeReasonTip (data) {
      const timeMap = {
        'MONTH': {'zh-cn': '一个月', 'en': 'month'},
        'DAY': {'zh-cn': '一天', 'en': 'day'},
        'WEEK': {'zh-cn': '一周', 'en': 'week'}
      }
      const reason = {
        'LOW_FREQUENCY': this.$t('usage_time_tip', {date: this.lowFrequency.frequency_time_window && timeMap[this.lowFrequency.frequency_time_window][this.$store.state.system.lang], time: this.lowFrequency.low_frequency_threshold}),
        'INCLUDED': this.$t('exist_index_tip'),
        'SIMILAR': this.$t('similar_index_tip')
      }
      return reason[data.row.info.remove_reason]
    }
  }
</script>
<style lang="less">
  @import '../../../../../../assets/styles/variables.less';
  .index-table {
    td, th {
      height: 36px;
      padding: 0;
    }
    .el-icon-ksd-filter {
      &:hover {
        color: @base-color;
      }
      &.isFilter {
        color: @base-color;
      }
    }
    .cell {
      .col-tab-note {
        padding-top: 2px;
        margin-left: -5px;
        line-height: 1;
      }
      .th-note-tag {
        max-width: 100px;
        text-overflow: ellipsis;
        overflow: hidden;
        margin-left: 5px;
      }
    }
  }
</style>
