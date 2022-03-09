<template>
  <div>
    <!-- 实际信息展示区 -->
    <el-table
      key="aggIndexList"
      ref="aggIndexList"
      border
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
          <div v-loading="curRowExpandLoading[scope.row.id]">
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
              size="medium"
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
              :refTag="pageRefTags.addIndexPager"
              class="ksd-center ksd-mt-10" ref="pager"
              :per-page-size="perPageSize"
              :totalSize="scope.row.size"
              :layout="'prev, pager, next'"
              :background="false"
              @handleCurrentChange="(currentPage) => { handleCurrentChange(currentPage, scope.row) }">
            </kap-pager>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        width="100"
        :renderHeader="renderRecommendType"
        prop="recommendation_type">
        <template slot-scope="scope">
          {{$t(scope.row.recommendation_type)}}
        </template>
      </el-table-column>
      <el-table-column
        prop="id"
        label="Index ID">
      </el-table-column>
      <el-table-column
        prop="storage_size"
        :label="$t('th_dataSize')"
        sortable>
        <template slot-scope="scope">
          {{scope.row.storage_size | dataSize}}
        </template>
      </el-table-column>
      <el-table-column
        prop="query_hit_count"
        :label="$t('th_useCount')"
        sortable>
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
  import { handleError } from 'util/business'
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
        'currentSelectedProject'
      ]),
      ...mapState({
      })
    },
    methods: {
      ...mapActions({
        getAggIndex: 'GET_AGG_INDEX_CONTENTLIST'
      }),
      ...mapMutations({
      }),
      rowKey (row) {
        return row.item_id
      }
    },
    locales
  })
  export default class recomAggIndex extends Vue {
    pageRefTags = pageRefTags
    curTableInTableKeyword = {}
    checkedStatus = []
    tableShowList = []
    ST = null
    expandRows = []
    curRowExpandLoading = {}
    perPageSize = +localStorage.getItem(this.pageRefTags.addIndexPager) || pageCount

    /* @Watch('list', { deep: true })
    onListChange (val) {
      // this.tableShowList = objectClone(val)
      this.tableShowList = val
      renderTableColumnSelected(this.tableShowList, this, 'aggIndexList')
    } */

    mounted () {
      this.tableShowList = this.list
      renderTableColumnSelected(this.tableShowList, this, 'aggIndexList')
    }

    async loadAggIndexList (row, page) {
      try {
        this.curRowExpandLoading[row.id] = true
        let params = {
          project: this.currentSelectedProject,
          model: this.modelDesc.uuid,
          id: row.id,
          page_offset: page.curpage, // 每次展开默认在第一页
          page_size: this.perPageSize,
          content: this.curTableInTableKeyword[row.item_id]
        }
        const response = await this.getAggIndex(params)
        const result = await handleSuccessAsync(response)
        row.contentShow = result.content.map((re) => {
          return {content: re}
        })
        row.size = result.size
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
        this.loadAggIndexList(row, {curpage: 0})
      }
    }

    // 通知外部，选中/取消选中所有的行，更变list的isSelect的值
    selectionAllChange (selection) {
      let selectAllType = selection.length > 0 ? 'all' : 'null'
      this.$emit('aggIndexSelectedChange', {type: 'agg_index', selectType: selectAllType})
    }

    // 单元行的选中/取消
    selectionChange (selection, row) {
      row.isSelected = !row.isSelected
      this.$emit('aggIndexSelectedChange', {type: 'agg_index', selectType: 'single', data: row})
    }

    filterFav () { // 筛选建议类型
      if (this.checkedStatus.length === 0) {
        this.tableShowList = objectClone(this.list)
      } else {
        this.tableShowList = this.list.filter((item) => {
          return this.checkedStatus.indexOf(item.recommendation_type) > -1
        })
      }
      renderTableColumnSelected(this.tableShowList, this, 'aggIndexList')
    }

    handleTableInTableFilter (row) {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.loadAggIndexList(row, {curpage: 0})
      }, 100)
    }

    handleCurrentChange (currentPage, row) {
      this.loadAggIndexList(row, {curpage: currentPage})
    }

    renderRecommendType (h) {
      let typeList = ['ADDITION', 'REMOVAL', 'MODIFICATION']
      let items = []
      for (let i = 0; i < typeList.length; i++) {
        items.push(<el-checkbox label={typeList[i]} key={typeList[i]}><span class="ksd-fs-12">{this.$t(typeList[i])}</span></el-checkbox>)
      }
      return (<span>
        <span>{this.$t('th_recommendType')}</span>
        <el-popover
          ref="ipFilterPopover"
          placement="bottom"
          popperClass="filter-popover">
          <el-checkbox-group class="filter-groups" value={this.checkedStatus} onInput={val => (this.checkedStatus = val)} onChange={this.filterFav}>
            {items}
          </el-checkbox-group>
          <i class={this.checkedStatus.length > 0 && this.checkedStatus.length < 3 ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
        </el-popover>
      </span>)
    }
  }
</script>
<style lang="less">
  @import '../../../../../../assets/styles/variables.less';
</style>
