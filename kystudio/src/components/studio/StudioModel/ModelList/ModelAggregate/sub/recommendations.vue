<template>
  <el-card class="recommendations-card">
    <div slot="header">
      <p>{{$t('recommendations')}}</p>
    </div>
    <div class="detail-content">
      <p class="title-tip">{{$t('recommendationsTip1')}}<span v-if="$lang !== 'en'&&datasourceActions.includes('acceRuleSettingActions')">{{$t('recommendationsTip2')}}</span><a href="javascript:void();" v-if="datasourceActions.includes('acceRuleSettingActions')" @click="jumpToSetting">{{$t('modifyRules')}}</a><span v-if="$lang === 'en' && datasourceActions.includes('acceRuleSettingActions')">{{$t('recommendationsTip2')}}</span></p>
      <div class="ksd-mb-10 ksd-mt-10 ksd-fs-12" >
        <el-button size="mini" :disabled="!selectedList.length" type="primary" @click="betchAccept" icon="el-icon-ksd-accept">{{$t('accept')}}</el-button><el-button plain size="mini" :disabled="!selectedList.length" @click="betchDelete" icon="el-icon-ksd-table_delete">{{$t('delete')}}</el-button>
      </div>
      <el-table
        nested
        border
        :data="recommendationsList.list"
        class="recommendations-table"
        size="medium"
        max-height="350"
        v-loading="loadingRecommends"
        :empty-text="emptyText"
        @selection-change="handleSelectionChange"
        @sort-change="changeSort"
      >
        <el-table-column type="selection" width="44"></el-table-column>
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
            <el-tag size="mini" :type="scope.row.type.split('_')[0] === 'ADD' ? 'success' : 'danger'" v-if="['ADD', 'REMOVE'].includes(scope.row.type.split('_')[0])">{{scope.row.type.split('_')[0] === 'ADD' ? $t('newAdd') : $t('delete')}}</el-tag>
            {{$t(scope.row.type.split('_')[1])}}
          </template>
        </el-table-column>
        <el-table-column
          width="120"
          prop="id"
          label="Index ID">
          <template slot-scope="scope">
            <span v-if="scope.row.type !== 'ADD_AGG_INDEX' && scope.row.type !== 'ADD_TABLE_INDEX'">{{$t(scope.row.index_id)}}</span>
          </template>
        </el-table-column>
        <!-- <el-table-column
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
        </el-table-column> -->
        <el-table-column
          width="110"
          prop="data_size"
          :label="$t('th_dataSize')"
          sortable>
          <template slot-scope="scope">
            {{formatDataSize(scope.row.data_size)}}
          </template>
        </el-table-column>
        <el-table-column
          width="120"
          prop="hit_count"
          :label="$t('th_useCount')"
          :render-header="renderHeaderCol"
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
          :label="$t('th_note')">
          <div slot-scope="scope" class="col-tab-note">
            <template v-if="'recommendation_source' in scope.row.memo_info">
              <el-tooltip class="item" effect="dark" :content="removeReasonTip(scope)" placement="top">
                <el-tag class="th-note-tag" size="small" type="warning">{{$t(scope.row.memo_info.recommendation_source)}}</el-tag>
              </el-tooltip>
            </template>
          </div>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.action')" width="83" fixed="right">
          <template slot-scope="scope">
            <common-tip :content="$t('viewDetail')">
              <i class="el-icon-ksd-desc" @click="showDetail(scope.row)"></i>
            </common-tip>
            <common-tip :content="$t('accept')">
              <i class="el-icon-ksd-accept ksd-ml-5" @click="confrim([scope.row])"></i>
            </common-tip>
            <common-tip :content="$t('delete')">
              <i class="el-icon-ksd-table_delete ksd-ml-5" @click="removeIndex(scope.row)"></i>
            </common-tip>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager class="ksd-center ksd-mtb-10" ref="indexPager" :totalSize="recommendationsList.totalSize" :refTag="pageRefTags.recommendationsPager" :perPageSize="recommendationsList.page_size" :curPage="recommendationsList.page_offset+1" v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    </div>
    <!-- 索引详情 -->
    <el-dialog
      class="layout-details"
      :title="indexDetailTitle"
      width="480px"
      :append-to-body="true"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="showIndexDetail = false"
      :visible="true"
      v-if="showIndexDetail"
    >
      <el-table
        border
        v-loading="loadingDetails"
        :data="detailData"
        class="index-details-table"
        size="medium"
        :fit="false"
        :empty-text="emptyText"
        style="width: 100%"
        :cell-class-name="getCellClassName"
      >
        <el-table-column width="34" type="expand">
          <template slot-scope="scope" v-if="scope.row.content">
            <template v-if="scope.row.type === 'cc'">
              <p><span class="label">{{$t('th_expression')}}：</span>{{scope.row.content}}</p>
            </template>
            <template v-if="scope.row.type === 'dimension'">
              <p><span class="label">{{$t('th_column')}}：</span>{{JSON.parse(scope.row.content).column}}</p>
              <p><span class="label">{{$t('th_dataType')}}：</span>{{JSON.parse(scope.row.content).data_type}}</p>
            </template>
            <template v-if="scope.row.type === 'measure'">
              <p><span class="label">{{$t('th_column')}}：</span>{{JSON.parse(scope.row.content).name}}</p>
              <p><span class="label">{{$t('th_function')}}：</span>{{JSON.parse(scope.row.content).function.expression}}</p>
              <p><span class="label">{{$t('th_parameter')}}：</span>{{JSON.parse(scope.row.content).function.parameters}}</p>
            </template>
          </template>
        </el-table-column>
        <el-table-column type="index" :label="$t('order')" width="50"></el-table-column>
        <el-table-column :label="$t('th_name')" width="260">
          <template slot-scope="scope">
            <span class="column-name" :title="scope.row.name" :style="{width: scope.row.add ? 'calc(100% - 50px)' : '100%'}">{{scope.row.name}}</span>
            <el-tag class="add-tag" size="mini" type="success" v-if="scope.row.add">{{$t('newAdd')}}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="type" :label="$t('th_type')" width="95" show-overflow-tooltip>
          <template slot-scope="scope">
            {{$t(scope.row.type)}}
          </template>
        </el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain size="medium" @click="showIndexDetail = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" icon="el-icon-ksd-accept" @click="acceptLayout" :loading="accessLoading">{{$t('accept')}}</el-button>
      </div>
    </el-dialog>
    <!-- cc/度量/维度更名 -->
    <el-dialog
      class="layout-details"
      :title="$t('validateTitle')"
      width="480px"
      :append-to-body="true"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="showValidate = false"
      :visible="true"
      v-if="showValidate"
    >
      <p>{{$t('validateModalTip')}}</p>
      <el-table
        nested
        border
        :data="getValidateList"
        class="validate-table"
        size="medium"
        :empty-text="emptyText"
      >
        <el-table-column width="34" type="expand">
          <template slot-scope="scope">
            <template v-if="scope.row.type === 'cc'">
              <p><span class="label">{{$t('th_expression')}}：</span>{{scope.row.content}}</p>
            </template>
            <template v-if="scope.row.type === 'dimension'">
              <p><span class="label">{{$t('th_column')}}：</span>{{JSON.parse(scope.row.content).column}}</p>
              <p><span class="label">{{$t('th_dataType')}}：</span>{{JSON.parse(scope.row.content).data_type}}</p>
            </template>
            <template v-if="scope.row.type === 'measure'">
              <p><span class="label">{{$t('th_column')}}：</span>{{JSON.parse(scope.row.content).name}}</p>
              <p><span class="label">{{$t('th_function')}}：</span>{{JSON.parse(scope.row.content).function.expression}}</p>
              <p><span class="label">{{$t('th_parameter')}}：</span>{{JSON.parse(scope.row.content).function.parameters}}</p>
            </template>
          </template>
        </el-table-column>
        <el-table-column :label="$t('th_name')" width="300" show-overflow-tooltip>
          <template slot-scope="scope">
            <el-form :model="scope.row" :ref="`validateForm_${scope.row.item_id}`" :rules="scope.row.type !== 'cc' ? rules : rulesCC">
              <el-form-item prop="name">
                <el-tooltip class="item" effect="dark" :content="$t('usedInOtherModel')" placement="top" :disabled="scope.row.type === 'cc' ? !scope.row.cross_model : true">
                  <el-input :class="{'is-error': sameNameErrorStatus(scope.row.item_id)}" v-model="scope.row.name" size="mini" @change="changeInputContent" :disabled="scope.row.type === 'cc' ? scope.row.cross_model : false"></el-input>
                </el-tooltip>
                <p class="error-text" v-if="sameNameErrorStatus(scope.row.item_id)">{{$t('sameNameTips')}}</p>
              </el-form-item>
            </el-form>
          </template>
        </el-table-column>
        <el-table-column prop="type" :label="$t('th_type')" show-overflow-tooltip>
          <template slot-scope="scope">
            {{$t(scope.row.type)}}
          </template>
        </el-table-column>
      </el-table>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain size="medium" @click="showValidate = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" icon="" @click="addLayout" :loading="isLoading">{{$t('add')}}</el-button>
      </div>
    </el-dialog>
  </el-card>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { transToGmtTime, postCloudUrlMessage } from 'util/business'
import { mapActions, mapState, mapGetters } from 'vuex'
import { handleSuccessAsync, handleError, getQueryString, ArrayFlat } from '../../../../../../util'
import { pageRefTags, NamedRegex1, NamedRegex } from 'config'
import filterElements from '../../../../../../filter/index'

@Component({
  props: {
    modelDesc: {
      type: Object,
      default: () => {
        return {}
      }
    }
  },
  computed: {
    ...mapState({
      currentProject: state => state.project.selected_project
    }),
    ...mapGetters([
      'datasourceActions'
    ])
  },
  methods: {
    ...mapActions({
      getAllRecommendations: 'GET_ALL_RECOMMENDATIONS',
      deleteRecommendations: 'DELETE_RECOMMENDATIONS',
      accessRecommendations: 'ACCESS_RECOMMENDATIONS',
      getRecommendDetails: 'GET_RECOMMEND_DETAILS',
      validateRecommend: 'VALIDATE_RECOMMEND',
      refreshRecommendationCount: 'RECOMMENDATION_COUNT_REFRESH'
    }),
    ...mapActions('ConfirmSegment', {
      callConfirmSegmentModal: 'CALL_MODAL'
    })
  },
  locales: {
    'en': {
      recommendations: 'Recommendations',
      recommendationsTip1: 'Recommendations are generated by analyzing the query history and model usage.',
      recommendationsTip2: 'in project settings.',
      modifyRules: ' Modify rules ',
      odifyRules: 'Modify Rules ',
      th_recommendType: 'Type',
      th_name: 'Name',
      th_table: 'Table',
      th_column: 'Column',
      th_dataType: 'Data Type',
      th_function: 'Function',
      th_parameter: 'Function Parameter',
      th_expression: 'Expression',
      th_dataSize: 'Data Size',
      th_useCount: 'Usage',
      th_column_count: 'Column Totals',
      th_updateDate: 'Last Updated Time',
      th_note: 'Note',
      th_source: 'Source',
      th_type: 'Type',
      imported: 'Import',
      query_history: 'Query History',
      AGG: 'Aggregate Index',
      TABLE: 'Table Index',
      ADD_AGG_INDEX: 'Add Aggregate Index',
      REMOVE_AGG_INDEX: 'Delete Aggregate Index',
      ADD_TABLE_INDEX: 'Add Table Index',
      REMOVE_TABLE_INDEX: 'Delete Table Index',
      usage_time_tip: 'The usage of this index in the past {date} is lower than {time} times.',
      exist_index_tip: 'There already exists one index or more who could include this index.',
      similar_index_tip: 'There already exists one index or more who has a high similarity with this index.',
      imported_index_tip: 'Generated by the imported SQLs',
      query_history_tip: 'Generated by query history',
      LOW_FREQUENCY: 'Low Frequency',
      INCLUDED: 'Included by other indexes',
      SIMILAR: 'High Similarity',
      IMPORTED: 'Imported',
      QUERY_HISTORY: 'Query History',
      accept: 'Accept',
      delete: 'Delete',
      usedCountTip: 'For adding indexes, it means how many historical queries could be optimized;  for deleting indexes, it means how many times the index has been used.',
      viewDetail: 'Details',
      deleteRecommendTip: 'Selected recommendations would be permanently deleted. Do you want to continue?',
      deleteTitle: 'Delete Recommendations',
      deleteSuccess: 'Deleted successfully',
      aggDetailTitle: 'Aggregate Index Details',
      tableDetailTitle: 'Table Index Details',
      order: 'Order',
      cc: 'Computed Columns',
      dimension: 'Dimension',
      measure: 'Measure',
      newAdd: 'Add',
      validateTitle: 'Add Items to Model',
      validateModalTip: 'To accept the selected recommendations, the following items have to be added to the model:',
      add: 'Add',
      requiredName: 'Please enter alias',
      sameNameTips: 'The name already exists. Please rename and try again.',
      bothAcceptAddAndDelete: 'Successfully added {addLength} index(es), and deleted {delLength} index(es).',
      onlyAcceptAdd: 'Successfully added {addLength} index(es).',
      onlyAcceptDelete: 'Successfully deleted {delLength} index(es). ',
      buildIndexTip: ' Build Index',
      buildIndex: 'Build Index',
      batchBuildSubTitle: 'Please choose which data ranges you\'d like to build with the added indexes.',
      onlyStartLetters: 'Only supports starting with a letter',
      usedInOtherModel: 'Can\'t rename this computed column, as it\'s been used in other models.'
    },
    'zh-cn': {
      recommendations: '优化建议',
      recommendationsTip1: '以下为系统根据您的查询历史及使用情况对模型生成的优化建议。',
      recommendationsTip2: '可在项目设置中',
      modifyRules: '配置规则',
      th_recommendType: '建议类型',
      th_name: '名称',
      th_table: '表',
      th_column: '列',
      th_dataType: '数据类型',
      th_function: '函数',
      th_parameter: '函数参数',
      th_expression: '表达式',
      th_dataSize: '数据大小',
      th_useCount: '使用次数',
      th_column_count: '列数总计',
      th_updateDate: '最后更新时间',
      th_note: '备注',
      th_source: '来源',
      th_type: '类型',
      imported: '导入',
      query_history: '查询历史',
      AGG: '聚合索引',
      TABLE: '明细索引',
      ADD_AGG_INDEX: '新增聚合索引',
      REMOVE_AGG_INDEX: '删除聚合索引',
      ADD_TABLE_INDEX: '新增明细索引',
      REMOVE_TABLE_INDEX: '删除明细索引',
      usage_time_tip: '该索引在过去{date}内使用频率低于{time}次。',
      exist_index_tip: '已有索引可以包含该索引。',
      similar_index_tip: '存在与该索引相似的索引。',
      imported_index_tip: '根据导入 SQL 生成',
      query_history_tip: '根据查询历史生成',
      LOW_FREQUENCY: '低频使用',
      INCLUDED: '包含关系',
      SIMILAR: '高相似度',
      IMPORTED: '导入',
      QUERY_HISTORY: '查询历史',
      accept: '通过',
      delete: '删除',
      usedCountTip: '若为新增索引，表示该索引可优化多少条历史查询；若为删除索引，表示该索引被使用的次数。',
      viewDetail: '查看详情',
      deleteRecommendTip: '所选优化建议删除后不可恢复。确定要删除吗？',
      deleteTitle: '删除优化建议',
      deleteSuccess: '已删除',
      aggDetailTitle: '聚合索引详情',
      tableDetailTitle: '明细索引详情',
      order: '顺序',
      cc: '可计算列',
      dimension: '维度',
      measure: '度量',
      newAdd: '新增',
      validateTitle: '添加以下内容至模型',
      validateModalTip: '通过所选优化建议需要添加以下内容至模型：',
      add: '确认添加',
      requiredName: '请输入别名',
      sameNameTips: '该名称已存在，请重新命名。',
      bothAcceptAddAndDelete: '成功新增 {addLength} 条索引，删除 {delLength} 条索引。',
      onlyAcceptAdd: '成功新增 {addLength} 条索引。',
      onlyAcceptDelete: '成功删除 {delLength} 条索引。',
      buildIndexTip: '立即构建索引',
      buildIndex: '构建索引',
      batchBuildSubTitle: '请为新增的索引选择需要构建至的数据范围。',
      onlyStartLetters: '仅支持字母开头',
      usedInOtherModel: '该可计算列已在其他模型中使用，不可修改名称。'
    }
  }
})
export default class IndexList extends Vue {
  pageRefTags = pageRefTags
  transToGmtTime = transToGmtTime
  loadingRecommends = false
  loadingDetails = false
  recommendationsList = {
    list: [],
    page_offset: 0,
    totalSize: 0,
    sort_by: '',
    reverse: false,
    page_size: +localStorage.getItem(this.pageRefTags.recommendationsPager) || 10
  }
  typeList = ['ADD_AGG_INDEX', 'REMOVE_AGG_INDEX', 'ADD_TABLE_INDEX', 'REMOVE_TABLE_INDEX']
  source = ['imported', 'query_history']
  lowFrequency = {
    frequency_time_window: '',
    low_frequency_threshold: 0
  }
  checkedStatus = []
  sourceCheckedStatus = []
  selectedList = []
  showIndexDetail = false
  detailData = []
  currentIndex = null
  accessLoading = false
  validateData = {}
  showValidate = false
  isLoading = false
  rules = {
    name: [{required: true, validator: this.validateName, trigger: 'blur'}]
  }
  rulesCC = {
    name: [{required: true, validator: this.validateNameCC, trigger: 'blur'}]
  }
  hasError = false
  sameNameErrorIds = []

  get emptyText () {
    return this.$t('kylinLang.common.noData')
  }

  get indexDetailTitle () {
    return this.currentIndex ? this.currentIndex.type.split('_')[1] === 'AGG' ? this.$t('aggDetailTitle') : this.$t('tableDetailTitle') : ''
  }

  get getValidateList () {
    return this.validateData.list.filter(item => (item.type === 'dimension' && item.add) || (item.type === 'measure' && item.add) || (item.type === 'cc' && item.add))
  }

  sameNameErrorStatus (id) {
    return this.sameNameErrorIds.includes(id)
  }

  // 更改 input 内容
  changeInputContent () {
    this.sameNameErrorIds = []
  }

  // 移除错误提示信息
  removeSameNameErrror () {
    this.$nextTick(() => {
      const rfs = Object.keys(this.$refs).filter(it => /^validateForm_/.test(it))
      rfs.forEach(item => {
        const dom = this.$refs[item] && this.$refs[item].$el.getElementsByClassName('el-form-item__error')
        if (dom && dom.length > 0 && dom[0].innerText.trim() === 'false') {
          dom[0].parentNode.removeChild(dom[0])
        }
      })
    })
  }

  formatDataSize (dataSize) {
    if (dataSize < 0) {
      return ''
    } else {
      return filterElements.dataSize(dataSize)
    }
  }

  getCellClassName (scope) {
    return scope.columnIndex === 0 && !scope.row.content ? 'hide-cell-expand' : ''
  }

  created () {
    this.getRecommendations()
  }

  mounted () {
  }

  // 通过的建议中是否有同名的
  checkNameInCurrentRecommends (value) {
    return this.validateData.list.filter(it => it.name === value).length > 1
  }

  // 校验更改的cc名是否与列名相同
  checkSameCCNameInColumns (value) {
    const { all_named_columns } = this.modelDesc
    return all_named_columns.filter(item => item.name === value).length > 0
  }

  validateName (rule, value, callback) {
    if (!value || !value.trim()) {
      callback(new Error(this.$t('requiredName')))
    } else if (!NamedRegex1.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip2')))
    } else {
      callback()
    }
  }

  validateNameCC (rule, value, callback) {
    if (!NamedRegex.test(value.toUpperCase())) {
      return callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else if (/^\d|^_+/.test(value)) {
      return callback(new Error(this.$t('onlyStartLetters')))
    } else {
      callback()
    }
  }

  renderHeaderCol (h, { column, index }) {
    return <span class="used-count">
      {this.$t('th_useCount')}
      <el-tooltip content={ this.$t('usedCountTip') } effect="dark" placement="top">
        <span class="icon el-icon-ksd-what ksd-ml-5"></span>
      </el-tooltip>
    </span>
  }

  async acceptLayout () {
    this.showIndexDetail = false
    await this.confrim([this.currentIndex])
  }

  handleSelectionChange (val) {
    this.selectedList = val
  }

  // 展示优化建议详情
  showDetail (row) {
    this.showIndexDetail = true
    this.loadingDetails = true
    this.currentIndex = row
    this.getRecommendDetails({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      id: row.item_id,
      is_add: row.is_add
    }).then(async (res) => {
      let data = await handleSuccessAsync(res)
      this.detailData = [...data.cc_items.map(it => ({...it, type: 'cc'})), ...data.dimension_items.map(it => ({...it, type: 'dimension'})), ...data.measure_items.map(it => ({...it, type: 'measure'}))]
      this.loadingDetails = false
    }).catch(e => {
      handleError(e)
      this.loadingDetails = false
    })
  }

  // 获取优化建议
  getRecommendations (type) {
    const { page_offset, page_size, reverse, sort_by } = this.recommendationsList
    this.loadingRecommends = true
    this.getAllRecommendations({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      page_offset,
      page_size,
      type: this.checkedStatus.join(','),
      reverse,
      sort_by
    }).then(async (res) => {
      const data = await handleSuccessAsync(res)
      this.recommendationsList.list = data.layouts
      this.recommendationsList.totalSize = data.size
      if (type && type === 'refreshCount') {
        this.refreshModelRecCount()
      }
      this.loadingRecommends = false
      if (data.broken_recs && data.broken_recs.length > 0) {
        this.refreshRecommendationCount({model_id: this.modelDesc.uuid, project: this.currentProject, action: 'refresh'})
      }
    }).catch(e => {
      handleError(e)
      this.loadingRecommends = false
    })
  }

  // 批量删除
  betchDelete () {
    let recs_to_add_layout = []
    let recs_to_remove_layout = []
    this.selectedList.forEach(item => {
      if (item.type.split('_')[0] === 'ADD') {
        recs_to_add_layout.push(item.item_id)
      } else {
        recs_to_remove_layout.push(item.item_id)
      }
    })
    this.removeApi(recs_to_add_layout, recs_to_remove_layout)
  }

  // 删除优化建议
  removeIndex (row) {
    let recs_to_add_layout = []
    let recs_to_remove_layout = []
    row.type.split('_')[0] === 'ADD' ? recs_to_add_layout.push(row.item_id) : recs_to_remove_layout.push(row.item_id)
    this.removeApi(recs_to_add_layout, recs_to_remove_layout)
  }

  async removeApi (recs_to_add_layout, recs_to_remove_layout) {
    await this.$confirm(this.$t('deleteRecommendTip'), this.$t('deleteTitle'), {
      confirmButtonText: this.$t('delete')
    })
    this.deleteRecommendations({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      recs_to_add_layout: recs_to_add_layout.join(','),
      recs_to_remove_layout: recs_to_remove_layout.join(',')
    }).then(async (res) => {
      await handleSuccessAsync(res)
      this.$message({
        type: 'success',
        message: this.$t('deleteSuccess')
      })
      this.recommendationsList.page_offset = 0
      this.getRecommendations('refreshCount')
    }).catch(e => {
      handleError(e)
    })
  }

  // 批量通过
  betchAccept () {
    this.confrim(this.selectedList)
  }

  // 通过优化建议
  confrim (idList) {
    this.validateRecommend({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      recs_to_add_layout: idList.filter(it => it.is_add).map(v => v.item_id),
      recs_to_remove_layout: idList.filter(it => !it.is_add).map(v => v.item_id)
    }).then(async (res) => {
      let data = await handleSuccessAsync(res)
      let { recs_to_add_layout, recs_to_remove_layout, cc_items, dimension_items, measure_items } = data
      const addCCLen = cc_items.filter(it => it.add).length
      const addDimensionLen = dimension_items.filter(it => it.add).length
      const addMeasure = measure_items.filter(it => it.add).length
      if (recs_to_add_layout.length && addCCLen + addDimensionLen + addMeasure > 0) {
        this.showValidate = true
        this.validateData = {
          recs_to_add_layout,
          recs_to_remove_layout,
          list: [
            ...cc_items.map(it => ({...it, name: it.name.split('.').splice(-1).join(''), type: 'cc'})),
            ...dimension_items.map(it => {
              const name = it.name.split('.').splice(-1).join('')
              if (this.sameNameValidation(name).length > 1) {
                return {...it, name: it.name.split('.').reverse().join('_'), type: 'dimension'}
              } else {
                return {...it, name: it.name.split('.').splice(-1).join(''), type: 'dimension'}
              }
            }),
            ...measure_items.filter(item => item.name !== 'COUNT_ALL').map(it => ({...it, type: 'measure'}))
          ]
        }
      } else {
        this.validateData = {recs_to_add_layout, recs_to_remove_layout, list: []}
        this.accessApi(recs_to_add_layout, recs_to_remove_layout)
      }
    }).catch((e) => {
      handleError(e)
    })
  }

  // 判断是否同名，同名则改成 column_table 形式
  sameNameValidation (name) {
    const { all_named_columns } = this.modelDesc
    return all_named_columns.filter(v => {
      const table = v.column ? v.column.split('.')[0] : ''
      let regx = new RegExp(`_${table}$`)
      if (v.name === name || v.name.replace(regx, '') === name) {
        return v
      }
    })
  }

  // 添加更名后的layout
  async addLayout () {
    const rfs = Object.keys(this.$refs).filter(it => /^validateForm_/.test(it) && this.$refs[it] && this.$refs[it].model.add)
    for (let item of rfs) {
      const flag = await this.$refs[item].validate()
      if (!flag) {
        this.hasError = true
        break
      }
    }
    if (this.hasError) return
    let names = {}
    const { recs_to_add_layout, recs_to_remove_layout } = this.validateData
    this.isLoading = true
    this.validateData.list.forEach((it) => {
      names[it.item_id] = it.name
    })
    this.accessApi(recs_to_add_layout, recs_to_remove_layout, names).then(() => {
      this.sameNameErrorIds = []
      this.isLoading = false
      this.showValidate = false
    }).catch(e => {
      this.isLoading = false
    })
  }

  // 建议通过接口调用
  async accessApi (recs_to_add_layout, recs_to_remove_layout, names) {
    names = names || {}
    return new Promise((resolve, reject) => {
      const emptyName = Object.values(names).some(it => !it)
      if (emptyName) {
        reject()
      } else {
        this.accessRecommendations({
          project: this.currentProject,
          modelId: this.modelDesc.uuid,
          recs_to_add_layout: recs_to_add_layout,
          recs_to_remove_layout: recs_to_remove_layout,
          names
        }).then(async (res) => {
          try {
            const result = await handleSuccessAsync(res)
            let acceptIndexs = () => {
              return {
                add: recs_to_add_layout.length,
                del: recs_to_remove_layout.length
              }
            }
            this.$message({
              type: 'success',
              message: <span>{acceptIndexs().add > 0 && acceptIndexs().del > 0
              ? this.$t('bothAcceptAddAndDelete', {addLength: acceptIndexs().add, delLength: acceptIndexs().del})
                : acceptIndexs().add > 0 ? this.$t('onlyAcceptAdd', {addLength: acceptIndexs().add})
                : this.$t('onlyAcceptDelete', {delLength: acceptIndexs().del})}<a href="javascript:void();" onClick={() => this.buildIndex({layoutIds: result.added_layouts})}>{
                  (acceptIndexs().add > 0 && acceptIndexs().del > 0 || acceptIndexs().add > 0) && this.modelDesc.segments.length ? this.$t('buildIndexTip') : ''
                }</a></span>
            })
            this.recommendationsList.page_offset = 0
            this.getRecommendations('refreshCount')
            this.$emit('accept')
            resolve()
          } catch (e) {
            reject()
          }
        }).catch(e => {
          const { body: { exception } } = e
          const data = JSON.parse(exception.split('\n')[1])
          this.sameNameErrorIds = ArrayFlat(Object.values(data))
          // handleError(e)
          reject()
        })
      }
    })
  }

  // 新增建议构建索引
  buildIndex ({layoutIds}) {
    this.callConfirmSegmentModal({
      title: this.$t('buildIndex'),
      subTitle: this.$t('batchBuildSubTitle'),
      indexes: layoutIds || [],
      submitText: this.$t('buildIndex'),
      model: this.modelDesc
    })
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
      'SIMILAR': this.$t('similar_index_tip'),
      'IMPORTED': this.$t('imported_index_tip'),
      'QUERY_HISTORY': this.$t('query_history_tip')
    }
    return reason[data.row.memo_info.recommendation_source]
  }

  // 筛选类型来源
  filterType (v, type) {
    this.recommendationsList.page_offset = 0
    type === 'checkedStatus' ? (this.checkedStatus = v) : (this.sourceCheckedStatus = v)
    this.getRecommendations()
  }

  // 分页操作
  pageCurrentChange (offset, size) {
    if (size !== this.recommendationsList.page_size) {
      this.recommendationsList.page_offset = 0
      this.recommendationsList.page_size = size
    } else {
      this.recommendationsList.page_offset = offset
    }
    this.getRecommendations()
  }

  // 更改排序
  changeSort ({prop, order}) {
    this.recommendationsList = {
      ...this.recommendationsList,
      reverse: order !== 'descending',
      sort_by: prop,
      page_offset: 0
    }
    this.getRecommendations()
  }

  // 跳转至 setting 设置界面
  jumpToSetting () {
    if (getQueryString('from') === 'cloud' || getQueryString('from') === 'iframe') {
      postCloudUrlMessage(this.$route, { name: 'kapSetting' })
    } else {
      this.$router.push({path: '/setting', query: {moveTo: 'index-suggest-setting'}})
    }
  }

  // 刷新模型列表上的优化建议个数
  refreshModelRecCount () {
    const { page_offset, page_size, reverse, sort_by } = this.recommendationsList
    this.getAllRecommendations({
      project: this.currentProject,
      modelId: this.modelDesc.uuid,
      page_offset,
      page_size,
      type: '',
      reverse,
      sort_by
    }).then(async (res) => {
      const data = await handleSuccessAsync(res)
      this.modelDesc.recommendations_count = data.size
    })
  }
}
</script>

<style lang="less">
@import '../../../../../../assets/styles/variables.less';
.el-card.recommendations-card {
  border: none;

  .el-card__header {
    background: none;
    border-bottom: none;
    height: 24px;
    font-size: 14px;
    padding: 0px;
    margin-bottom: 5px;
  }
  .el-card__body {
    padding: 0 !important;
  }
  .title-tip {
    color: #5C5C5C;
    font-size: 12px;
    font-weight: 400;
    line-height: 18px;
  }
}
.el-table.index-details-table {
  .cell {
    height: 28px;
    line-height: 28px;
    .el-table__expand-icon {
      >.el-icon {
        margin-top: -2px;
      }
    }
    .column-name {
      display: inline-block;
      width: calc(~'100% - 50px');
      text-overflow: ellipsis;
      overflow: hidden;
      white-space: nowrap;
    }
  }
  .expanded {
    .el-table__expand-icon {
      >.el-icon {
        margin-top: -6px;
        margin-left: -2px;
      }
    }
  }
  .el-table__expanded-cell {
    padding: 10px;
    font-size: 12px;
    color: @text-title-color;
    p {
      margin-bottom: 5px;
      &:last-child {
        margin-bottom: 0;
      }
    }
    .label {
      color: @text-normal-color;
    }
  }
  .add-tag {
    position: absolute;
    right: 10px;
    top: 8px;
  }
  .el-table__expand-column.hide-cell-expand {
    .cell {
      pointer-events: none;
    }
    .el-table__expand-icon {
      color: #ccc;
      cursor: not-allowed;
    }
  }
}
.el-table.validate-table {
  margin-top: 10px;
  .el-form-item__content {
    line-height: 23px;
    > .is-error {
      .el-input__inner {
        border: 1px solid @error-color-1;
      }
    }
    .error-text {
      color: @error-color-1;
      font-size: 12px;
    }
  }
  .cell {
    height: initial;
  }
  .el-table__expanded-cell {
    padding: 10px;
    font-size: 12px;
    color: @text-title-color;
    p {
      margin-bottom: 5px;
      &:last-child {
        margin-bottom: 0;
      }
    }
    .label {
      color: @text-normal-color;
    }
  }
}
.layout-details {
  .el-dialog__body {
    max-height: 400px;
    overflow: auto;
    .el-form-item__error {
      white-space: normal;
    }
  }
}
</style>
