<template>
  <div class="source-hive clearfix">
    <div class="list clearfix">
      <TreeList
        ref="tree-list"
        class="table-tree"
        :data="treeData"
        :is-show-filter="true"
        :is-show-resize-bar="false"
        :on-filter="handleFilter"
        :filter-white-list-types="['datasource', 'database']"
        @resize="handleResize"
        @click="handleClickNode"
        @node-expand="handleNodeExpand"
        @load-more="handleLoadMore"
      />
      <div class="split" v-if="false">
        <i class="el-icon-ksd-more_03"></i>
      </div>
    </div>
    <div class="content" :style="contentStyle">
      <div class="content-body" :class="{ 'has-tips': isShowTips }">
        <template v-if="selectedDatabases.length || selectedTables.length">
          <div class="category databases" v-if="selectedDatabases.length">
            <div class="header font-medium">
              <span>{{$t('database')}}</span>
              <span>({{selectedDatabases.length}})</span>
            </div>
            <div class="names">
              <el-select multiple filterable v-model="selectedDatabases" @remove-tag="handleRemoveTable" @input="handleAddTable">
                <el-option
                  v-for="option in databaseOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value">
                </el-option>
              </el-select>
            </div>
          </div>
          <div class="category tables" v-if="selectedTables.length">
            <div class="header font-medium">
              <span>{{$t('tableName')}}</span>
              <span>({{selectedTables.length}})</span>
            </div>
            <div class="names">
              <el-select multiple filterable v-model="selectedTables" @remove-tag="handleRemoveTable" @input="handleAddTable">
                <el-option
                  v-for="option in tableOptions"
                  :key="option.value"
                  :label="option.label"
                  :value="option.value">
                </el-option>
              </el-select>
            </div>
          </div>
        </template>
        <template v-else>
          <div class="empty">
            <img class="empty-img" src="../../../../assets/img/no_data.png" />
            <p class="empty-text">{{$t('kylinLang.common.noData')}}</p>
          </div>
        </template>
      </div>
      <transition name="fade">
        <div class="tips" v-if="isShowTips">
          <div class="close el-icon-ksd-close" @click="handleHideTips"></div>
          <div class="header font-medium">{{sourceType === sourceTypes['HIVE'] ? $t('loadHiveTipHeader') : $t('loadTipHeader')}}</div>
          <ul class="body">
            <li>{{sourceType === sourceTypes['HIVE'] ? $t('loadHiveTip1') : $t('loadTip1')}}</li>
            <li>{{sourceType === sourceTypes['HIVE'] ? $t('loadHiveTip2') : $t('loadTip2')}}</li>
          </ul>
        </div>
      </transition>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import TreeList from '../../TreeList'
import { sourceTypes } from '../../../../config'
import { getDatabaseTree, getTableTree } from './handler'
import { handleSuccessAsync, handleError } from '../../../../util'
import arealabel from '../../area_label.vue'

@Component({
  props: {
    selectedTables: {
      default: () => []
    },
    selectedDatabases: {
      default: () => []
    },
    sourceType: Number
  },
  components: {
    TreeList,
    arealabel
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'selectedProjectDatasource'
    ])
  },
  methods: {
    ...mapActions({
      fetchDatabase: 'LOAD_HIVEBASIC_DATABASE',
      fetchTables: 'LOAD_HIVE_TABLES'
    })
  },
  locales
})
export default class SourceHive extends Vue {
  treeData = []
  contentStyle = {
    marginLeft: null,
    width: null
  }
  sourceTypes = sourceTypes
  timer = null
  isDatabaseError = false
  isShowTips = true
  selectorWidth = 0
  filterText = ''

  get databaseOptions () {
    return this.treeData.map(database => ({
      value: database.id,
      label: database.id
    }))
  }
  get tableOptions () {
    const tableOptions = []
    this.treeData.forEach(database => {
      return database.children.forEach(table => {
        tableOptions.push({
          value: table.id,
          label: table.id
        })
      })
    })
    return tableOptions
  }
  @Watch('selectedTables')
  @Watch('selectedDatabases')
  onSelectedItemsChange () {
    // 刷新table或者db的选中状态
    for (const database of this.treeData) {
      database.isSelected = this.selectedDatabases.includes(database.id)
      if (database.isSelected) {
        for (const table of database.children) {
          table.isSelected = true
          table.clickable = false
        }
      } else {
        for (const table of database.children) {
          if (!table.isLoaded) {
            table.isSelected = this.selectedTables.includes(table.id)
            table.clickable = true
          }
        }
      }
    }
  }
  setNextPagination (pagination) {
    pagination.pageOffset++
  }
  clearPagination (pagination) {
    pagination.pageOffset = 0
  }
  hideNodeLoading (data) {
    data.isLoading = false
  }
  constructor () {
    super()
    this.getDatabaseTree = getDatabaseTree.bind(this)
    this.getTableTree = getTableTree.bind(this)
  }
  async mounted () {
    await this.loadDatabase()
  }
  updated () {
    this.refreshSelectorWidth()
    this.refreshTagElWidth()
  }
  refreshSelectorWidth () {
    const selectorEl = this.$el.querySelector('.el-select')
    this.selectorWidth = selectorEl && selectorEl.getBoundingClientRect().width
  }
  refreshTagElWidth () {
    const tagEls = this.$el.querySelectorAll('.el-tag')
    for (let i = 0; i < tagEls.length; i++) {
      const tagEl = tagEls[i]
      tagEl.title = tagEl.innerText
      tagEl.style.maxWidth = `${this.selectorWidth}px`
    }
  }
  async loadDatabase () {
    this.$refs['tree-list'].showLoading()
    try {
      const projectName = this.currentSelectedProject
      const sourceType = this.sourceType
      const res = await this.fetchDatabase({ projectName, sourceType })
      this.treeData = this.getDatabaseTree(await handleSuccessAsync(res))
      this.isDatabaseError = false
    } catch (e) {
      this.isDatabaseError = true
      handleError(e)
      console.log(e)
    }
    this.$refs['tree-list'].hideLoading()
  }
  async loadTables ({database, tableName = '', isTableReset = false}) {
    const projectName = this.currentSelectedProject
    const sourceType = this.sourceType
    const databaseName = database.id
    const pagination = database.pagination
    const response = await this.fetchTables({ projectName, sourceType, databaseName, tableName, ...pagination })
    const { size, tables } = await handleSuccessAsync(response)
    this.getTableTree(database, { size, tables }, isTableReset)
    this.setNextPagination(pagination)
    // this.$emit('input', { selectedTables: [...this.selectedTables] })
  }
  handleFilter (filterText) {
    clearInterval(this.timer)

    return new Promise(async resolve => {
      this.timer = setTimeout(async () => {
        const requests = this.treeData.map(async database => {
          const { pagination } = database
          const tableName = filterText
          this.clearPagination(pagination)

          await this.loadTables({ database, tableName, isTableReset: true })
        })
        await Promise.all(requests)
        this.treeData = [...this.treeData]
        this.filterText = filterText
        this.onSelectedItemsChange()
        resolve()
      }, 1000)
    })
  }
  async handleSelectDatabase (event, data) {
    event.preventDefault()
    event.stopPropagation()
    this.selectedDatabases.includes(data.id)
      ? this.handleRemoveDatabase(data.id)
      : this.handleAddDatabase(data.id)
  }
  async handleClickNode (data, node, event) {
    if ((data.type === 'table' && data.clickable)) {
      this.selectedTables.includes(data.id)
        ? this.handleRemoveTable(data.id)
        : this.handleAddTable(data.id)
    }
    if (data.type === 'datasource' && this.isDatabaseError) {
      await this.loadDatabase()
    }
  }
  handleResize (treeWidth) {
    const marginLeft = treeWidth + 25 + 20
    this.contentStyle.marginLeft = `${marginLeft}px`
    this.contentStyle.width = `${this.$el.clientWidth - marginLeft}px`
  }
  async handleNodeExpand (data) {
    if (data.isLoading) {
      if (data.type === 'database') {
        await this.loadTables({ database: data })
      }
      this.hideNodeLoading(data)
    }
  }
  async handleLoadMore (data) {
    const database = this.treeData.find(database => database.id === data.parent.id)
    const tableName = this.filterText
    this.loadTables({ database, tableName })
  }
  handleAddDatabase (addDatabaseId) {
    let selectedTables = this.selectedTables
    let selectedDatabases = addDatabaseId instanceof Array ? addDatabaseId : [...this.selectedDatabases, addDatabaseId]

    selectedDatabases.forEach(database => {
      selectedTables = selectedTables.filter(table => table.indexOf(`${database}.`) !== 0)
    })
    this.$emit('input', { selectedDatabases, selectedTables })
  }
  handleRemoveDatabase (removeDatabaseId) {
    const selectedDatabases = this.selectedDatabases.filter(databaseId => databaseId !== removeDatabaseId)
    this.$emit('input', { selectedDatabases })
  }
  handleAddTable (addTableId) {
    const selectedTables = addTableId instanceof Array ? addTableId : [...this.selectedTables, addTableId]
    this.$emit('input', { selectedTables })
  }
  handleRemoveTable (removeTableId) {
    const selectedTables = this.selectedTables.filter(tableId => tableId !== removeTableId)
    this.$emit('input', { selectedTables })
  }
  handleValidateFail () {
    this.$message(this.$t('selectedHiveValidateFailText'))
  }
  handleHideTips () {
    this.isShowTips = false
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.source-hive {
  .list {
    position: relative;
    float: left;
  }
  .table-tree {
    width: 480px;
    float: left;
    padding: 20px 0;
    margin-left: 20px;
  }
  .split {
    position: absolute;
    top: 50%;
    right: 0;
    transform: translate(20px, 100%);
    * {
      cursor: default;
    }
  }
  .filter-box {
    box-sizing: border-box;
    margin-bottom: 10px;
    width: 210px;
  }
  .filter-tree {
    height: 470px;
    overflow: auto;
    border: 1px solid @line-border-color;
  }
  .content {
    margin-left: calc(480px + 25px + 20px);
    padding: 62px 20px 20px 0;
    position: relative;
    height: 470px;
  }
  .content-body {
    position: relative;
    height: 470px;
    border: 1px solid @line-border-color;
    transition: height .2s .2s;
    overflow: auto;
  }
  .content-body.has-tips {
    height: 322px;
  }
  .el-tag {
    margin-right: 10px;
  }
  .databases,
  .tables {
    padding: 15px;
    .header {
      color: @text-normal-color;
      margin-bottom: 10px;
    }
    .names .el-select {
      width: 100%;
    }
    .names .el-select .el-input__inner {
      border: none;
    }
    .names .el-select .el-input__suffix {
      display: none;
    }
    .names .el-select .el-select__input {
      width: 1px !important;
    }
    .el-tag {
      position: relative;
      margin-right: 10px;
      margin-left: 0px;
      padding-right: 25px;
      display: inline-block;
      text-overflow: ellipsis;
      overflow: hidden;
    }
    .el-tag .el-tag__close {
      position: absolute;
      top: 50%;
      right: 2px;
      transform: scale(.8) translateY(-50%);
    }
  }
  .category {
    border-bottom: 1px solid @line-border-color;
  }
  .category:last-child {
    border-bottom: none;
  }
  .empty {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    text-align: center;
  }
  .empty-img {
    width: 40px;
    margin-bottom: 7px;
  }
  .empty-text {
    font-size: 14px;
    line-height: 1.5;
    color: @text-normal-color;
  }
  .tips {
    position: absolute;
    padding: 15px;
    border-radius: 2px;
    background-color: @base-color-9;
    bottom: 20px;
    right: 20px;
    .header {
      color: @text-normal-color;
      font-size: 13px;
      margin-bottom: 2px;
    }
    .body {
      line-height: 1.33;
      color: @text-normal-color;
      font-size: 12px;
    }
    ul, li {
      list-style: decimal;
    }
    ul {
      padding-left: 16px;
    }
    .close {
      position: absolute;
      top: 6px;
      right: 6px;
      font-size: 16px;
      cursor: pointer;
      color: @base-color;
    }
  }
  .fade-enter-active, .fade-leave-active {
    transition: opacity .2s;
  }
  .fade-enter, .fade-leave-to {
    opacity: 0;
  }
  // 定制化datasource tree样式
  .table-tree {
    // 去掉默认样式
    // .el-tree-node__content:hover {
    //   background-color: inherit;
    // }
    // .el-tree-node:focus>.el-tree-node__content {
    //   background-color: inherit;
    //   color: inherit;
    // }
    // 定制样式: database
    .el-tree > .el-tree-node > .el-tree-node__content {
      position: relative;
      padding-top: 10px;
      padding-bottom: 10px;
    }
    .el-tree > .el-tree-node {
      border-bottom: 1px solid @line-border-color;
    }
    .el-tree > .el-tree-node > .el-tree-node__content > .tree-item {
      position: static;
    }
    .select-all {
      display: none;
      position: absolute;
      top: 50%;
      right: 10px;
      transform: translateY(-50%);
      line-height: 36px;
      font-size: 12px;
      &:hover {
        color: #0988de;
      }
    }
    .el-tree-node__expand-icon {
      padding-top: 0;
      padding-bottom: 0;
    }
    .el-tree-node__content {
      min-height: 16px;
      height: auto;
      padding-top: 12px;
      padding-bottom: 12px;
    }
    .el-tree-node__content:hover .select-all {
      display: block;
    }
    .label-synced {
      position: absolute;
      top: 50%;
      right: 14px;
      color: @text-disabled-color;
      transform: translateY(-50%);
    }
    .tree-item {
      position: relative;
      user-select: none;
      width: 100%;
      white-space: normal;
      word-break: break-all;
    }
    .el-icon-ksd-good_health {
      color: @btn-success-normal;
    }
    .database {
      .el-icon-ksd-good_health {
        margin-right: 5px;
      }
    }
    .table {
      width: calc(~'100% - 24px');
      &.synced {
        width: calc(~'100% - 100px');
      }
      .el-icon-ksd-good_health {
        position: absolute;
        left: 0;
        top: 50%;
        transform: translate(-20px, -50%);
      }
    }
    .selected {
      .database,
      .table {
        color: @base-color;
        &.disabled {
          color: @text-title-color;
        }
      }
    }
    .database,
    .table {
      color: @text-normal-color;
    }
    .table.parent-selected .el-icon-ksd-good_health {
      transform: translate(-2px, -50%);
    }
    .table.parent-selected {
      padding-left: 18px;
    }
    .load-more {
      line-height: inherit;
    }
  }
}
</style>
