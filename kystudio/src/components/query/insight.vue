<template>
  <div id="newQuery" v-guide.queryBox>
    <div class="table-layout clearfix">
      <div class="layout-left">
        <DataSourceBar
          :project-name="currentSelectedProject"
          :is-show-load-source="false"
          :is-expand-on-click-node="false"
          :is-show-drag-width-bar="true"
          :expand-node-types="['datasource']"
          @autoComplete="handleAutoComplete"
          @click="clickTable">
        </DataSourceBar>
      </div>
      <div class="layout-right">
        <div class="ksd_right_box">
          <div class="query_result_box ksd-border-tab">
            <div class="btn-group">
              <el-button size="small" plain="plain" @click.native="closeAllTabs" style="display:inline-block">{{$t('closeAll')}}</el-button><el-button
              size="small" plain="plain" @click.native="openSaveQueryListDialog" style="display:inline-block">{{$t('kylinLang.query.reLoad')}}</el-button>
            </div>
            <tab class="insight_tab" type="card" :isedit="true" :tabslist="editableTabs" :active="activeSubMenu" v-on:clicktab="activeTab"  v-on:removetab="delTab">
              <template slot-scope="props">
                <queryTab
                  v-on:addTab="addTab"
                  v-on:changeView="changeTab"
                  v-on:resetQuery="resetQuery"
                  :completeData="completeData"
                  :tipsName="tipsName"
                  :tabsItem="props.item"></queryTab>
              </template>
            </tab>
          </div>
          <el-dialog
            :title="$t('savedQueries')"
            width="720px"
            class="saved_query_dialog"
            top="5vh"
            limited-area
            :close-on-press-escape="false"
            :close-on-click-modal="false"
            :visible.sync="savedQueryListVisible">
            <kap-empty-data v-if="!savedSize" size="small">
            </kap-empty-data>
            <div class="list_block" v-scroll v-else>
              <div class="saved_query_content">
                <div class="form_block" v-for="(savequery, index) in savedList" :key="savequery.name" >
                  <el-checkbox v-model="checkedQueryList" :label="index" class="query_check">
                    <el-form class="narrowForm" label-position="left" label-width="105px">
                      <el-form-item :label="$t('kylinLang.query.name')+' :'" class="ksd-mb-2 narrowFormItem" >
                        <span>{{savequery.name}}</span>
                      </el-form-item>
                      <el-form-item :label="$t('kylinLang.query.desc')+' :'" class="ksd-mb-2 narrowFormItem" >
                        <span class="desc-block">{{savequery.description}}</span>
                      </el-form-item>
                      <el-form-item :label="$t('kylinLang.query.querySql')+' :'" prop="sql" class="ksd-mb-2 narrowFormItem">
                        <el-button plain size="mini" @click="toggleDetail(index)">
                          {{$t('kylinLang.common.seeDetail')}}
                          <i class="el-icon-arrow-down" v-show="!savequery.isShow"></i>
                          <i class="el-icon-arrow-up" v-show="savequery.isShow"></i>
                        </el-button>
                        <kap-editor width="99%" height="150" lang="sql" theme="chrome" v-model="savequery.sql" dragbar="#393e53" ref="saveQueries" :readOnly="true" v-if="savequery.isShow" class="ksd-mt-6">
                        </kap-editor>
                      </el-form-item>
                      <div class="btn-group">
                        <el-button size="small" type="info" class="remove_query_btn" text @click="removeQuery(savequery)">{{$t('kylinLang.common.delete')}}</el-button>
                      </div>
                    </el-form>
                  </el-checkbox>
                </div>
                <el-button plain size="small" class="reload-more-btn" @click="pageCurrentChange" v-if="savedList.length < savedSize">{{$t('more')}}</el-button>
              </div>
            </div>
            <span slot="footer" class="dialog-footer">
              <el-button @click="cancelResubmit" size="medium">{{$t('kylinLang.common.cancel')}}</el-button><el-button
              type="primary" plain @click="resubmit" size="medium" :disabled="!checkedQueryList.length">{{$t('kylinLang.common.submit')}}</el-button>
            </span>
          </el-dialog>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import tab from '../common/tab'
import queryTab from './query_tab'
import DataSourceBar from '../common/DataSourceBar'
import { kapConfirm, hasRole, handleSuccess, handleError } from '../../util/business'
import { mapActions, mapMutations, mapGetters } from 'vuex'
import { insightKeyword } from '../../config'
@Component({
  methods: {
    ...mapActions({
      getSavedQueries: 'GET_SAVE_QUERIES',
      delQuery: 'DELETE_QUERY',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      query: 'QUERY_BUILD_TABLES'
    }),
    ...mapMutations({
      saveTabs: 'SET_QUERY_TABS'
    })
  },
  components: {
    tab,
    queryTab,
    DataSourceBar
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'getQueryTabs',
      'datasourceActions'
    ])
  },
  locales: {
    'en': {dialogHiveTreeNoData: 'Please click data source to load source tables', trace: 'Trace', savedQueries: 'Save Queries', queryBox: 'Query Box', more: 'More', closeAll: 'Close All', delSqlTitle: 'Delete SQL', confirmDel: 'Are you sure to delete {queryName}?'},
    'zh-cn': {dialogHiveTreeNoData: '请点击数据源来加载源表', trace: '追踪', savedQueries: '保存的查询', queryBox: '查询窗口', more: '更多', closeAll: '关闭全部', delSqlTitle: '删除查询语句', confirmDel: '确认删除 {queryName} 吗？'}
  }
})
export default class NewQuery extends Vue {
  savedQueryListVisible = false
  editableTabs = []
  activeSubMenu = 'WorkSpace'
  savedQuriesSize = 0
  queryCurrentPage = 1
  datasource = []
  savedList = []
  savedSize = 0
  checkedQueryList = []
  completeData = []
  tipsName = ''
  handleAutoComplete (data) {
    this.completeData = [...data, ...insightKeyword]
  }
  toggleDetail (index) {
    this.savedList[index].isShow = !this.savedList[index].isShow
  }
  loadSavedQuery (pageIndex) {
    this.getSavedQueries({
      project: this.currentSelectedProject || null,
      limit: 10,
      offset: pageIndex
    }).then((res) => {
      handleSuccess(res, (data) => {
        data.saved_queries.forEach((item) => {
          item['isShow'] = false
        })
        this.savedList = this.savedList.concat(data.saved_queries)
        this.savedSize = data.size
      })
    }, (res) => {
      handleError(res)
    })
  }
  resetQuery () {
    this.editableTabs[0].queryObj = null
    this.editableTabs[0].queryErrorInfo = ''
    this.editableTabs[0].extraoption = null
    this.cacheTabs()
  }
  addTab (targetName, queryObj) {
    this.editableTabs[0].queryObj = queryObj
    this.editableTabs[0].cancelQuery = false
    var tabIndex = this.editableTabs.length > 1 ? this.editableTabs[1].index + 1 : this.editableTabs[0].index + 1
    var tabName = targetName + tabIndex
    this.editableTabs.splice(1, 0, {
      title: tabName,
      name: tabName,
      icon: 'el-icon-loading',
      spin: true,
      extraoption: null,
      queryErrorInfo: '',
      queryObj: queryObj,
      index: tabIndex
    })
    // this.activeSubMenu = tabName
    this.cacheTabs()
  }
  activeTab (tabName) {
    this.activeSubMenu = tabName
  }
  delTab (targetName) {
    if (targetName === 'WorkSpace') {
      return
    }
    let tabs = this.editableTabs
    let activeName = this.activeSubMenu
    if (activeName === targetName) {
      tabs.forEach((tab, index) => {
        if (tab.name === targetName) {
          let nextTab = tabs[index - 1] || tabs[index + 1]
          if (nextTab) {
            activeName = nextTab.name
          }
        }
      })
    }
    this.editableTabs = tabs.filter(tab => tab.name !== targetName)
    this.activeSubMenu = activeName
    this.editableTabs[0].cancelQuery = true
    this.cacheTabs()
  }
  clickTable (leaf) {
    this.tipsName = ''
    this.$nextTick(() => {
      if (leaf) {
        this.tipsName = leaf.label
      }
    })
  }
  closeAllTabs () {
    this.editableTabs.splice(1, this.editableTabs.length - 1)
    this.editableTabs[0].cancelQuery = true
    this.activeSubMenu = 'WorkSpace'
    this.cacheTabs()
  }
  pageCurrentChange () {
    this.queryCurrentPage++
    this.loadSavedQuery(this.queryCurrentPage - 1)
  }
  removeQuery (query) {
    kapConfirm(this.$t('confirmDel', {queryName: query.name}), null, this.$t('delSqlTitle')).then(() => {
      this.delQuery({project: this.currentSelectedProject, id: query.id}).then((response) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.savedList = []
        this.loadSavedQuery(this.queryCurrentPage - 1)
      })
    })
  }
  cancelResubmit () {
    this.savedQueryListVisible = false
  }
  resubmit () {
    if (this.checkedQueryList.length > 0) {
      this.checkedQueryList.forEach((index) => {
        var queryObj = {
          acceptPartial: true,
          limit: this.listRows,
          offset: 0,
          project: this.currentSelectedProject,
          sql: this.savedList[index].sql
        }
        this.addTab('query', queryObj)
      })
    }
    this.savedQueryListVisible = false
  }
  changeTab (index, data, errorInfo) {
    if (index === 0 || index === this.editableTabs[1].index) { // 编辑器结果中断查询时置空或者显示最新一条查询的结果
      this.editableTabs[0].extraoption = data
      this.editableTabs[0].queryErrorInfo = errorInfo
      this.editableTabs[0].cancelQuery = false
    }
    if (index) {
      let tabs = this.editableTabs
      for (var k = 1; k < tabs.length; k++) {
        if (tabs[k].index === index) {
          tabs[k].icon = errorInfo ? 'el-icon-ksd-error_01' : 'el-icon-ksd-good_health'
          tabs[k].spin = errorInfo === 'circle-o-notch'
          tabs[k].extraoption = data
          tabs[k].queryErrorInfo = errorInfo
          break
        }
      }
    }
    this.cacheTabs()
  }
  cacheTabs () {
    const project = this.currentSelectedProject
    const obj = {[project]: this.editableTabs}
    this.saveTabs({tabs: obj})
  }
  openSaveQueryDialog () {
    this.$nextTick(() => {
      this.saveQueryFormVisible = true
    })
  }
  openSaveQueryListDialog () {
    this.savedQueryListVisible = true
    this.checkedQueryList = []
    this.savedList = []
    this.loadSavedQuery()
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  created () {
    this.editableTabs = this.getQueryTabs && this.getQueryTabs[this.currentSelectedProject]
    ? this.getQueryTabs[this.currentSelectedProject]
    : [{
      title: 'sqlEditor',
      i18n: 'sqlEditor',
      name: 'WorkSpace',
      icon: '',
      spin: true,
      extraoption: null,
      queryErrorInfo: '',
      queryObj: null,
      index: 0,
      cancelQuery: false
    }]
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #newQuery {
    position: relative;
    height: 100%;
    .data-source-bar .el-tree__empty-block {
      display: none;
    }
    #tab-WorkSpace .el-icon-close {
      visibility: hidden;
      display: none;
    }
    .el-icon-ksd-keyboard {
      cursor: inherit;
    }
    .saved_query_dialog {
      .el-dialog__body {
        padding: 0;
        height: 503px;
        position: relative;
        .list_block {
          width: 100%;
          height: 503px;
          .saved_query_content {
            margin: 20px;
          }
        }
      }
      .desc-block {
        display: inline-block;
        width: 535px;
        word-break: break-word;
        white-space: pre-wrap;
        overflow: hidden;
        line-height: 21px;
      }
    }
    // .nodata {
    //   text-align: center;
    //   color: @text-disabled-color;
    //   position: absolute;
    //   top: 50%;
    //   left: 50%;
    //   transform: translate(-50%, -50%);
    // }
    .form_block {
      .query_check {
        display: flex;
        .el-checkbox__input {
          flex-grow: 0;
        }
        .el-checkbox__label {
          flex-grow: 1;
        }
        .narrowForm {
          border: 1px solid @line-border-color;
          padding: 10px;
          margin-bottom: 10px;
          position: relative;
          background-color: @aceditor-bg-color;
          &:hover {
            box-shadow: 0 0 2px 0 #ccc, 0 0px 2px 0 #ccc;
          }
          .narrowFormItem {
            .el-form-item__content, .el-form-item__label {
              line-height: 22px;
            }
            .el-button--mini {
              padding: 5px 8px;
            }
          }
          .btn-group {
            position: absolute;
            top: 5px;
            right: 10px;
            .remove_query_btn {
              color: @text-normal-color;
              &:hover {
                color: @base-color;
              }
            }
          }
        }
        &.is-checked {
          .narrowForm {
            border-color: @base-color;
          }
        }
      }
    }
    .operatorBox{
      .tips_box{
        color: @text-normal-color;
        flex:1;
        display: flex;
        align-items: center;
      }
      .operator{
        height: 24px;
        line-height: 24px;
        .el-form-item__label{
          padding:0 12px 0 0;
        }
        .el-form-item{
          margin-bottom:0;
          &:last-child {
            margin-right: 0;
            .el-form-item__content {
              position: relative;
              top: -1px;
            }
          }
        }
        .el-form-item__content{
          line-height: 24px;
        }
      }
    }
    .submit-tips {
      float: right;
      font-size: 12px;
      line-height: 18px;
      vertical-align: middle;
      margin-top: 5px;
      color: @text-disabled-color;
      i {
        cursor: default;
      }
    }
    .insight_tab{
      .el-tabs__new-tab{
        display: none;
      }
      .el-tabs__header {
        margin-bottom: 0;
      }
    }
    .query_result_box{
      border: 0;
      position: relative;
      top: -5px;
      > .btn-group {
        position: absolute;
        right: 0px;
        top: 2px;
        z-index: 99;
      }
      h3{
        margin: 20px;
      }
      .el-tabs{
        margin-top: 3px;
        .el-tabs__nav-wrap {
          width: calc(~'100% - 250px');
          &::after {
            background-color: transparent;
          }
        }
        .el-tabs__content{
          padding: 0px;
          .el-tab-pane{
            padding-top: 15px;
          }
        }
      }
    }
    .el-icon-ksd-good_health {
      color: @color-success;
      font-size: 12px;
    }
    .el-icon-error {
      color: @color-danger;
      font-size: 12px;
    }
    .el-icon-ksd-error_01 {
      color: red;
      font-size: 12px;
    }
    .reload-more-btn {
      width: 100px;
      margin: 0 auto;
      display: block;
      margin-top: 20px;
    }
  }
</style>
