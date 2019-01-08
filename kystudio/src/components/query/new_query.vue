<template>
  <div id="newQuery">
    <div class="table-layout clearfix"> 
      <div class="layout-left">
        <DataSourceBar
          :project-name="currentSelectedProject"
          :is-show-load-source="false"
          :is-expand-on-click-node="false"
          :expand-node-types="['datasource', 'database']"
          @autoComplete="handleAutoComplete"
          @click="clickTable">
        </DataSourceBar>
      </div>
      <div class="layout-right">
        <div class="ksd_right_box">
          <div class="query_result_box ksd-border-tab">
            <div class="btn-group">
              <el-button size="mini" plain="plain" @click.native="closeAllTabs" style="display:inline-block">{{$t('closeAll')}}</el-button>
              <el-button size="mini" plain="plain" icon="el-icon-ksd-sql" @click.native="openSaveQueryListDialog" style="display:inline-block">{{$t('kylinLang.query.reLoad')}}</el-button>
            </div>
            <tab class="insight_tab" :isedit="true" :tabslist="editableTabs" :active="activeSubMenu" v-on:clicktab="activeTab"  v-on:removetab="delTab">
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
            width="660px"
            class="saved_query_dialog"
            top="10vh"
            :visible.sync="savedQueryListVisible">
            <div class="list_block" v-scroll>
              <div v-if="!savedSize" class="nodata">
                <div class="ksd-mb-20"><img src="../../assets/img/save_query.png" style="height:80px;"></div>
                <span>{{$t('kylinLang.common.noData')}}</span>
              </div>
              <div class="saved_query_content" v-else>
                <div class="form_block" v-for="(savequery, index) in savedList" :key="savequery.name" >
                  <el-checkbox v-model="checkedQueryList" :label="index" class="query_check">
                    <el-form class="narrowForm">
                      <el-form-item :label="$t('kylinLang.query.name')+' :'" class="ksd-mb-2 narrowFormItem" >
                        <span>{{savequery.name}}</span>
                      </el-form-item>
                      <el-form-item :label="$t('kylinLang.query.desc')+' :'" class="ksd-mb-2 narrowFormItem" >
                        <span>{{savequery.description}}</span>
                      </el-form-item>
                      <el-form-item :label="$t('kylinLang.query.querySql')+' :'" prop="sql" class="ksd-mb-2 narrowFormItem">
                        <el-button plain size="mini" @click="toggleDetail(index)">
                          {{$t('kylinLang.common.seeDetail')}}
                          <i class="el-icon-arrow-down" v-show="!savequery.isShow"></i>
                          <i class="el-icon-arrow-up" v-show="savequery.isShow"></i>
                        </el-button>
                        <kap-editor width="99%" height="150" lang="sql" theme="chrome" v-model="savequery.sql" dragbar="#393e53" ref="saveQueries" v-show="savequery.isShow" class="ksd-mt-6">
                        </kap-editor>
                      </el-form-item>
                      <div class="btn-group">
                        <el-button size="small" type="info" class="remove_query_btn" text @click="removeQuery(savequery.id)">{{$t('kylinLang.common.delete')}}</el-button>
                      </div>
                    </el-form>
                  </el-checkbox>
                </div>
                <el-button plain size="small" class="reload-more-btn" @click="pageCurrentChange" v-if="savedList.length < savedSize">{{$t('more')}}</el-button>
              </div>
            </div>
            <span slot="footer" class="dialog-footer">
              <el-button @click="cancelResubmit" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
              <el-button type="primary" plain @click="resubmit" size="medium" :disabled="!checkedQueryList.length">{{$t('kylinLang.common.submit')}}</el-button>
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
      loadDataSourceByProject: 'LOAD_DATASOURCE'
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
      'getQueryTabs'
    ])
  },
  locales: {
    'en': {dialogHiveTreeNoData: 'Please click data source to load source tables', trace: 'Trace', savedQueries: 'Save Queries', queryBox: 'Query Box', more: 'More', closeAll: 'Close All'},
    'zh-cn': {dialogHiveTreeNoData: '请点击数据源来加载源表', trace: '追踪', savedQueries: '保存的查询', queryBox: '查询窗口', more: '更多', closeAll: '关闭全部'}
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
    this.editableTabs[0].queryErrorInfo = ''
    this.cacheTabs()
  }
  addTab (targetName, queryObj) {
    this.editableTabs[0].queryObj = queryObj
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
    this.activeSubMenu = tabName
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
    this.cacheTabs()
  }
  clickTable (leaf) {
    if (leaf) {
      this.tipsName = leaf.label
    }
  }
  closeAllTabs () {
    this.editableTabs.splice(1, this.editableTabs.length - 1)
    this.activeSubMenu = 'WorkSpace'
    this.cacheTabs()
  }
  pageCurrentChange () {
    this.queryCurrentPage++
    this.loadSavedQuery(this.queryCurrentPage - 1)
  }
  removeQuery (queryId) {
    kapConfirm(this.$t('kylinLang.common.confirmDel')).then(() => {
      this.delQuery({project: this.currentSelectedProject, id: queryId}).then((response) => {
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
    if (index) {
      let tabs = this.editableTabs
      for (var k = 0; k < tabs.length; k++) {
        if (tabs[k].index === index) {
          tabs[k].icon = errorInfo ? 'el-icon-ksd-error_01' : 'el-icon-ksd-good_health'
          tabs[k].spin = errorInfo === 'circle-o-notch'
          tabs[k].extraoption = data
          tabs[k].queryErrorInfo = errorInfo
          break
        }
      }
      this.cacheTabs()
    }
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
      title: 'Work Space',
      name: 'WorkSpace',
      icon: '',
      spin: true,
      extraoption: null,
      queryErrorInfo: '',
      queryObj: null,
      index: 0
    }]
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #newQuery {
    position: relative;
    height: 100%;
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
        .list_block {
          width: 100%;
          height: 480px;
          .saved_query_content {
            margin: 20px;
          }
        }
      }
    }
    .nodata {
      text-align: center;
      margin: 80px 0;
    }
    .form_block {
      .query_check {
        display: flex;
        flex-grow: 1;
        align-items: flex-start;
        white-space: inherit;
        .el-checkbox__input {
          flex-grow: 0;
        }
        .el-checkbox__label {
          flex-grow: 1;
        }
        .narrowForm {
          border: 1px solid @line-border-color;
          padding: 15px;
          margin-bottom: 10px;
          position: relative;
          background-color: @aceditor-bg-color;
          &:hover {
            box-shadow: 0 0 6px 0 #cfd8dc, 0 2px 4px 0 #cfd8dc;
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
            top: 65px;
            right: 20px;
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
        align-items: flex-start;
      }
      .operator{
        height: 30px;
        line-height: 30px;
        .el-form-item__label{
          padding:0 12px 0 0;
        }
        .el-form-item{
          margin-bottom:0;
          &:last-child {
            margin-right: 0;
          }
        }
        .el-form-item__content{
          line-height: 30px;
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
        position: relative;
        top: 2px;
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
      > .btn-group {
        position: absolute;
        right: 10px;
        top: 5px;
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
        .el-tabs__nav{
          margin-left: 0px;
        }
        .el-tabs__content{
          padding: 0px;
          border-top: 1px solid @line-border-color;
          .el-tab-pane{
            padding-top: 20px;
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
