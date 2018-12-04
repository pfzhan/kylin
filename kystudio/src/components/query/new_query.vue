<template>
  <div id="newQuery">
    <div class="table-layout clearfix"> 
      <div class="layout-left">
        <DataSourceBar
          :project-name="currentSelectedProject"
          :is-show-load-source="true"
          :is-expand-on-click-node="false"
          :expand-node-types="['datasource', 'database']"
          @autoComplete="handleAutoComplete"
          @click="clickTable">
        </DataSourceBar>
      </div>
      <div class="layout-right">
        <div class="ksd_right_box">
          <div class="ksd-title-label ksd-mb-10">{{$t('queryBox')}}</div>
          <div class="query_panel_box ksd-mb-20">
            <kap-editor ref="insightBox" height="170" lang="sql" theme="chrome" @keydown.meta.enter.native="submitQuery" @keydown.ctrl.enter.native="submitQuery" v-model="sourceSchema">
            </kap-editor>
            <div class="clearfix operatorBox">
              <p class="tips_box">
                <el-button size="small" plain="plain" icon="el-icon-ksd-sql" @click.native="openSaveQueryListDialog" style="display:inline-block">{{$t('kylinLang.query.reLoad')}}</el-button>
                <el-button size="small" plain="plain" @click.native="resetQuery" style="display:inline-block">{{$t('kylinLang.query.clear')}}</el-button>
              </p>
              <p class="operator">
                <el-form :inline="true" class="demo-form-inline">
                  <el-form-item v-show="showHtrace">
                    <el-checkbox v-model="isHtrace" @change="changeTrace">{{$t('trace')}}</el-checkbox>
                   </el-form-item>
                  <el-form-item>
                    <el-checkbox v-model="hasLimit" @change="changeLimit">Limit</el-checkbox>
                  </el-form-item>
                  <el-form-item>
                    <el-input  placeholder="" size="small" style="width:90px;" v-model="listRows" class="limit-input"></el-input>
                  </el-form-item>
                  <el-form-item>
                    <el-button type="primary" plain size="small" @click="submitQuery">{{$t('kylinLang.common.submit')}}</el-button>
                  </el-form-item>
                </el-form>
              </p>
            </div>
            <div class="submit-tips">
              <span>{{$t('kylinLang.common.notice')}}: </span>
              <i class="el-icon-ksd-keyboard ksd-fs-16" ></i>
              control / command + Enter = <span>{{$t('kylinLang.common.submit')}}</span></div>
          </div>
          <!-- <div class="line" style="margin: 20px 0;" v-show='editableTabs&&editableTabs.length'></div> -->
          <div class="query_result_box ksd-border-tab" v-show='editableTabs&&editableTabs.length'>
            <tab type="card" class="insight_tab" v-on:addtab="addTab" :isedit="true" :tabslist="editableTabs" :active="activeSubMenu"  v-on:removetab="delTab">
              <template slot-scope="props">
                <component :is="props.item.content" v-on:changeView="changeTab" :extraoption="props.item.extraoption"></component>
              </template>
            </tab>
          </div>
          <save_query_dialog v-if="extraoptionObj" :show="saveQueryFormVisible" :extraoption="extraoptionObj" v-on:closeModal="closeModal"></save_query_dialog>
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
                        <kap-editor width="99%" height="80" lang="sql" theme="chrome" v-model="savequery.sql" dragbar="#393e53" ref="saveQueries" v-show="savequery.isShow" class="ksd-mt-6">
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
import querypanel from './query_panel'
import queryresult from './query_result'
import saveQueryDialog from './save_query_dialog'
import DataSourceBar from '../common/DataSourceBar'
import { kapConfirm, hasRole, hasPermission, handleSuccess, handleError } from '../../util/business'
import { mapActions, mapGetters } from 'vuex'
import { permissions, insightKeyword } from '../../config'
@Component({
  methods: {
    ...mapActions({
      getSavedQueries: 'GET_SAVE_QUERIES',
      delQuery: 'DELETE_QUERY',
      loadDataSourceByProject: 'LOAD_DATASOURCE'
    })
  },
  components: {
    querypanel,
    queryresult,
    tab,
    'save_query_dialog': saveQueryDialog,
    DataSourceBar
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales: {
    'en': {dialogHiveTreeNoData: 'Please click data source to load source tables', trace: 'Trace', savedQueries: 'Save Queries', queryBox: 'Query Box', more: 'More'},
    'zh-cn': {dialogHiveTreeNoData: '请点击数据源来加载源表', trace: '追踪', savedQueries: '保存的查询', queryBox: '查询窗口', more: '更多'}
  }
})
export default class NewQuery extends Vue {
  hasLimit = true
  listRows = 500
  project = localStorage.getItem('selected_project')
  sourceSchema = ''
  isHtrace = false
  tableData = []
  saveQueryFormVisible = false
  savedQueryListVisible = false
  editableTabs = []
  activeSubMenu = 'Query1'
  savedQuriesSize = 0
  queryCurrentPage = 1
  extraoptionObj = null
  datasource = []
  savedList = []
  savedSize = 0
  checkedQueryList = []

  handleAutoComplete (data) {
    this.setCompleteData([...data, ...insightKeyword])
  }
  toggleDetail (index) {
    this.savedList[index].isShow = !this.savedList[index].isShow
  }
  resetQuery () {
    this.sourceSchema = ''
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
  addTab (targetName, componentName, extraData) {
    this.extraoptionObj = extraData
    let tabs = this.editableTabs
    let hasTab = false
    tabs.forEach((tab, index) => {
      if (tab.name === targetName) {
        this.activeSubMenu = targetName
        hasTab = true
      }
    })
    if (!hasTab) {
      var tabIndex = this.editableTabs.length ? this.editableTabs[this.editableTabs.length - 1].index + 1 : 1
      extraData.index = tabIndex
      var tabName = targetName + tabIndex
      this.editableTabs.push({
        title: tabName,
        name: tabName,
        content: componentName,
        icon: 'el-icon-loading',
        spin: true,
        extraoption: extraData,
        index: tabIndex
      })
      this.activeSubMenu = tabName
    }
  }
  delTab (targetName) {
    let tabs = this.editableTabs
    let activeName = this.activeSubMenu
    if (activeName === targetName) {
      tabs.forEach((tab, index) => {
        if (tab.name === targetName) {
          let nextTab = tabs[index + 1] || tabs[index - 1]
          if (nextTab) {
            activeName = nextTab.name
          }
        }
      })
    }
    this.editableTabs = tabs.filter(tab => tab.name !== targetName)
    this.activeSubMenu = activeName
  }
  clickTable (leaf) {
    if (leaf) {
      var tipsName = leaf.label
      var editor = this.$refs.insightBox
      editor.$emit('focus')
      editor.$emit('insert', tipsName)
      this.sourceSchema = editor.getValue()
    }
  }
  changeLimit () {
    if (this.hasLimit) {
      this.listRows = 500
    } else {
      this.listRows = 0
    }
  }
  changeTrace () {
    if (this.isHtrace) {
      kapConfirm(this.$t('htraceTips'))
    }
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
        this.addTab('query', 'querypanel', queryObj)
      })
    }
    this.savedQueryListVisible = false
  }
  changeTab (index, data, icon, componentName) {
    let tabs = this.editableTabs
    for (var k = 0; k < tabs.length; k++) {
      if (tabs[k].index === index) {
        tabs[k].content = componentName || 'queryresult'
        tabs[k].icon = icon || 'el-icon-success'
        tabs[k].spin = icon === 'circle-o-notch'
        tabs[k].extraoption.data = data
        break
      }
    }
  }
  openSaveQueryDialog () {
    this.$nextTick(() => {
      this.saveQueryFormVisible = true
    })
  }
  closeModal () {
    this.saveQueryFormVisible = false
  }
  openSaveQueryListDialog () {
    this.savedQueryListVisible = true
    this.checkedQueryList = []
    this.savedList = []
    this.loadSavedQuery()
  }
  submitQuery () {
    var queryObj = {
      acceptPartial: true,
      limit: this.listRows,
      offset: 0,
      project: this.currentSelectedProject,
      sql: this.sourceSchema,
      backdoorToggles: {
        DEBUG_TOGGLE_HTRACE_ENABLED: this.isHtrace
      }
    }
    this.addTab('query', 'querypanel', queryObj)
  }
  hasProjectAdminPermission () {
    return hasPermission(this, permissions.ADMINISTRATION.mask)
  }
  setCompleteData (data) {
    this.$refs.insightBox.$emit('setAutoCompleteData', data)
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  get showHtrace () {
    return this.$store.state.system.showHtrace === 'true'
  }
  async mounted () {
    if (!this.currentSelectedProject) {
      return
    }
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #newQuery {
    position: relative;
    border-top: 1px solid @line-border-color;
    height: 100%;
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
            bottom: 10px;
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
    }
    .query_result_box{
      border: 0;
      h3{
        margin: 20px;
      }
      .el-tabs{
        margin-top: 12px;
        .el-tabs__nav{
          margin-left: 20px;
        }
        .el-tabs__content{
          padding: 0px;
          .el-tab-pane{
            padding: 10px 20px 20px 20px;
          }
        }
      }
    }
    .el-icon-success {
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
