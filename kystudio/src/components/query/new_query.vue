<template>
  <div id="newQuery">
    <div class="table-layout clearfix"> 
      <div class="layout-left">
        <DataSourceBar
          :is-show-load-source="true"
          :is-expand-on-click-node="false"
          :datasource="datasource"
          :expand-node-types="['datasouce', 'database']"
          @autoComplete="handleAutoComplete"
          @click="clickTable">
        </DataSourceBar>
      </div>
      <div class="layout-right">
        <div class="ksd_right_box">
          <div class="query_panel_box ksd-mb-20">
            <kap_editor ref="insightBox" height="170" lang="sql" theme="chrome" v-model="sourceSchema">
            </kap_editor>
            <div class="clearfix operatorBox">
              <p class="tips_box">
                <el-button size="small" plain="plain" icon="el-icon-ksd-sql" @click.native="openSaveQueryListDialog" style="display:inline-block">{{$t('kylinLang.query.reLoad')}}</el-button>
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
          </div>
          <!-- <div class="line" style="margin: 20px 0;" v-show='editableTabs&&editableTabs.length'></div> -->
          <div class="query_result_box ksd-border-tab" v-show='editableTabs&&editableTabs.length'>
            <tab type="card" class="insight_tab" v-on:addtab="addTab" :isedit="true" :tabslist="editableTabs" :active="activeSubMenu"  v-on:removetab="delTab">
              <template slot-scope="props">
                <component :is="props.item.content" v-on:changeView="changeTab" v-on:reloadSavedProject="loadSavedQuery" :extraoption="props.item.extraoption"></component>
              </template>
            </tab>
          </div>
          <save_query_dialog v-if="extraoptionObj" :show="saveQueryFormVisible" :extraoption="extraoptionObj" v-on:closeModal="closeModal" v-on:reloadSavedProject="reloadSavedProject"></save_query_dialog>
          <el-dialog
            :title="$t('savedQueries')"
            width="660px"
            :visible.sync="savedQueryListVisible">
            <div v-if="!savedSize" class="nodata">
              <div class="ksd-mb-20"><img src="../../assets/img/save_query.png" style="height:80px;"></div>
              <span>{{$t('kylinLang.common.noData')}}</span>
            </div>
            <div class="saved_query_content">
              <el-form v-for="(savequery, index) in savedList" :key="savequery.name" class="narrowForm">
                <el-form-item :label="$t('kylinLang.query.name')+' :'" class="ksd-mb-2 narrowFormItem" >
                  <span>{{savequery.name}}</span>
                </el-form-item>
                <!-- <el-form-item :label="$t('kylinLang.query.desc')+' :'" class="ksd-mb-2 narrowFormItem"  style="margin-left:0;">
                  <span>{{savequery.project}}</span>
                </el-form-item> -->
                <el-form-item :label="$t('kylinLang.query.desc')+' :'" class="ksd-mb-2 narrowFormItem" >
                  <span>{{savequery.description}}</span>
                </el-form-item>
                <el-form-item :label="$t('kylinLang.query.querySql')+' :'" prop="sql" class="ksd-mb-2 narrowFormItem">
                  <el-button plain size="mini" @click="toggleDetail(index)">
                    {{$t('kylinLang.common.seeDetail')}}
                    <i class="el-icon-arrow-down" v-show="!showDetail || (showDetail&&showDetailInd!==index)"></i>
                    <i class="el-icon-arrow-up" v-show="showDetail&&showDetailInd==index"></i>
                  </el-button>
                  <kap_editor width="99%" height="80" lang="sql" theme="chrome" v-model="savequery.sql" dragbar="#393e53" ref="saveQueries" v-show="showDetail&&showDetailInd==index" class="ksd-mt-6">
                  </kap_editor>
                </el-form-item>
                <div class="btn-group">
                  <kap-icon-button size="small" @click.native="removeQuery(savequery.id)">{{$t('kylinLang.common.delete')}}</kap-icon-button>
                  <kap-icon-button type="primary" plain size="small" @click.native="resubmit(savequery.sql)">{{$t('kylinLang.common.submit')}}</kap-icon-button>
                </div>
              </el-form>
            </div>
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
import { kapConfirm, hasRole, hasPermission } from '../../util/business'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync } from '../../util/index'
import { pageCount, permissions, insightKeyword } from '../../config'
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
    'en': {dialogHiveTreeNoData: 'Please click data source to load source tables', trace: 'Trace', savedQueries: 'Save Queries'},
    'zh-cn': {dialogHiveTreeNoData: '请点击数据源来加载源表', trace: '追踪', savedQueries: '保存的查询'}
  }
})
export default class NewQuery extends Vue {
  hasLimit = true
  listRows = 50000
  project = localStorage.getItem('selected_project')
  sourceSchema = ''
  isHtrace = false
  tableData = []
  saveQueryFormVisible = false
  savedQueryListVisible = false
  editableTabs = []
  activeSubMenu = 'Query1'
  cacheQuery = {}
  savedQuriesSize = 0
  cookieQueries = []
  cookieQuerySize = 0
  cookieQueryCurrentPage = 1
  queryCurrentPage = 1
  showDetail = false
  showDetailInd = -1
  extraoptionObj = null
  datasource = []

  openLoadDataSourceDialog () {}
  handleAutoComplete (data) {
    this.setCompleteData([...data, ...insightKeyword])
  }
  toggleDetail (index) {
    this.showDetail = !this.showDetail
    this.showDetailInd = index
  }
  loadSavedQuery (pageIndex) {
    this.getSavedQueries({
      pageData: {
        project: this.project || null,
        limit: this.listRows,
        offset: pageIndex
      }
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
      this.addQueryInCache(extraData.sql)
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
      var editor = this.$refs.insightBox.$refs.kapEditor.editor
      editor.focus()
      editor.insert(tipsName)
      this.sourceSchema = editor.getValue()
    }
  }
  changeLimit () {
    if (this.hasLimit) {
      this.listRows = 50000
    } else {
      this.listRows = 0
    }
  }
  changeTrace () {
    if (this.isHtrace) {
      kapConfirm(this.$t('htraceTips'))
    }
  }
  pageCurrentChange (currentPage) {
    this.queryCurrentPage = currentPage
    this.getSavedQueries({
      pageData: {
        projectName: this.project || null,
        pageSize: this.$refs.savedQueryPager.pageSize,
        pageOffset: currentPage - 1
      }
    })
  }
  pageCurrentChangeForCookie (currentPage) {
    this.cookieQueryCurrentPage = currentPage
    var wantPagerQuery = this.cacheQuery[this.project].slice(0)
    wantPagerQuery.sort((a, b) => {
      return b.queryTime - a.queryTime
    })
    this.cookieQueries = Object.assign([], wantPagerQuery.slice((currentPage - 1) * pageCount, currentPage * pageCount))
    this.cookieQuerySize = this.cacheQuery[this.project] && this.cacheQuery[this.project].length || 0
  }
  removeQuery (queryId) {
    kapConfirm(this.$t('kylinLang.common.confirmDel')).then(() => {
      this.delQuery(queryId).then((response) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.loadSavedQuery(this.queryCurrentPage - 1)
      })
    })
  }
  resubmit (sql) {
    var queryObj = {
      acceptPartial: true,
      limit: this.listRows,
      offset: 0,
      project: this.project,
      sql: sql
    }
    this.addTab('query', 'querypanel', queryObj)
    this.pageCurrentChangeForCookie(this.cookieQueryCurrentPage || 1)
    this.$nextTick(() => {
      document.getElementById('scrollBox').scrollTop = document.getElementById('scrollBox').scrollHeight + 1200
    })
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
  addQueryInCache (sql) {
    this.cacheQuery[this.project] = this.cacheQuery[this.project] || []
    this.cacheQuery[this.project].push({sql: sql, queryTime: Date.now()})
    localStorage.setItem('queryCache', JSON.stringify(this.cacheQuery))
  }
  openSaveQueryDialog () {
    this.$nextTick(() => {
      this.saveQueryFormVisible = true
    })
  }
  closeModal () {
    this.saveQueryFormVisible = false
  }
  reloadSavedProject () {
    this.$emit('reloadSavedProject', 0)
  }
  openSaveQueryListDialog () {
    this.savedQueryListVisible = true
    this.loadSavedQuery()
  }
  submitQuery () {
    var queryObj = {
      acceptPartial: true,
      limit: this.listRows,
      offset: 0,
      project: this.project,
      sql: this.sourceSchema,
      backdoorToggles: {
        DEBUG_TOGGLE_HTRACE_ENABLED: this.isHtrace
      }
    }
    this.addTab('query', 'querypanel', queryObj)
    this.pageCurrentChangeForCookie(this.cookieQueryCurrentPage || 1)
  }
  hasProjectAdminPermission () {
    return hasPermission(this, permissions.ADMINISTRATION.mask)
  }
  setCompleteData (data) {
    const editor = this.$refs.insightBox.$refs.kapEditor.editor

    editor.completers.splice(0, editor.completers.length - 3)
    editor.completers.unshift({
      identifierRegexps: [/[.a-zA-Z_0-9]/],
      getCompletions (editor, session, pos, prefix, callback) {
        if (prefix.length === 0) {
          return callback(null, data)
        } else {
          return callback(null, data)
        }
      }
    })
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  get savedSize () {
    return this.$store.state.datasource.savedQueriesSize
  }
  get savedList () {
    return this.$store.state.datasource.savedQueries
  }
  get showHtrace () {
    return this.$store.state.system.showHtrace === 'true'
  }
  async mounted () {
    var editor = this.$refs.insightBox.$refs.kapEditor.editor
    editor.commands.on('afterExec', function (e, t) {
      if (e.command.name === 'insertstring' && (e.args === ' ' || e.args === '.')) {
        var all = e.editor.completers
        // e.editor.completers = completers;
        e.editor.execCommand('startAutocomplete')
        e.editor.completers = all
      }
    })
    editor.setOptions({
      wrap: 'free',
      enableBasicAutocompletion: true,
      enableSnippets: true,
      enableLiveAutocompletion: true
    })
    if (!this.project) {
      return
    }
    const res = await this.loadDataSourceByProject({project: this.currentSelectedProject, isExt: true})
    this.datasource = await handleSuccessAsync(res)
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #newQuery {
    position: relative;
    border-top: 1px solid @line-border-color;
    height: 100%;
    #table_layout {
      width: 100%;
      height: 100%;
      display: table;
      #left_b {
        display: table-cell;
        width: 250px;
        vertical-align: top;
        height: 100%;
        clear: both;
        border-right: solid 1px #cfd8dc;
      }
      #right_b {
        display: table-cell;
        vertical-align: top;
        padding: 20px;
        .query_panel_box {
          border: 1px solid @line-border-color;
          border-top-left-radius: 5px;
          border-top-right-radius: 5px;
          .smyles_editor_wrap {
            margin: -1px;
            .ace-chrome {
              border-top-left-radius: 5px;
              border-top-right-radius: 5px;
              border-bottom-left-radius: 0;
              border-bottom-right-radius: 0;
            }
          }
        }
        .saved_query_content {
          height: 620px;
          overflow-y: scroll;
        }
      }
    }
    .nodata {
      text-align: center;
      margin: 80px 0;
    }
    .narrowForm {
      border: 1px solid @line-border-color;
      padding: 15px;
      margin-bottom: 10px;
      position: relative;
      background-color: @aceditor-bg-color;
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
        top: 15px;
        right: 20px;
      }
    }
    .operatorBox{
      margin-top: -3px;
      display: flex;
      padding: 16px;
      background-color: @grey-4;
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
  }
</style>
