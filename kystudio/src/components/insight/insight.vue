<template>
  <div class="insight_box ksd-border-tab">
  	<div class="ksd_left_bar" id="input-inner">
     <tree class="insight-search" :empty-text="$t('treeNoData')" :expandnodeclick="false" :treedata="tableData" :placeholder="$t('kylinLang.common.pleaseFilter')"  :indent="4" :expandall="false" :showfilter="true" :allowdrag="false" @contentClick="clickTable" maxlevel="4"></tree>
    </div>
    <div class="ksd_right_box">
	 <el-tabs type="card" v-model="activeMenu" class="query_box" @tab-click="handleClick">
     <el-tab-pane :label="$t('newQuery')" name="first">
      <kap_editor ref="insightBox" height="170" lang="sql" theme="chrome" v-model="sourceSchema">
      </kap_editor>
       <div class="clearfix operatorBox">
         <p class="tips_box">{{$t('tips')}}</p>
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
               <el-button type="primary" size="small" @click="submitQuery">{{$t('kylinLang.common.submit')}}</el-button>
             </el-form-item>
           </el-form>
         </p>
       </div>
     </el-tab-pane>
	  <el-tab-pane :label="$t('saveQueries')" name="second">
    <!-- <kap-nodata v-if="!savedSize"></kap-nodata> -->
    <div v-if="!savedSize" class="nodata">
      <div class="ksd-mb-20"><img src="../../assets/img/save_query.png" style="height:80px;"></div>
      <span>{{$t('kylinLang.common.noData')}}</span>
    </div>
    <div>
       <el-form v-for="savequery in savedList" :key="savequery.name" class="narrowForm">
        <el-form-item label="" class="ksd-mb-2 narrowFormItem" >
          <span>{{savequery.name}}</span>
        </el-form-item>
        <el-form-item label="" class="ksd-mb-2 narrowFormItem"  style="margin-left:0;">
          <span><i class="el-icon-ksd-project"></i> {{savequery.project}}</span>
        </el-form-item>
        <el-form-item label="" class="ksd-mb-2 narrowFormItem" >
          <span><i class="el-icon-ksd-desc" v-if="savequery.description"></i> {{savequery.description}}</span>
        </el-form-item>
        <el-collapse >
          <el-collapse-item title="SQL" name="1" style="color:#9095ab;font-size:12px">
            <editor v-model="savequery.sql" ref="saveQueries" lang="sql" theme="chrome" width="99%" height="200"></editor>
          </el-collapse-item>
          <div class="btn-group">
            <kap-icon-button type="primary" plain size="small" @click.native="resubmit(savequery.sql)">{{$t('kylinLang.common.submit')}}</kap-icon-button>
            <kap-icon-button size="small" @click.native="removeQuery(savequery.id)">{{$t('kylinLang.common.delete')}}</kap-icon-button>
          </div>
        </el-collapse>
      </el-form>
      </div>
      <!-- <div v-for="(savequery, index) in savedList"> {{savequery.name}}</div> -->
      <pager ref="savedQueryPager" class="ksd-center pagerMbReset" :totalSize="savedSize"  v-on:handleCurrentChange='pageCurrentChange' ></pager>
    </el-tab-pane>
	  <el-tab-pane :label="$t('queryHistory')" name="third">
    <div class="cookieQueries">
      <!-- <kap-nodata v-if="!cookieQuerySize"></kap-nodata> -->
      <div v-if="!cookieQuerySize" class="nodata">
      <div class="ksd-mb-20"><img src="../../assets/img/save_query.png" style="height:80px;"></div>
      <span>{{$t('noQueryHistory')}}</span>
    </div>
      <el-form  v-for="query in cookieQueries" :key="query.queryTime" class="narrowForm" style="min-height: 80px;">
        <el-form-item class="ksd-mb-2 narrowFormItem" ></el-form-item>
        <el-form-item class="ksd-mb-2 narrowFormItem" style="margin-left:0;">
        <span><i class="el-icon-ksd-project"></i> {{ project }}</span>
        </el-form-item>
        <el-form-item class="ksd-mb-2 narrowFormItem" >
        <span><i class="el-icon-ksd-calendar"></i> {{transToGmtTime(query.queryTime)}}</span>
        </el-form-item>
        <el-collapse >
          <el-collapse-item title="SQL" name="1">
            <editor v-model="query.sql" ref="cacheQueries" lang="sql" theme="chrome" width="99%" height="200"></editor>
          </el-collapse-item>
          <div class="btn-group">
            <kap-icon-button type="primary" plain size="small" @click.native="resubmit(query.sql)">{{$t('kylinLang.common.submit')}}</kap-icon-button>
            <kap-icon-button size="small" @click.native="removeQueryCache(query.sql)">{{$t('kylinLang.common.delete')}}</kap-icon-button>
          </div>
        </el-collapse>
      </el-form>
      </div>
      <pager ref="savedQueryPagerForCookie" class="ksd-center pagerMbReset" :totalSize="cookieQuerySize"  v-on:handleCurrentChange='pageCurrentChangeForCookie' ></pager>
    </el-tab-pane>
	</el-tabs>
  <div class="line" style="margin:0 0 30px 20px" v-show='editableTabs&&editableTabs.length'></div>
  <div class="query_result_box ksd-border-tab" v-show='editableTabs&&editableTabs.length'>
     <!--<div>-->
     <!--<h3 class="ksd-inline ksd-mt-2 ksd-mb-6" style="font-size:14px;">{{$t('result')}}</h3>-->
      <!-- <el-form :inline="true" class="demo-form-inline ksd-fright ksd-mr-20 ksd-mt-20">
          <el-form-item label="Status">
           <el-select v-model="defaultQueryStatus" placeholder="请选择" style="width:90px">
              <el-option
                v-for="item in queryStatus"
                :label="item.label"
                :value="item.value">
              </el-option>
            </el-select>
          </el-form-item>
        </el-form> -->
      <!--</div>-->

     <tab type="card" class="insight_tab" v-on:addtab="addTab" :isedit="true"   :tabslist="editableTabs"  :active="activeSubMenu"  v-on:removetab="delTab">
       <template slot-scope="props">
        <component :is="props.item.content" v-on:changeView="changeTab" v-on:reloadSavedProject="loadSavedQuery" :extraoption="props.item.extraoption"></component>
       </template>
     </tab>
  </div>
    </div>
  </div>
</template>
<script>
import tab from '../common/tab'
import querypanel from 'components/insight/query_panel'
import queryresult from 'components/insight/query_result'
import { mapActions } from 'vuex'
// import 'vue2-ace-editor'
// import languageTool from 'brace/ext/language_tools'
import { groupData, indexOfObjWithSomeKey } from '../../util/index'
import { handleSuccess, kapConfirm, transToGmtTime } from '../../util/business'
import { pageCount, insightKeyword } from '../../config'
export default {
  name: 'insight',
  props: ['userDetail'],
  created () {
    this.$on('editRoleFormValid', (t) => {
      this.$emit('validSuccess', this.userDetail)
    })
    var localCache = JSON.parse(localStorage.getItem('queryCache') || '{}')
    this.cacheQuery[this.project] = this.cacheQuery[this.project] || []
    this.$set(this.cacheQuery, this.project, this.cacheQuery[this.project].concat(localCache[this.project] || []))
    this.loadSavedQuery(0)
    // this.cookieQueries = Object.assign([], this.cacheQuery[this.project] && this.cacheQuery[this.project].slice(0, pageCount) || [])
    this.pageCurrentChangeForCookie(1)
    this.cookieQuerySize = this.cacheQuery[this.project] && this.cacheQuery[this.project].length || 0
  },
  beforeRouteLeave (to, from, next) {
  // 导航离开该组件的对应路由时调用
  // 可以访问组件实例 `this`
    var hasEditTab = false
    this.editableTabs.forEach((tab) => {
      if (tab.icon === 'circle-o-notch') {
        hasEditTab = true
      }
    })
    if (hasEditTab) {
      this.$confirm(this.$t('willGo'), this.$t('kylinLang.common.tip'), {
        confirmButtonText: this.$t('go'),
        cancelButtonText: this.$t('kylinLang.common.cancel'),
        type: 'warning'
      }).then(() => {
        if (to.name === 'refresh') {
          next()
          this.$nextTick(() => {
            this.$router.replace('insight')
          })
          return
        }
        next()
      }).catch(() => {
        next(false)
      })
    } else {
      next()
    }
  },
  data () {
    return {
      hasLimit: true,
      listRows: 50000,
      activeMenu: 'first',
      tabCount: 0,
      activeSubMenu: 'Query1',
      project: localStorage.getItem('selected_project'),
      editableTabs: [],
      tableData: [{
        id: 1,
        label: 'Tables',
        children: []
      }],
      queryStatus: [{
        label: 'ALL',
        value: 0
      }, {
        label: 'Success',
        value: 1
      }, {
        label: 'Failed',
        value: 2
      }, {
        label: 'Executing',
        value: 3
      }],
      sourceSchema: '',
      modelAssets: [],
      defaultQueryStatus: 'ALL',
      savedQuries: [],
      savedQuriesSize: 0,
      cacheQuery: {},
      cookieQueries: [],
      cookieQuerySize: 0,
      cookieQueryCurrentPage: 1,
      queryCurrentPage: 1,
      isHtrace: false
    }
  },
  computed: {
    savedSize () {
      return this.$store.state.datasource.savedQueriesSize
    },
    savedList () {
      return this.$store.state.datasource.savedQueries
    },
    showHtrace () {
      return this.$store.state.system.showHtrace === 'true'
    }
  },
  methods: {
    ...mapActions({
      loadBuildCompleteTables: 'LOAD_BUILD_COMPLETE_TABLES',
      query: 'QUERY_BUILD_TABLES',
      getSavedQueries: 'GET_SAVE_QUERIES',
      delQuery: 'DELETE_QUERY'
    }),
    transToGmtTime: transToGmtTime,
    loadSavedQuery (pageIndex) {
      this.getSavedQueries({
        pageData: {
          project: this.project || null,
          pageSize: pageCount,
          pageOffset: pageIndex
        }
      })
    },
    pageCurrentChange (currentPage) {
      this.queryCurrentPage = currentPage
      this.getSavedQueries({
        pageData: {
          project: this.project || null,
          pageSize: this.$refs.savedQueryPager.pageSize,
          pageOffset: currentPage - 1
        }
      })
    },
    pageCurrentChangeForCookie (currentPage) {
      this.cookieQueryCurrentPage = currentPage
      var wantPagerQuery = this.cacheQuery[this.project].slice(0)
      wantPagerQuery.sort((a, b) => {
        return b.queryTime - a.queryTime
      })
      this.cookieQueries = Object.assign([], wantPagerQuery.slice((currentPage - 1) * pageCount, currentPage * pageCount))
      this.cookieQuerySize = this.cacheQuery[this.project] && this.cacheQuery[this.project].length || 0
    },
    addTab (targetName, componentName, extraData) {
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
    },
    changeTrace () {
      if (this.isHtrace) {
        kapConfirm(this.$t('htraceTips'))
      }
    },
    // filterTab (state) {
    //   if (state === 0) {
    //     changeArrObject(this.editableTabs, '*', 'disabled', false, this)
    //   } else if (state === 1) {
    //     changeArrObject(this.editableTabs, '*', 'disabled', true, this)
    //     changeArrObject(this.editableTabs, 'icon', 'el-icon-circle-check', 'disabled', false, this)
    //   } else if (state === 2) {
    //     changeArrObject(this.editableTabs, '*', 'disabled', true, this)
    //     changeArrObject(this.editableTabs, 'icon', 'el-icon-ksd-error_01', 'disabled', false, this)
    //   } else if (state === 3) {
    //     changeArrObject(this.editableTabs, '*', 'disabled', true, this)
    //     changeArrObject(this.editableTabs, 'icon', 'el-icon-loading', 'disabled', false, this)
    //   }
    //   console.log(this.editableTabs, 'ffff')
    // },
    addQueryInCache (sql) {
      this.cacheQuery[this.project] = this.cacheQuery[this.project] || []
      // if (indexOfObjWithSomeKey(this.cacheQuery[this.project], 'sql', sql) < 0) {
      //   this.cacheQuery[this.project].push({sql: sql, queryTime: Date.now()})
      // }
      this.cacheQuery[this.project].push({sql: sql, queryTime: Date.now()})
      localStorage.setItem('queryCache', JSON.stringify(this.cacheQuery))
    },
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
    },
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
    },
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
    },
    removeQueryCache (sql) {
      this.cacheQuery[this.project] = this.cacheQuery[this.project] || []
      var sqlIndexInCache = indexOfObjWithSomeKey(this.cacheQuery[this.project], 'sql', sql)
      if (sqlIndexInCache >= 0) {
        this.cacheQuery[this.project].splice(sqlIndexInCache, 1)
        // this.cookieQuerySize = this.cookieQuerySize - 1 > 0 ? this.cookieQuerySize - 1 : 0
      }
      localStorage.setItem('queryCache', JSON.stringify(this.cacheQuery))
      this.pageCurrentChangeForCookie(this.cookieQueryCurrentPage)
    },
    tempSaveQueries () {
      localStorage.getItem('selected_project')
    },
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
    },
    clickTable (leaf) {
      if (leaf) {
        var tipsName = leaf.label
        var editor = this.$refs.insightBox.$refs.kapEditor.editor
        editor.focus()
        editor.insert(tipsName)
        this.sourceSchema = editor.getValue()
      }
    },
    changeLimit () {
      if (this.hasLimit) {
        this.listRows = 50000
      } else {
        this.listRows = 0
      }
    },
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
    },
    handleClick (tab, event) {
      if (tab.name === 'second') {
        var saveEditor = this.$refs.saveQueries
        if (saveEditor && saveEditor.length > 0) {
          for (let i = 0; i < saveEditor.length; i++) {
            saveEditor[i].editor.setOptions({
              wrap: 'free',
              enableBasicAutocompletion: true,
              enableSnippets: true,
              enableLiveAutocompletion: true
            })
          }
        }
      }
      if (tab.name === 'third') {
        var cacheEditor = this.$refs.cacheQueries
        if (cacheEditor && cacheEditor.length > 0) {
          for (let i = 0; i < cacheEditor.length; i++) {
            cacheEditor[i].editor.setOptions({
              wrap: 'free',
              enableBasicAutocompletion: true,
              enableSnippets: true,
              enableLiveAutocompletion: true
            })
          }
        }
      }
    }
  },
  watch: {
    defaultQueryStatus (val) {
      this.filterTab(val)
    }
  },
  mounted () {
    var editor = this.$refs.insightBox.$refs.kapEditor.editor
    var setCompleteData = function (data) {
      editor.completers.splice(0, editor.completers.length - 3)
      editor.completers.unshift({
        identifierRegexps: [/[.a-zA-Z_0-9]/],
        getCompletions: function (editor, session, pos, prefix, callback) {
          if (prefix.length === 0) {
            return callback(null, data)
          } else {
            return callback(null, data)
          }
        }
      })
    }
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
    // editor.setTheme('ace/theme/monokai')
    // editor.getSession().setMode('brace/mode/sql')
    // alert(screen.availHeight - 65 - 50 - 55)
    // let iHeight = screen.availHeight - 65 - 50 - 103
    let iHeight = document.documentElement.clientHeight - 66 - 48 - 69
    this.$el.querySelector('.filter-tree').style.height = iHeight + 'px'
    var autoCompeleteData = [].concat(insightKeyword)
    // setCompleteData(autoCompeleteData)
    if (!this.project) {
      return
    }
    this.loadBuildCompleteTables(this.project).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        if (data.length === 0) {
          this.tableData = []
        } else {
          this.tableData = [{
            id: 1,
            label: 'Tables',
            children: []
          }]
        }
        var databaseObj = groupData(data, 'table_SCHEM')
        for (var i in databaseObj) {
          var obj = {
            label: i,
            children: []
          }
          autoCompeleteData.push({meta: 'datasource', caption: i, value: i, scope: 1})
          var tableData = databaseObj[i]
          for (var s = 0; s < tableData.length; s++) {
            var tableName = tableData[s].table_NAME
            var tableObj = {
              label: tableName,
              children: [],
              tags: tableData[s].type.map((tag) => {
                return tag[0]
              })
            }
            autoCompeleteData.push({meta: 'table', caption: i + '.' + tableName, value: i + '.' + tableName, scope: 1})
            autoCompeleteData.push({meta: 'table', caption: tableName, value: tableName, scope: 1})
            for (var m = 0; m < tableData[s].columns.length; m++) {
              var columnName = tableData[s].columns[m].column_NAME
              autoCompeleteData.push({meta: 'column', caption: columnName, value: columnName, scope: 1})
              tableObj.children.push({
                label: tableData[s].columns[m].column_NAME,
                subLabel: tableData[s].columns[m].type_NAME,
                tags: tableData[s].columns[m].type.map((tag) => {
                  if (tag !== 'PK' && tag !== 'FK') {
                    return tag[0]
                  }
                  return tag
                })
              })
            }

            obj.children.push(tableObj)
          }
          this.tableData[0].children.push(obj)
        }
        setCompleteData(autoCompeleteData)
      })
    })
  },
  components: {
    querypanel,
    queryresult,
    tab
  },
  locales: {
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin', newQuery: 'New Query', saveQueries: 'Save Queries', queryHistory: 'Query History', tips: 'Tips: Click left tree to add table or columns in query box or press space key to show auto complete menu.', result: 'Result', 'willGo': 'You have unfinished request detected, Do you want to continue?', 'go': 'Continue go', 'treeNoData': 'No data', trace: 'Trace', htraceTips: 'Please make sure Zipkin server is properly deployed according to the manual of performance diagnose package.', noQueryHistory: 'Please try to save the frequent queries for later use.'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员', newQuery: '新查询', saveQueries: '保存的查询', queryHistory: '查询历史', tips: '技巧: 点击左侧树结构选中表名或者列名或按空格键触发提示。', result: '查询结果', 'willGo': '检测到有执行的请求，是否继续跳转？', 'go': '继续跳转', 'treeNoData': '暂无数据', trace: '追踪', htraceTips: '请确保已经按照性能诊断工具包使用说明部署完毕Zipkin服务器。', noQueryHistory: 'Please try to save the frequent queries for later use.'}
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .insight_box {
    position: relative;
    .nodata {
      text-align: center;
      margin-top: 220px;
    }
    .operatorBox{
      margin-top:10px;
      display:flex;
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
    .narrowForm {
      min-height: 100px;
      border-bottom: 1px solid @line-border-color;
    }
    .narrowFormItem{
      display: inline-block;
      margin-left: 10px;
      &:first-child {
        display: block;
        margin-left: 0;
        margin-top: 20px;
        .el-form-item__label,
        .el-form-item__content {
          font-size: 16px;
          color: @color-text-primary;
        }
      }
      .el-form-item__label,
      .el-form-item__content{
        line-height: 24px;
        color: @color-text-regular;
        i {
          cursor: default;
        }
      }
    }
    .cookieQueries .el-form{
      margin-bottom:10px;
      &:last-child{
        margin-bottom:0;
      }
    }
    .pagerMbReset{
      margin-bottom:20px!important;
    }
    .el-collapse-item__header {
      border-bottom: none;
      height: 48px;
      line-height: 28px;
    }
    .el-collapse-item__wrap {
      border-bottom: none;
      margin-top: -15px;
      .el-collapse-item__content {
        margin-bottom: 45px;
      }
    }
    .el-collapse-item > div {
      .el-collapse-item__arrow {
        line-height: 30px;
        float: none;
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
    .el-collapse{
      border:none;
      position: relative;
      .btn-group {
        position: absolute;
        bottom: 20px;
        right: 30px;
      }
    }
    .tree_box{
      height: 100%;
      margin-top: 16px;
      .tag_D{
        color:@base-color;
        border:solid 1px @base-color;
        border-radius: 100%;
        font-size: 12px;
      }
      .tag_M{
        color:@base-color;
        border:solid 1px @base-color;
        border-radius: 100%;
        font-size: 12px;
      }
      .tag_L{
        // color:@base-color;
        // border:solid 1px @base-color;
        border-radius: 0;
        font-size: 12px;
      }
      .tag_F{
        // color:@base-color;
        // border:solid 1px @base-color;
        border-radius: 0;
        font-size: 12px;
      }
      .tag_PK{
        color:@base-color;
        border:solid 1px @base-color;
        width: 18px;
        border-radius: 4px;
        font-size: 12px;
      }
      .tag_FK{
        color:@base-color;
        border:solid 1px @base-color;
        width: 18px;
        border-radius: 4px;
        font-size: 12px;
      }

    }
    .query_box.el-tabs{
      margin:0 20px 15px 20px;
      .el-tabs__nav-wrap{
        border-bottom: none;
      }
      .el-tabs__item {
        &.is-active,
        &:hover {
          color: @color-text-primary;
          font-weight: bolder;
        }
      }
    }
    .tips_box{
      font-size: 12px;
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
    .ace_print-margin{
      display: none!important;
    }
    .el-tabs__content {
      overflow: visible;
    }
  }
  .insight-search{
    .el-input__inner{
      height: 30px;
    }
  }
  .query_box{
    .el-tabs__header{
      margin-left: -271px;
      margin-bottom: 20px;
    }
  }
</style>

