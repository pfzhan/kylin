<template>
  <div class="insight_box ksd-border-tab">
  	<div class="ksd_left_bar">
     <tree :treedata="tableData" placeholder="输入关键字过滤"  :indent="4" :expandall="true" :showfilter="true" :allowdrag="false" @nodeclick="clickTable"></tree>
    </div>
    <div class="ksd_right_box">
	 <el-tabs type="border-card" v-model="activeMenu" class="query_box">
	  <el-tab-pane :label="$t('newQuery')" name="first">
      <editor v-model="sourceSchema" lang="sql" theme="chrome" width="100%" height="200" useWrapMode="true"></editor>
      <p class="tips_box">{{$t('tips')}}</p>
      <p class="ksd-right">
        <el-form :inline="true" class="demo-form-inline">
          <el-form-item label="Limit">
            <el-input  placeholder="" style="width:90px;" v-model="listRows"></el-input>
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="submitQuery">{{$t('kylinLang.common.submit')}}</el-button>
          </el-form-item>
        </el-form>
      </p>
    </el-tab-pane>
	  <el-tab-pane :label="$t('saveQueries')" name="second">
    <kap-nodata v-if="!savedSize"></kap-nodata>
    <div>
       <el-form  label-width="90px"  v-for="savequery in savedList" :key="savequery.name">
        <el-form-item label="SQL Name:" class="ksd-mb-2" >
          {{savequery.name}}
        </el-form-item>
        <el-form-item label="Project:" class="ksd-mb-2" >
          {{savequery.project}}
        </el-form-item>
        <el-form-item label="Description:" class="ksd-mb-2" >
          {{savequery.description}}
        </el-form-item>
        <el-collapse >
        <div class="ksd-fright"> 
        <kap-icon-button  icon="refresh" type="primary" @click.native="resubmit(savequery.sql)">Resubmit</kap-icon-button>
        <kap-icon-button  icon="close" type="danger" @click.native="removeQuery(savequery.id)">Remove</kap-icon-button>
        </div>
          <el-collapse-item title="SQL" name="1">
            <editor v-model="savequery.sql" lang="sql" theme="chrome" class="ksd-mt-20" width="100%" height="200" useWrapMode="true"></editor>
          </el-collapse-item>
        </el-collapse>  
      </el-form>
      </div>
      <!-- <div v-for="(savequery, index) in savedList"> {{savequery.name}}</div> -->
      <pager ref="savedQueryPager" class="ksd-center" :totalSize="savedSize"  v-on:handleCurrentChange='pageCurrentChange' ></pager>
    </el-tab-pane>
	  <el-tab-pane :label="$t('queryHistory')" name="third">
    <div>
      <kap-nodata v-if="!cookieQuerySize"></kap-nodata>
      <el-form  label-width="90px"  v-for="query in cookieQueries" :key="query.queryTime">
        <el-form-item label="Queried At:" class="ksd-mb-2" >
          {{query.queryTime|gmtTime}} in Project: {{ project }}
         
        </el-form-item>
        <el-collapse >
         <div class="ksd-fright">
          <kap-icon-button icon="refresh" type="primary" @click.native="resubmit(query.sql)">Resubmit</kap-icon-button>
          <kap-icon-button icon="close" type="danger" @click.native="removeQueryCache(query.sql)">Remove</kap-icon-button>
          </div>
          <el-collapse-item title="SQL" name="1">
            <editor v-model="query.sql" lang="sql" theme="chrome" class="ksd-mt-20" width="100%" height="200" useWrapMode="true"></editor>
          </el-collapse-item>
        </el-collapse>  
      </el-form>
      </div>
      <pager ref="savedQueryPagerForCookie" class="ksd-center" :totalSize="cookieQuerySize"  v-on:handleCurrentChange='pageCurrentChangeForCookie' ></pager>
    </el-tab-pane>
	</el-tabs>
  <div class="query_result_box ksd-border-tab">
     <div>
     <h3 class="ksd-inline">{{$t('result')}}</h3>
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
      </div>

     <tab type="border-card" class="insight_tab" v-on:addtab="addTab" :isedit="true"   :tabslist="editableTabs"  :active="activeSubMenu"  v-on:removetab="delTab">
       <template scope="props">
        <component :is="props.item.content" v-on:changeView="changeTab" v-on:reloadSavedProject="loadSavedQuery" :extraoption="props.item.extraoption"></component>
       </template>
     </tab>
  </div>
    </div>
  </div>
</template>
<script>
import editor from 'vue2-ace-editor'
import tab from '../common/tab'
import querypanel from 'components/insight/query_panel'
import queryresult from 'components/insight/query_result'
import { mapActions } from 'vuex'
import {groupData, indexOfObjWithSomeKey} from '../../util/index'
import { handleSuccess } from '../../util/business'
import { pageCount } from '../../config'
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
    this.cookieQueries = Object.assign([], this.cacheQuery[this.project] && this.cacheQuery[this.project].slice(0, pageCount) || [])
    this.cookieQuerySize = this.cacheQuery[this.project] && this.cacheQuery[this.project].length || 0
  },
  data () {
    return {
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
      queryCurrentPage: 1
    }
  },
  computed: {
    savedSize () {
      return this.$store.state.datasource.savedQueriesSize
    },
    savedList () {
      return this.$store.state.datasource.savedQueries
    }
  },
  methods: {
    ...mapActions({
      loadBuildCompleteTables: 'LOAD_BUILD_COMPLETE_TABLES',
      query: 'QUERY_BUILD_TABLES',
      getSavedQueries: 'GET_SAVE_QUERIES',
      delQuery: 'DELETE_QUERY'
    }),
    loadSavedQuery (pageIndex) {
      this.getSavedQueries({
        projectName: this.project,
        pageData: {
          pageSize: pageCount,
          pageOffset: pageIndex
        }
      })
    },
    pageCurrentChange (currentPage) {
      this.queryCurrentPage = currentPage
      this.getSavedQueries({
        projectName: this.project,
        pageData: {
          pageSize: this.$refs.savedQueryPager.pageSize,
          pageOffset: currentPage - 1
        }
      })
    },
    pageCurrentChangeForCookie (currentPage) {
      this.cookieQueryCurrentPage = currentPage
      this.cookieQueries = Object.assign([], this.cacheQuery[this.project].slice((currentPage - 1) * pageCount, currentPage * pageCount))
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
          icon: 'circle-o-notch',
          spin: true,
          extraoption: extraData,
          index: tabIndex
        })
        this.activeSubMenu = tabName
        this.addQueryInCache(extraData.sql)
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
    //     changeArrObject(this.editableTabs, 'icon', 'el-icon-circle-close', 'disabled', false, this)
    //   } else if (state === 3) {
    //     changeArrObject(this.editableTabs, '*', 'disabled', true, this)
    //     changeArrObject(this.editableTabs, 'icon', 'el-icon-loading', 'disabled', false, this)
    //   }
    //   console.log(this.editableTabs, 'ffff')
    // },
    addQueryInCache (sql) {
      this.cacheQuery[this.project] = this.cacheQuery[this.project] || []
      if (indexOfObjWithSomeKey(this.cacheQuery[this.project], 'sql', sql) < 0) {
        this.cacheQuery[this.project].push({sql: sql, queryTime: Date.now()})
      }
      localStorage.setItem('queryCache', JSON.stringify(this.cacheQuery))
    },
    changeTab (index, data, icon, componentName) {
      let tabs = this.editableTabs
      for (var k = 0; k < tabs.length; k++) {
        if (tabs[k].index === index) {
          tabs[k].content = componentName || 'queryresult'
          tabs[k].icon = icon || 'check-circle'
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
    },
    removeQuery (queryId) {
      this.$confirm('此操作将永久删除该条记录, 是否继续?', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        this.delQuery(queryId).then((response) => {
          this.$message({
            type: 'success',
            message: '删除成功!'
          })
          this.loadSavedQuery(this.queryCurrentPage - 1)
        })
      }).catch(() => {
        this.$message({
          type: 'info',
          message: '已取消删除'
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
    clickTable () {
    },
    submitQuery () {
      var queryObj = {
        acceptPartial: true,
        limit: this.listRows,
        offset: 0,
        project: this.project,
        sql: this.sourceSchema
      }
      this.addTab('query', 'querypanel', queryObj)
      this.pageCurrentChangeForCookie(this.cookieQueryCurrentPage || 1)
    }
  },
  watch: {
    defaultQueryStatus (val) {
      this.filterTab(val)
    }
  },
  mounted () {
    if (!this.project) {
      return
    }
    this.loadBuildCompleteTables(this.project).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        var databaseObj = groupData(data, 'table_SCHEM')
        for (var i in databaseObj) {
          var obj = {
            label: i,
            children: []
          }
          var tableData = databaseObj[i]
          for (var s = 0; s < tableData.length; s++) {
            var tableObj = {
              label: tableData[s].table_NAME,
              children: [],
              tags: tableData[s].type.map((tag) => {
                return tag[0]
              })
            }
            for (var m = 0; m < tableData[s].columns.length; m++) {
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
      })
    })
  },
  components: {
    editor,
    querypanel,
    queryresult,
    tab
  },
  locales: {
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin', newQuery: 'New Query', saveQueries: 'Save Queries', queryHistory: 'Query History', tips: 'Tips: Ctrl+Shift+Space or Alt+Space(Windows), Command+Option+Space(Mac) to list tables/columns in query box.', result: 'Result'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员', newQuery: '新查询', saveQueries: '保存的查询', queryHistory: '查询历史', tips: '技巧: Ctrl+Shift+Space 或 Alt+Space(Windows), Command+Option+Space(Mac) 可以在查询框中列出表/列名.', result: '查询结果'}
  }
}
</script>
<style lang="less">
  .insight_box {
    position: relative;
    .el-icon-circle-close {
      color: red;
    }
    .el-collapse{
      border:none;
    }
    .tree_box{
      margin-top: 20px;
      .tag_D{
        color:#48576a;
        border:solid 1px #48576a;
      }
      .tag_M{
        color:#20a0ff;
        border:solid 1px #20a0ff;
      }
      .tag_L{
        color:#48576a;
        border:solid 1px #48576a;
        border-radius: 0
      }
      .tag_F{
        color:#20a0ff;
        border:solid 1px #20a0ff;
        border-radius: 0
      }
      .tag_PK{
        color:#48576a;
        border:solid 1px #48576a;
        width: 18px;
      }
      .tag_FK{
        color:#20a0ff;
        border:solid 1px #20a0ff;
        width: 18px;
      }

    }
    .query_box.el-tabs{
      margin: 20px;
    }
    .tips_box{
      font-size: 12px;
    }
    .query_result_box{
      border-top: solid 1px #d1dbe5;
      h3{
        margin: 20px;
      }
      .el-tabs{
        margin-top: 20px;
        .el-tabs__nav{
          margin-left: 20px;
        }
        .el-tabs__content{
          padding: 0px;
          .el-tab-pane{
            padding: 20px;
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
</style>

