<template>
  <div id="queryTab">
    <!-- for guide start -->
    <el-button @click="handleForGuide" style="position: absolute;" v-visible v-guide.queryTriggerBtn></el-button>
    <!-- for guide end -->
    <div class="query_panel_box ksd-mb-30">
      <kap-editor ref="insightBox" :class="{'guide-WorkSpaceEditor':isWorkspace}" height="170" lang="sql" theme="chrome" @keydown.meta.enter.native="submitQuery(sourceSchema)" @keydown.ctrl.enter.native="submitQuery(sourceSchema)" v-model="sourceSchema" :readOnly="!isWorkspace">
      </kap-editor>
      <div class="clearfix operatorBox">
        <p class="tips_box">
          <el-button size="small" @click.native="openSaveQueryDialog" :disabled="!sourceSchema">{{$t('kylinLang.query.saveQuery')}}</el-button><el-button
          size="small" plain="plain" @click.native="resetQuery" v-if="isWorkspace" style="display:inline-block">{{$t('kylinLang.query.clear')}}</el-button>
        </p>
        <p class="operator" v-if="isWorkspace">
          <el-form :inline="true" class="demo-form-inline">
            <el-form-item v-show="showHtrace">
              <el-checkbox v-model="isHtrace" @change="changeTrace">{{$t('trace')}}</el-checkbox>
            </el-form-item><el-form-item>
              <el-checkbox v-model="hasLimit" @change="changeLimit">Limit</el-checkbox>
            </el-form-item><el-form-item>
              <el-input placeholder="" size="small" style="width:90px;" @input="handleInputChange" v-model="listRows" class="limit-input"></el-input>
            </el-form-item><el-form-item>
              <el-button type="primary" v-guide.workSpaceSubmit  plain size="small" class="ksd-btn-minwidth" :disabled="!sourceSchema" :loading="isLoading" @click="submitQuery(sourceSchema)">{{$t('kylinLang.common.submit')}}</el-button>
            </el-form-item>
          </el-form>
        </p>
      </div>
      <div class="submit-tips" v-if="isWorkspace">
        <i class="el-icon-ksd-info ksd-fs-12" ></i>
        Control / Command + Enter = <span>{{$t('kylinLang.common.submit')}}</span></div>
    </div>
    <div v-show="isLoading" class="ksd-center ksd-mt-10">
      <el-progress type="circle" :percentage="percent"></el-progress>
    </div>
    <div id="queryPanelBox" v-if="extraoptionObj&&errinfo">
      <div class="resultTipsLine">
        <div class="resultTips">
          <p class="resultText" v-if="extraoptionObj.queryId">
            <span class="label">{{$t('kylinLang.query.query_id')}}: </span>
            <span class="text" v-if="extraoptionObj.queryId">{{extraoptionObj.queryId}}</span>
            <common-tip :content="$t('linkToSpark')" v-if="extraoptionObj.appMasterURL">
              <a target="_blank" :href="extraoptionObj.appMasterURL"><i class="el-icon-ksd-go"></i></a>
            </common-tip>
          </p>
          <!-- <p class="resultText"><span class="label">{{$t('kylinLang.query.status')}}</span>
          <span class="ky-error">{{$t('kylinLang.common.error')}}</span></p> -->
        </div>
        <pre class="error-block" v-html="errinfo"></pre>
      </div>
    </div>
    <queryresult :extraoption="extraoptionObj" :isWorkspace="isWorkspace" v-if="extraoptionObj&&!errinfo" :queryExportData="tabsItem.queryObj"></queryresult>
    <save_query_dialog :show="saveQueryFormVisible" :sql="this.sourceSchema" :project="currentSelectedProject" v-on:closeModal="closeModal"></save_query_dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import queryresult from './query_result'
import saveQueryDialog from './save_query_dialog'
import { kapConfirm, handleSuccess, handleError } from '../../util/business'
@Component({
  props: ['tabsItem', 'completeData', 'tipsName'],
  methods: {
    ...mapActions({
      query: 'QUERY_BUILD_TABLES'
    })
  },
  components: {
    queryresult,
    'save_query_dialog': saveQueryDialog
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales: {
    'en': {
      trace: 'Trace',
      queryBox: 'Query Box',
      linkToSpark: 'Jump to Spark Web UI'
    },
    'zh-cn': {
      trace: '追踪',
      queryBox: '查询窗口',
      linkToSpark: '跳转至 Spark 任务详情'
    }
  }
})
export default class QueryTab extends Vue {
  hasLimit = true
  listRows = 500
  sourceSchema = ''
  isHtrace = false
  saveQueryFormVisible = false
  extraoptionObj = null
  isLoading = false
  percent = 0
  ST = null
  errinfo = ''
  isWorkspace = true
  // 为了guide
  handleForGuide (obj) {
    let action = obj.action
    let data = obj.data
    if (action === 'intoEditor') {
      this.activeSubMenu = 'WorkSpace'
    } else if (action === 'inputSql') {
      this.sourceSchema = data
    } else if (action === 'requestSql') {
      const queryObj = {
        acceptPartial: true,
        limit: 500,
        offset: 0,
        project: this.currentSelectedProject,
        sql: data,
        backdoorToggles: {
          DEBUG_TOGGLE_HTRACE_ENABLED: false
        }
      }
      this.query(queryObj)
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
  openSaveQueryDialog () {
    this.saveQueryFormVisible = true
  }
  closeModal () {
    this.saveQueryFormVisible = false
  }
  handleInputChange (value) {
    this.$nextTick(() => {
      this.listRows = (isNaN(value) || value === '' || value < 0) ? 0 : Number(value)
    })
  }
  resetQuery () {
    this.$emit('resetQuery')
    this.$nextTick(() => {
      this.sourceSchema = ''
      this.extraoptionObj = null
      this.errinfo = ''
      const editor = this.$refs.insightBox
      editor.$emit('setValue', '')
    })
  }
  submitQuery (querySql) {
    if (!this.isWorkspace || !querySql) {
      return
    }
    const queryObj = {
      acceptPartial: true,
      limit: this.listRows,
      offset: 0,
      project: this.currentSelectedProject,
      sql: querySql,
      backdoorToggles: {
        DEBUG_TOGGLE_HTRACE_ENABLED: this.isHtrace
      }
    }
    this.$emit('addTab', 'query', queryObj)
  }
  resetResult () {
    this.extraoptionObj = null
    this.errinfo = ''
    this.isLoading = false
    this.queryLoading()
    this.$nextTick(() => {
      this.isLoading = true
    })
  }
  queryResult (queryObj) {
    this.resetResult()
    this.query(queryObj).then((res) => {
      clearInterval(this.ST)
      handleSuccess(res, (data) => {
        this.isLoading = false
        this.extraoptionObj = data
        if (data.isException) {
          this.errinfo = data.exceptionMessage
          this.$emit('changeView', this.tabsItem.index, data, data.exceptionMessage)
        } else {
          this.errinfo = ''
          this.$emit('changeView', this.tabsItem.index, data)
        }
      })
    }, (res) => {
      this.isLoading = false
      handleError(res, (data, code, status, msg) => {
        this.errinfo = msg || this.$t('kylinLang.common.timeOut')
        this.$emit('changeView', this.tabsItem.index, data, this.errinfo)
      })
    })
  }
  queryLoading () {
    var _this = this
    this.percent = 0
    clearInterval(this.ST)
    this.ST = setInterval(() => {
      var randomPlus = Math.round(10 * Math.random())
      if (_this.percent + randomPlus < 99) {
        _this.percent += randomPlus
      } else {
        clearInterval(_this.ST)
      }
    }, 300)
  }
  get showHtrace () {
    return this.$store.state.system.showHtrace === 'true'
  }
  @Watch('completeData')
  onCompleteDataChange (val) {
    if (val) {
      this.$refs.insightBox.$emit('setAutoCompleteData', this.completeData)
    }
  }
  @Watch('tipsName')
  onTipsNameChange (val) {
    if (val && this.$parent.name === 'WorkSpace' && this.$parent.active) {
      const editor = this.$refs.insightBox
      editor.$emit('focus')
      editor.$emit('insert', val)
      this.sourceSchema = editor.getValue()
    }
  }
  @Watch('tabsItem.queryObj')
  onTabsItemChange (val) {
    if (val) {
      this.extraoptionObj = this.tabsItem.extraoption
      this.errinfo = this.tabsItem.queryErrorInfo
      this.sourceSchema = this.tabsItem.queryObj && this.tabsItem.queryObj.sql || ''
      this.isWorkspace = this.tabsItem.name === 'WorkSpace'
      if (this.tabsItem.queryObj && this.tabsItem.index) {
        this.queryResult(this.tabsItem.queryObj)
      } else {
        this.resetResult()
      }
    }
  }
  @Watch('tabsItem.extraoption')
  onTabsResultChange (val) {
    this.isLoading = false
    this.extraoptionObj = this.tabsItem.extraoption
    this.errinfo = this.tabsItem.queryErrorInfo
  }
  @Watch('tabsItem.queryErrorInfo')
  onQueryException (val) {
    this.isLoading = false
    this.errinfo = this.tabsItem.queryErrorInfo
  }
  @Watch('tabsItem.cancelQuery')
  onCancelQuery (val) {
    this.$nextTick(() => {
      if (val && this.isLoading) {
        this.isLoading = false
        this.$emit('changeView', 0, null, '')
      }
    })
  }
  destoryed () {
    clearInterval(this.ST)
  }
  created () {
    this.extraoptionObj = this.tabsItem.extraoption
    this.errinfo = this.tabsItem.queryErrorInfo
    this.sourceSchema = this.tabsItem.queryObj && this.tabsItem.queryObj.sql || ''
    this.isWorkspace = this.tabsItem.name === 'WorkSpace'
    if (this.tabsItem.queryObj && !this.tabsItem.extraoption && this.tabsItem.index) {
      this.queryResult(this.tabsItem.queryObj)
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  #queryPanelBox {
    .el-progress{
      margin-bottom: 10px;
    }
    .error-block {
      height: 200px;
      overflow-y: scroll;
      background-color: @fff;
      border: 1px solid @grey-2;
      padding: 10px;
      margin-top: 6px;
    }
  }
</style>
