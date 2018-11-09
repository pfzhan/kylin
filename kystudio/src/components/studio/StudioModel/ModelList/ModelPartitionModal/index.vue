<template>
  <el-dialog 
    :title="$t('setting')"
    width="660px"
    append-to-body
    :visible="isShow" 
    class="model-partition-dialog" 
    @close="isShow && handleClose(false)" 
    :close-on-press-escape="false" 
    :close-on-click-modal="false">     
    <div class="ky-list-title">分区设置</div>
    <!-- <div class="ky-list-sub-title">一级分区</div>
    <el-form :inline="true" :model="form" class="demo-form-inline">
      <el-form-item label="表">
        <el-select v-model="form.region" placeholder="请选择表">
          <el-option label="1" value="shanghai"></el-option>
          <el-option label="2" value="beijing"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="列">
        <el-select v-model="form.region" placeholder="请选择列">
          <el-option label="1" value="shanghai"></el-option>
          <el-option label="2" value="beijing"></el-option>
        </el-select>
      </el-form-item>
    </el-form> -->
    <!-- <div class="ky-list-sub-title ksd-mt-16">时间分区</div> -->
    <el-form :model="partitionMeta" label-width="85px" class="ksd-mt-16" label-position="top"> 
      <el-form-item :label="$t('partitionDateColumn')">
        <el-col :span="12">
          <el-form-item prop="date1">
             <el-select v-model="partitionMeta.table" @change="partitionTableChange" placeholder="表" style="width:248px">
              <el-option :label="t.alias" :value="t.alias" v-for="t in partitionTables" :key="t.alias">{{t.alias}}</el-option>
            </el-select>
          </el-form-item>
        </el-col>
        <el-col :span="12">
          <el-select v-model="partitionMeta.column" placeholder="列" filterable style="width:248px">
          <el-option :label="t.name" :value="t.name" v-for="t in columns" :key="t.name">
            <span style="float: left">{{ t.name }}</span>
            <span class="ky-option-sub-info">{{ t.datatype }}</span>
          </el-option>
        </el-select>
        </el-col>
      </el-form-item>
      <el-form-item :label="$t('dateFormat')">
        <el-select size="medium" v-model="partitionMeta.format" style="width:248px" :placeholder="$t('kylinLang.common.pleaseSelect')">
          <el-option
            v-for="item in formatList"
            :key="item.value"
            :label="item.label"
            :value="item.value">
          </el-option>
        </el-select>
      </el-form-item>
    </el-form>
    <div class="ky-line"></div>
    <div class="ky-list-title ksd-mt-16">Where 条件设置</div>
    <el-input type="textarea" class="where-area" v-model="filterCondition"></el-input>
    <div class="ksd-mt-10">Please input : “column_name = value”, i.e. Region = Beijing</div>
    <div slot="footer" class="dialog-footer">
      <!-- <span class="ksd-fleft up-performance"><i class="el-icon-ksd-arrow_up"></i>提升<i>5%</i></span> -->
      <!-- <span class="ksd-fleft down-performance"><i class="el-icon-ksd-arrow_down"></i>下降<span>5%</span></span> -->
      <el-button plain  size="medium" @click="isShow && handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" plain @click="savePartition()" size="medium">{{$t('kylinLang.common.ok')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../../store'
import locales from './locales'
import store, { types } from './store'
import { DatePartitionRule, timeDataType } from '../../../../../config'
import NModel from '../../ModelEdit/model.js'
// import { titleMaps, cancelMaps, confirmMaps, getSubmitData } from './handler'
// import { handleSuccessAsync, handleError } from '../../../util'

vuex.registerModule(['modals', 'ModelPartitionModal'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('ModelPartitionModal', {
      isShow: state => state.isShow,
      modelDesc: state => state.form.modelDesc,
      modelInstance: state => state.form.modelDesc && new NModel(state.form.modelDesc) || null,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('ModelPartitionModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      loadHiveInProject: 'LOAD_HIVE_IN_PROJECT',
      saveKafka: 'SAVE_KAFKA',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      saveSampleData: 'SAVE_SAMPLE_DATA',
      setModelPartition: 'MODEL_PARTITION_SET'
    })
  },
  locales
})
export default class ModelPartitionModal extends Vue {
  isLoading = false
  isFormShow = false
  factTableColumns = [{tableName: 'DEFAULT.KYLIN_SALES', column: 'PRICE'}]
  lookupTableColumns = [{tableName: 'DEFAULT.KYLIN_CAL_DT', column: 'CAL_DT'}]
  partitionMeta = {
    table: '',
    column: '',
    format: ''
  }
  filterCondition = ''
  dateFormat = [
    {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
    {label: 'yyyyMMdd', value: 'yyyyMMdd'},
    {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
    {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'}
  ]
  integerFormat = [
    {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
    {label: 'yyyyMMdd', value: 'yyyyMMdd'},
    {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
    {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'},
    {label: '', value: ''}
  ]
  get partitionTables () {
    let result = ['']
    if (this.isShow && this.modelInstance) {
      Object.values(this.modelInstance.tables).forEach((nTable) => {
        if (nTable.kind === 'FACT') {
          result.push(nTable)
        }
      })
    }
    return result
  }
  get selectedTable () {
    if (this.partitionMeta.table) {
      for (let i = 0; i < this.partitionTables.length; i++) {
        if (this.partitionTables[i].alias === this.partitionMeta.table) {
          return this.partitionTables[i]
        }
      }
    }
  }
  get columns () {
    if (!this.isShow || this.partitionMeta.table === '') {
      return []
    }
    let result = []
    this.modelDesc.all_named_columns.forEach((x) => {
      let tableAndColumn = x.column.split('.')
      let alias = tableAndColumn[0]
      if (alias === this.partitionMeta.table) {
        let colInfo = this.getColumnInfo(tableAndColumn[1])
        if (colInfo && DatePartitionRule.includes(colInfo.datatype)) {
          x.datatype = colInfo.datatype
          result.push(x)
        }
      }
    })
    return result
  }
  get formatList () {
    if (!this.partitionMeta.column) {
      return []
    }
    let partitionColumn = this.getColumnInfo(this.partitionMeta.column)
    if (!partitionColumn) {
      return []
    } else {
      if (timeDataType.indexOf(partitionColumn.datatype) === -1) {
        return this.integerFormat
      } else {
        return this.dateFormat
      }
    }
  }
  getColumnInfo (column) {
    if (this.selectedTable) {
      let len = this.selectedTable.columns && this.selectedTable.columns.length || 0
      for (let i = 0; i < len; i++) {
        const col = this.selectedTable.columns[i]
        if (col.name === column) {
          return col
        }
      }
    }
  }
  @Watch('isShow')
  initModeDesc () {
    if (this.isShow && this.modelDesc && this.modelDesc.partition_desc.partition_date_column) {
      let named = this.modelDesc.partition_desc.partition_date_column.split('.')
      this.partitionMeta.table = named[0]
      this.partitionMeta.column = named[1]
      this.partitionMeta.format = this.modelDesc.partition_desc.partition_date_format
      this.partitionMeta.filter_condition = this.modelDesc.filter_condition
    } else {
      this.resetForm()
    }
  }
  partitionTableChange () {
    this.partitionMeta.column = ''
    this.partitionMeta.format = ''
  }
  resetForm () {
    this.partitionMeta = {
      table: '',
      column: '',
      format: ''
    }
    this.filterCondition = ''
  }
  savePartition () {
    if (this.modelDesc) {
      this.modelDesc.partition_desc.partition_date_column = this.partitionMeta.table + '.' + this.partitionMeta.column
      this.modelDesc.partition_desc.partition_date_format = this.partitionMeta.format
      this.modelDesc.filter_condition = this.filterCondition
      this.modelDesc.project = this.currentSelectedProject
      this.handleClose(true)
    }
  }
  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: this.partitionMeta
      })
    }, 300)
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.model-partition-dialog {
  .where-area {
    margin-top:20px;
  }
  .up-performance{
    i {
      color:@normal-color-1;
      margin-right: 7px;
    }
    span {
      color:@normal-color-1;
      margin-left: 7px;
    }
  }
  .down-performance{
    i {
      color:@error-color-1;
      margin-right: 7px;
    }
    span {
      color:@error-color-1;
      margin-left: 7px;
    }
  }
}

</style>
