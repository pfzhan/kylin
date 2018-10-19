<template>
  <el-dialog 
    :title="$t('Saved Model')"
    width="660px"
    v-event-stop
    :visible.sync="isShow" 
    class="model-partition-dialog" 
    @close="isShow && handleClose(false)" 
    :close-on-press-escape="false" 
    :close-on-click-modal="false">     
    <div class="ky-list-title">Fact Table Setting</div>
    <div class="ky-list-sub-title">中心表叫做事实表</div>
    <el-form :model="form"  ref="ruleForm2" label-position="top">
      <el-form-item prop="pass">
        <el-select  style="width:522px;" v-model="form.fact_table" auto-complete="off">
          <el-option value="1">32</el-option>
        </el-select>
      </el-form-item>
    </el-form>
    <div class="ky-line-full"></div>
    <div class="ky-list-title ksd-mt-40">分区设置</div>
    <div class="ky-list-sub-title">一级分区</div>
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
    </el-form>
    <div class="ky-list-sub-title ksd-mt-2">时间分区</div>
    <el-form :inline="true" :model="form" class="demo-form-inline">
      <el-form-item label="表">
        <el-select v-model="form.region" placeholder="表">
          <el-option label="1" value="shanghai"></el-option>
          <el-option label="2" value="beijing"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="列">
        <el-select v-model="form.region" placeholder="列">
          <el-option label="1" value="1"></el-option>
          <el-option label="2" value="2"></el-option>
        </el-select>
      </el-form-item>
    </el-form>
    <div class="ky-line-full"></div>
    <div class="ky-list-title ksd-mt-30">Where 条件设置</div>
    <el-input type="textarea" class="where-area"></el-input>
    <div class="ksd-mt-10">Please input : “column_name = value”, i.e. Region = Beijing</div>
    <div slot="footer" class="dialog-footer">
      <!-- <span class="ksd-fleft up-performance"><i class="el-icon-ksd-arrow_up"></i>提升<i>5%</i></span> -->
      <span class="ksd-fleft down-performance"><i class="el-icon-ksd-arrow_down"></i>下降<span>5%</span></span>
      <el-button plain  size="medium" @click="isShow && handleClose(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" plain @click="saveLinks(currentLinkData.source.guid,currentLinkData.target.guid)" size="medium">{{$t('kylinLang.common.ok')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
// import { sourceTypes } from '../../../../config'
// import { titleMaps, cancelMaps, confirmMaps, getSubmitData } from './handler'
// import { handleSuccessAsync, handleError } from '../../../util'

vuex.registerModule(['modals', 'ModelPartitionModal'], store)

@Component({
  // props: {
  //   modelTables: {
  //     type: Array,
  //     default: null
  //   }
  // },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('ModelPartitionModal', {
      isShow: state => state.isShow,
      form: state => state.form
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
      saveSampleData: 'SAVE_SAMPLE_DATA'
    })
  },
  locales
})
export default class ModelPartitionModal extends Vue {
  isLoading = false
  isFormShow = false
  factTableColumns = [{tableName: 'DEFAULT.KYLIN_SALES', column: 'PRICE'}]
  lookupTableColumns = [{tableName: 'DEFAULT.KYLIN_CAL_DT', column: 'CAL_DT'}]

  getTableColumns () {
    this.modelTables.forEach((NTable) => {
      if (NTable.kind === 'FACT') {
        NTable.columns.forEach((col) => {
          this.factTableColumns.push({tableName: NTable.name, column: col.name, name: col.name, isSelected: false})
        })
      } else {
        NTable.columns.forEach((col) => {
          this.lookupTableColumns.push({tableName: NTable.name, column: col.name, name: col.name, isSelected: false})
        })
      }
    })
  }
  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 300)
  }
  handleClick () {
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
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
