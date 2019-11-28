<template>
  <div>
    <el-alert :title="modelTips" style="padding-top:0px" type="info" :closable="false" :show-background="false" show-icon></el-alert>
    <el-table
      :data="suggestModels"
      class="model-table"
      border
      :ref="tableRef"
      style="width: 100%"
      @select="handleSelectionModel"
      @selection-change="handleSelectionModelChange"
      @select-all="handleSelectionAllModel"
      max-height="430">
      <el-table-column type="selection" width="44"></el-table-column>
      <el-table-column type="expand" width="44">
        <template slot-scope="scope">
          <el-table :data="sqlsTable(scope.row.sqls)" border :show-header="false" stripe>
            <el-table-column prop="sql" show-overflow-tooltip></el-table-column>
          </el-table>
        </template>
      </el-table-column>
      <template v-if="!isOriginModelsTable">
        <el-table-column :label="$t('kylinLang.model.modelNameGrid')" prop="alias">
          <template slot-scope="scope">
            <el-input v-model="scope.row.alias" :class="{'name-error': scope.row.isNameError}" size="small" @change="handleRename(scope.row)"></el-input>
            <div class="rename-error" v-if="scope.row.isNameError">{{modelNameError}}</div>
          </template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.fact')" prop="fact_table" show-overflow-tooltip width="140"></el-table-column>
        <el-table-column :label="$t('kylinLang.common.dimension')" prop="dimensions" show-overflow-tooltip width="95" align="right">
          <template slot-scope="scope">{{scope.row.dimensions.length}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.measure')" prop="all_measures" width="90" align="right">
          <template slot-scope="scope">{{scope.row.all_measures.length}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.computedColumn')" prop="computed_columns" width="150" align="right">
          <template slot-scope="scope">{{scope.row.computed_columns.length}}</template>
        </el-table-column>
        <el-table-column :label="$t('index')" prop="index_plan.indexes" width="150" align="right">
          <template slot-scope="scope">{{scope.row.index_plan.indexes.length}}</template>
        </el-table-column>
        <el-table-column label="SQL" prop="sqls" width="60" align="right">
          <template slot-scope="scope">{{scope.row.sqls.length}}</template>
        </el-table-column>
      </template>
      <template v-else>
        <el-table-column :label="$t('kylinLang.model.modelNameGrid')" prop="alias">
          <template slot-scope="scope">
            <span>{{scope.row.alias}}</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.dimension')" prop="recommendation.dimension_recommendations" show-overflow-tooltip width="95" align="right">
          <template slot-scope="scope">{{scope.row.recommendation.dimension_recommendations.length}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.measure')" prop="recommendation.measure_recommendations" width="90" align="right">
          <template slot-scope="scope">{{scope.row.recommendation.measure_recommendations.length}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.computedColumn')" prop="recommendation.cc_recommendations" width="150" align="right">
          <template slot-scope="scope">{{scope.row.recommendation.cc_recommendations.length}}</template>
        </el-table-column>
        <el-table-column :label="$t('index')" prop="recommendation.index_recommendations" width="150" align="right">
          <template slot-scope="scope">{{scope.row.recommendation.index_recommendations.length}}</template>
        </el-table-column>
        <el-table-column label="SQL" prop="sqls" width="60" align="right">
          <template slot-scope="scope">{{scope.row.sqls.length}}</template>
        </el-table-column>
      </template>
    </el-table>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { NamedRegex } from 'config'
import { handleSuccess } from '../../../util/business'
import { handleError } from '../../../util/index'
import { mapActions } from 'vuex'
import locales from './locales'
@Component({
  props: ['suggestModels', 'tableRef', 'isOriginModelsTable'],
  methods: {
    ...mapActions({
      validateModelName: 'VALIDATE_MODEL_NAME'
    })
  },
  locales
})
export default class SuggestModel extends Vue {
  modelNameError = ''
  isNameErrorModelExisted = ''
  selectModels = []
  mounted () {
    this.$nextTick(() => {
      if (this.suggestModels.length) {
        this.suggestModels.forEach((model) => {
          this.$refs[this.tableRef].toggleRowSelection(model)
        })
      }
    })
  }
  get modelTips () {
    if (this.isOriginModelsTable) {
      return this.$t('originModelTips')
    } else {
      return this.$t('newModelTips')
    }
  }
  sqlsTable (sqls) {
    return sqls.map((s) => {
      return {sql: s}
    })
  }
  handleRename (model) {
    let suggestListRename = false
    model.isNameError = false
    this.modelNameError = ''
    if (model.isChecked) {
      if (!NamedRegex.test(model.alias.trim())) {
        model.isNameError = true
        suggestListRename = true
        this.modelNameError = this.$t('kylinLang.common.nameFormatValidTip')
        this.checkRenameModelExisted()
      }
      if (!suggestListRename) {
        for (let m = 0; m < this.suggestModels.length; m++) {
          if (this.suggestModels[m].uuid !== model.uuid && this.suggestModels[m].alias === model.alias.trim()) {
            model.isNameError = true
            suggestListRename = true
            this.modelNameError = this.$t('renameError')
            this.checkRenameModelExisted()
            break
          }
        }
      }
      if (!suggestListRename) {
        this.validateModelName({alias: model.alias.trim(), uuid: model.uuid, project: this.currentSelectedProject}).then((res) => {
          handleSuccess(res, (data) => {
            if (data) {
              model.isNameError = true
              this.modelNameError = this.$t('renameError')
            }
            this.checkRenameModelExisted()
          })
        }, (res) => {
          handleError(res)
        })
      }
    }
  }
  handleSelectionModel (selection, row) {
    row.isChecked = !row.isChecked
    if (!this.isOriginModelsTable) {
      this.handleRename(row)
    }
  }
  handleSelectionModelChange (selection) {
    this.selectModels = selection
    this.$emit('getSelectModels', this.selectModels)
  }
  handleSelectionAllModel (selection) {
    if (selection.length) {
      selection.forEach((m) => {
        m.isChecked = true
        if (!this.isOriginModelsTable) {
          this.handleRename(m)
        }
      })
    } else {
      this.suggestModels.forEach((m) => {
        m.isChecked = false
        if (!this.isOriginModelsTable) {
          this.handleRename(m)
        }
      })
    }
  }
  checkRenameModelExisted () {
    this.isNameErrorModelExisted = false
    for (let i = 0; i < this.suggestModels.length; i++) {
      if (this.suggestModels[i].isChecked && this.suggestModels[i].isNameError) {
        this.isNameErrorModelExisted = true
        break
      }
    }
    this.$emit('isValidated', this.isNameErrorModelExisted)
  }
}
</script>
