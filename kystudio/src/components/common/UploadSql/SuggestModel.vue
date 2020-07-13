<template>
  <div>
    <el-row :gutter="15" v-if="!isOriginModelsTable">
      <el-col :span="15">
        <el-table
          :data="suggestModels"
          class="model-table"
          border
          v-scroll-shadow
          :ref="tableRef"
          style="width: 100%"
          @select="handleSelectionModel"
          @selection-change="handleSelectionModelChange"
          @select-all="handleSelectionAllModel"
          @row-click="modelRowClick"
          :row-class-name="setRowClass"
          :max-height="maxHeight">
          <el-table-column type="selection" width="44"></el-table-column>
          <!-- <el-table-column type="expand" width="44">
            <template slot-scope="scope">
              <el-table :data="sqlsTable(scope.row.sqls)" border size="small" :show-header="false" stripe>
                <el-table-column prop="sql" show-overflow-tooltip></el-table-column>
              </el-table>
            </template>
          </el-table-column> -->
          <el-table-column :label="$t('kylinLang.model.modelNameGrid')" prop="alias">
            <template slot-scope="scope">
              <el-input v-model="scope.row.alias" :class="{'name-error': scope.row.isNameError}" size="small" @change="handleRename(scope.row)"></el-input>
              <div class="rename-error" v-if="scope.row.isNameError">{{modelNameError}}</div>
            </template>
          </el-table-column>
          <el-table-column :label="$t('kylinLang.common.fact')" prop="fact_table" show-overflow-tooltip width="140"></el-table-column>
          <!-- <el-table-column :label="$t('kylinLang.common.dimension')" prop="dimensions" show-overflow-tooltip width="95" align="right">
            <template slot-scope="scope">{{scope.row.dimensions.length}}</template>
          </el-table-column>
          <el-table-column :label="$t('kylinLang.common.measure')" prop="all_measures" width="90" align="right">
            <template slot-scope="scope">{{scope.row.all_measures.length}}</template>
          </el-table-column>
          <el-table-column :label="$t('kylinLang.common.computedColumn')" prop="computed_columns" width="150" align="right">
            <template slot-scope="scope">{{scope.row.computed_columns.length}}</template>
          </el-table-column>
          <el-table-column :label="$t('index')" prop="index_plan.indexes" width="70" align="right">
            <template slot-scope="scope">{{scope.row.index_plan.indexes.length}}</template>
          </el-table-column> -->
          <el-table-column label="SQL" prop="sqls" width="80" align="right" :render-header="renderHeaderSql">
            <template slot-scope="scope">{{scope.row.sqls.length}}</template>
          </el-table-column>
        </el-table>
      </el-col>
      <el-col :span="9">
        <el-table
          :data="modelDetails"
          class="model-table"
          border
          v-scroll-shadow
          style="width: 100%"
          :max-height="maxHeight">
          <el-table-column type="expand" width="44">
            <template slot-scope="scope">
              <template v-if="scope.row.type === 'cc'">
                <p><span class="label">{{$t('th_expression')}}：</span>{{scope.row.expression}}</p>
              </template>
              <template v-if="scope.row.type === 'dimension'">
                <p><span class="label">{{$t('th_column')}}：</span>{{scope.row.column}}</p>
                <!-- <p><span>{{$t('th_dataType')}}：</span>{{JSON.parse(scope.row.content).data_type}}</p> -->
              </template>
              <template v-if="scope.row.type === 'measure'">
                <p><span class="label">{{$t('th_column')}}：</span>{{scope.row.name}}</p>
                <p><span class="label">{{$t('th_function')}}：</span>{{scope.row.function.expression}}</p>
                <p><span class="label">{{$t('th_parameter')}}：</span>{{scope.row.function.parameters}}</p>
              </template>
            </template>
          </el-table-column>
          <el-table-column :label="$t('th_name')" prop="name" show-overflow-tooltip>
          </el-table-column>
          <el-table-column :label="$t('th_type')" prop="type" width="80">
            <template slot-scope="scope">
              {{ $t(scope.row.type) }}
            </template>
          </el-table-column>
        </el-table>
      </el-col>
    </el-row>
    <el-row :gutter="15" v-else>
      <el-col :span="15">
        <el-table
          :data="suggestModels"
          class="model-table"
          border
          v-scroll-shadow
          :ref="tableRef"
          style="width: 100%"
          @selection-change="handleSelectionRecommendationChange"
          @row-click="recommendRowClick"
          :row-class-name="setRowClass"
          :max-height="maxHeight">
          <el-table-column type="selection" width="44"></el-table-column>
          <el-table-column :label="$t('kylinLang.model.modelNameGrid')" show-overflow-tooltip prop="alias">
            <template slot-scope="scope">
              <span>{{scope.row.alias}}</span>
            </template>
          </el-table-column>
          <el-table-column :label="$t('recommendType')">
            <template slot-scope="scope">
              <span>
                <el-tag type="success" v-if="scope.row.recommendation.recommendation_type === 'ADDITION'" size="mini">{{$t(' ')}}</el-tag>
                {{scope.row.recommendation.recommendation_type}}
              </span>
            </template>
          </el-table-column>
          <!-- <el-table-column :label="$t('kylinLang.common.dimension')" prop="recommendation.dimension_recommendations" show-overflow-tooltip width="95" align="right">
            <template slot-scope="scope">{{scope.row.recommendation.dimension_recommendation_size}}</template>
          </el-table-column>
          <el-table-column :label="$t('kylinLang.common.measure')" prop="recommendation.measure_recommendations" width="90" align="right">
            <template slot-scope="scope">{{scope.row.recommendation.measure_recommendation_size}}</template>
          </el-table-column>
          <el-table-column :label="$t('kylinLang.common.computedColumn')" prop="recommendation.cc_recommendations" width="150" align="right">
            <template slot-scope="scope">{{scope.row.recommendation.cc_recommendation_size}}</template>
          </el-table-column>
          <el-table-column :label="$t('index')" prop="recommendation.index_recommendations" width="70" align="right">
            <template slot-scope="scope">{{scope.row.recommendation.index_recommendation_size}}</template>
          </el-table-column> -->
          <el-table-column label="SQL" prop="sqls" width="80" align="right" :render-header="renderHeaderSql">
            <template slot-scope="scope">{{scope.row.sqls.length}}</template>
          </el-table-column>
        </el-table>
      </el-col>
      <el-col :span="9">
        <el-table
          :data="recommendDetails"
          class="model-table"
          border
          v-scroll-shadow
          style="width: 100%"
          :max-height="maxHeight">
          <el-table-column type="expand" width="44">
            <template slot-scope="scope">
              <template v-if="scope.row.type === 'cc'">
                <p><span class="label">{{$t('th_expression')}}：</span>{{scope.row.expression}}</p>
              </template>
              <template v-if="scope.row.type === 'dimension'">
                <p><span class="label">{{$t('th_column')}}：</span>{{scope.row.column.column}}</p>
                <!-- <p><span>{{$t('th_dataType')}}：</span>{{JSON.parse(scope.row.content).data_type}}</p> -->
              </template>
              <template v-if="scope.row.type === 'measure'">
                <p><span class="label">{{$t('th_column')}}：</span><span class="break-word">{{scope.row.measure.name}}</span></p>
                <p><span class="label">{{$t('th_function')}}：</span>{{scope.row.measure.function.expression}}</p>
                <p><span class="label">{{$t('th_parameter')}}：</span>{{scope.row.measure.function.parameters}}</p>
              </template>
            </template>
          </el-table-column>
          <el-table-column :label="$t('th_name')" prop="name" show-overflow-tooltip>
          </el-table-column>
          <el-table-column :label="$t('th_type')" prop="type" width="80">
            <template slot-scope="scope">
              {{ $t(scope.row.type) }}
            </template>
          </el-table-column>
        </el-table>
      </el-col>
    </el-row>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { NamedRegex } from 'config'
import { handleSuccess } from '../../../util/business'
import { handleError } from '../../../util/index'
import { mapActions, mapGetters } from 'vuex'
import locales from './locales'
@Component({
  props: ['suggestModels', 'tableRef', 'isOriginModelsTable', 'maxHeight'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
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
  modelDetails = []
  selectRecommends = []
  recommendDetails = []
  activeRowId = ''
  mounted () {
    this.$nextTick(() => {
      this.suggestModels.forEach((model) => {
        this.$refs[this.tableRef] && this.$refs[this.tableRef].toggleRowSelection(model)
      })
      if (!this.isOriginModelsTable) {
        this.modelRowClick(this.suggestModels[0])
      } else {
        this.recommendRowClick(this.suggestModels[0])
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
  renderHeaderSql (h, { column, index }) {
    return <span class="sql-header">
      SQL
      <el-tooltip content={ this.$t('sqlInfo') } effect="dark" placement="top">
        <span class="icon el-icon-ksd-what ksd-ml-5"></span>
      </el-tooltip>
    </span>
  }
  setRowClass ({row}) {
    return row.uuid === this.activeRowId ? 'active-row' : ''
  }
  modelRowClick (row) {
    this.activeRowId = row.uuid
    this.modelDetails = [...row.all_measures.map(it => ({...it, type: 'measure'})), ...row.dimensions.map(it => ({...it, type: 'dimension'})), ...row.computed_columns.map(it => ({...it, type: 'cc'}))]
  }
  recommendRowClick (scope) {
    const row = scope.recommendation
    this.activeRowId = scope.uuid
    this.recommendDetails = [...row.cc_recommendations.map(it => ({...it, type: 'cc'})), ...row.dimension_recommendations.map(it => ({...it, name: it.column.name, type: 'dimension'})), ...row.measure_recommendations.map(it => ({...it, name: it.measure.name, type: 'measure'}))]
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
  handleSelectionRecommendationChange (val) {
    this.selectRecommends = val
    this.$emit('getSelectRecommends', this.selectRecommends)
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
<style lang="less">
  @import '../../../assets/styles/variables.less';
  .rename-error {
    color: @error-color-1;
    font-size: 12px;
    line-height: 1.2;
  }
  .break-word {
    word-break: break-all;
  }
  .model-table {
    .el-table__row {
      cursor: pointer;
    }
    .active-row {
      background-color: @base-color-9;
    }
    .el-table__expanded-cell {
      padding: 10px;
      font-size: 12px;
      color: @text-title-color;
      p {
        margin-bottom: 5px;
        &:last-child {
          margin-bottom: 0;
        }
      }
      .label {
        color: @text-normal-color;
      }
    }
    .sql-header {
      .icon {
        cursor: pointer;
      }
    }
  }
</style>
