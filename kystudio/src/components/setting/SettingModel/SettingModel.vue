<template>
  <div class="setting-model">
    <el-table
      :data="modelList"
      border
      style="width: 100%">
      <el-table-column width="230px" show-overflow-tooltip prop="name" :label="$t('kylinLang.model.modelNameGrid')"></el-table-column>
      <el-table-column prop="last_modified" show-overflow-tooltip width="250px" :label="$t('modifyTime')">
        <template slot-scope="scope">
          {{transToGmtTime(scope.row.last_modified)}}
        </template>
      </el-table-column>
      <el-table-column prop="owner" show-overflow-tooltip width="100" :label="$t('modifiedUser')"></el-table-column>
      <el-table-column min-width="400px" :label="$t('modelSetting')">
        <template slot-scope="scope">
          <div v-if="scope.row.segmentMerge&&scope.row.segmentMerge.length">
            <span class="model-setting-item" @click="editMergeItem(scope.row)">
              {{$t('segmentMerge')}}<span v-for="item in scope.row.segmentMerge" :key="item">{{$t(item)}}</span>
            </span>
            <i class="el-icon-ksd-symbol_type"></i>
          </div>
          <div v-if="scope.row.volatileRange">
            <span class="model-setting-item" @click="editVolatileItem(scope.row)">
              {{$t('volatileRange')}}<span>{{scope.row.volatileRange.value}} {{scope.row.volatileRange.unit}}</span>
            </span>
            <i class="el-icon-ksd-symbol_type"></i>
          </div>
          <div v-if="scope.row.retention">
            <span class="model-setting-item" @click="editRetentionItem(scope.row)">
              {{$t('retention')}}<span>{{scope.row.retention.value}} {{scope.row.retention.unit}}</span>
            </span>
            <i class="el-icon-ksd-symbol_type"></i>
          </div>
        </template>
      </el-table-column>
      <el-table-column
        width="100px"
        align="center"
        :label="$t('kylinLang.common.action')">
          <template slot-scope="scope">
            <i class="el-icon-ksd-table_add" @click="addSettingItem(scope.row)"></i>
          </template>
      </el-table-column>
    </el-table>
    <el-dialog :title="modelSettingTitle" :visible.sync="editModelSetting" width="440px" class="model-setting-dialog">
      <el-form label-position="top" size="medium" label-width="80px" :model="modelSettingForm">
        <el-form-item :label="$t('modelName')">
          <el-input v-model.trim="modelSettingForm.name" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('settingItem')" v-if="step=='stepOne'">
          <el-select v-model="modelSettingForm.settingItem" size="medium" :placeholder="$t('kylinLang.common.pleaseSelect')" style="width:100%">
            <el-option
              v-for="item in settingOption"
              :key="item"
              :label="item"
              :value="item">
            </el-option>
          </el-select>
          <p v-if="modelSettingForm.settingItem==='Auto-merge'">Segment auto-merge can help defragment your data file by merging the small segments into medium and large segment automatically.</p>
          <p v-if="modelSettingForm.settingItem==='Volatile Range'">‘Auto-Merge’ will not merge latest [Volatile Range] days cube segments, by default is 0.</p>
          <p v-if="modelSettingForm.settingItem==='Retention Threshold'">Only keep the segment whose data is in past given days in cube, the old segment will be automatically dropped from head. </p>
        </el-form-item>
        <el-form-item :label="$t('autoMerge')" class="ksd-mb-10" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Auto-merge'">
          <el-checkbox-group v-model="modelSettingForm.autoMerge" class="merge-groups">
            <div><el-checkbox v-for="(item, index) in mergeGroups" :label="item" :key="item" v-if="index<3">{{$t(item)}}</el-checkbox></div>
            <div><el-checkbox v-for="(item, index) in mergeGroups" :label="item" :key="item" v-if="index>2">{{$t(item)}}</el-checkbox></div>
          </el-checkbox-group>
        </el-form-item>
        <el-form-item :label="$t('volatileRangeItem')" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Volatile Range'">
          <el-input v-model.trim="modelSettingForm.volatileRange.value" class="retention-input"></el-input>
          <el-select v-model="modelSettingForm.volatileRange.unit" class="ksd-ml-8" size="medium" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option
              v-for="item in units"
              :key="item"
              :label="$t(item)"
              :value="item">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('retentionThreshold')" v-if="step=='stepTwo'&&modelSettingForm.settingItem==='Retention Threshold'">
          <el-input v-model.trim="modelSettingForm.retentionThreshold.value" class="retention-input"></el-input>
          <el-select v-model="modelSettingForm.retentionThreshold.unit" class="ksd-ml-8" size="medium" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option
              v-for="item in units"
              :key="item"
              :label="$t(item)"
              :value="item">
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="editModelSetting = false" size="medium" v-if="step=='stepOne'">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button @click="preStep" icon="el-icon-ksd-more_01-copy" size="medium" v-if="step=='stepTwo'">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button type="primary" plain @click="nextStep" size="medium" v-if="step=='stepOne'" :disabled="modelSettingForm.settingItem==''">{{$t('kylinLang.common.next')}}<i class="el-icon-ksd-more_02 el-icon--right"></i></el-button>
        <el-button
          type="primary"
          plain
          @click="submit"
          size="medium"
          v-if="step=='stepTwo'"
          :disabled="isSubmit">
            {{$t('kylinLang.common.submit')}}
          </el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'

import locales from './locales'
import { transToGmtTime } from '../../../util/business'
// import { handleSuccessAsync } from '../../../util/index'

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
    })
  },
  components: {
  },
  locales
})
export default class SettingStorage extends Vue {
  modelList = [
    {name: 'nmodel_basic_inner', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'], volatileRange: {value: 2, unit: 'days'}, retention: {value: 200, unit: 'days'}},
    {name: 'nmodel_full_measure_test', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'], retention: {value: 200, unit: 'days'}},
    {name: 'test_encoding', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'], retention: {value: 200, unit: 'days'}},
    {name: 'ut_inner_join_cube_partial', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'], retention: {value: 200, unit: 'days'}},
    {name: 'nmodel_basic', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'], retention: {value: 200, unit: 'days'}},
    {name: 'all_fixed_length', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'], retention: {value: 200, unit: 'days'}}
  ]
  editModelSetting = false
  isEdit = false
  step = 'stepOne'
  settingOption = ['Auto-merge', 'Volatile Range', 'Retention Threshold']
  mergeGroups = ['HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR']
  units = ['day', 'week', 'year']
  modelSettingForm = {name: '', settingItem: '', autoMerge: [], volatileRange: {value: 0, unit: ''}, retentionThreshold: {value: 0, unit: ''}}

  addSettingItem (row) {
    this.modelSettingForm.name = row.name
    this.editModelSetting = true
  }
  get modelSettingTitle () {
    return this.isEdit ? this.$t('editSetting') : this.$t('newSetting')
  }
  get isSubmit () {
    if (this.modelSettingForm.settingItem === 'Auto-merge' && !this.modelSettingForm.autoMerge.length) {
      return true
    } else if (this.modelSettingForm.settingItem === 'Volatile Range' && (!this.modelSettingForm.volatileRange.value || !this.modelSettingForm.volatileRange.unit)) {
      return true
    } else if (this.modelSettingForm.settingItem === 'Retention Threshold' && (!this.modelSettingForm.retentionThreshold.value || !this.modelSettingForm.retentionThreshold.unit)) {
      return true
    } else {
      return false
    }
  }
  nextStep () {
    this.step = 'stepTwo'
  }
  preStep () {
    this.step = 'stepOne'
  }
  editMergeItem (row) {
    this.modelSettingForm.name = row.name
    this.modelSettingForm.settingItem = 'Auto-merge'
    this.modelSettingForm.autoMerge = JSON.parse(JSON.stringify(row.segmentMerge))
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  editVolatileItem (row) {
    this.modelSettingForm.name = row.name
    this.modelSettingForm.settingItem = 'Volatile Range'
    this.modelSettingForm.volatileRange = JSON.parse(JSON.stringify(row.volatileRange))
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  editRetentionItem (row) {
    this.modelSettingForm.name = row.name
    this.modelSettingForm.settingItem = 'Retention Threshold'
    this.modelSettingForm.retentionThreshold = JSON.parse(JSON.stringify(row.retention))
    this.step = 'stepTwo'
    this.isEdit = true
    this.editModelSetting = true
  }
  submit () {}
  created () {
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.setting-model {
  .model-setting-item {
    background-color: @grey-4;
    line-height: 18px;
    > span {
      margin-left: 5px;
    }
    &:hover {
      color: @base-color;
      cursor: pointer;
    }
  }
}
.model-setting-dialog {
  .el-form-item__content p {
    font-size: 12px;
    line-height: 16px;
    color: @text-normal-color;
    margin-top: 5px;
  }
  .merge-groups {
    > div:nth-child(2) {
      margin-top: -15px;
    }
    .el-checkbox {
      &+.el-checkbox {
        margin-left: 30px;
      }
      &:nth-child(4) {
        margin-left: 0;
      }
    }
  }
  .retention-input {
    display: inline-block;
    width: 100px;
  }
}
</style>
