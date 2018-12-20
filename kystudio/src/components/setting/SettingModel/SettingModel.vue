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
            <span class="model-setting-item">
              {{$t('segmentMerge')}}<span v-for="item in scope.row.segmentMerge" :key="item">{{item}}</span>
            </span>
            <i class="el-icon-ksd-symbol_type"></i>
          </div>
          <div v-if="scope.row.retention">
            <span class="model-setting-item">
              {{$t('retention')}}<span>{{scope.row.retention}}</span>
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
            <i class="el-icon-ksd-table_add"></i>
          </template>
      </el-table-column>
    </el-table>
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
    {name: 'nmodel_basic_inner', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['One day', 'One week'], retention: '200 days'},
    {name: 'nmodel_full_measure_test', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['One day', 'One week'], retention: '200 days'},
    {name: 'test_encoding', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['One day', 'One week'], retention: '200 days'},
    {name: 'ut_inner_join_cube_partial', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['One day', 'One week'], retention: '200 days'},
    {name: 'nmodel_basic', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['One day', 'One week'], retention: '200 days'},
    {name: 'all_fixed_length', last_modified: 1545188124251, owner: 'ADMIN', segmentMerge: ['One day', 'One week'], retention: '200 days'}
  ]
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
</style>
