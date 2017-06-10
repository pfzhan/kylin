<template>
<div class="cube_desc_view">
  <el-steps :active="activeStep"  finish-status="finish" process-status="wait" center >
    <el-step :title="$t('cubeInfo')" @click.native="step(1)"></el-step>
    <el-step :title="$t('Sql')" @click.native="step(2)"></el-step>
    <el-step :title="$t('dimensions')" @click.native="step(3)"></el-step>
    <el-step :title="$t('measures')" @click.native="step(4)"></el-step>
    <el-step :title="$t('refreshSetting')" @click.native="step(5)"></el-step>
    <el-step :title="$t('tableIndex')" @click.native="step(6)"></el-step>
    <el-step :title="$t('configurationOverwrites')" @click.native="step(7)"></el-step>
    <el-step :title="$t('overview')" @click.native="step(8)"></el-step>
  </el-steps>
  <info v-if="activeStep===1" :cubeDesc="cube"></info>
  <sample_sql v-if="activeStep===2" :cubeDesc="cube"></sample_sql>
  <dimensions v-if="activeStep===3" :cubeDesc="cube"></dimensions>
  <measures v-if="activeStep===4" :cubeDesc="cube"></measures>
  <refresh_setting v-if="activeStep===5" :cubeDesc="cube"></refresh_setting>
  <table_index v-if="activeStep===6" :cubeDesc="cube" :cubeIndex="index"></table_index>
  <configuration_overwrites v-if="activeStep===7" :cubeDesc="cube"></configuration_overwrites>
  <overview v-if="activeStep===8" :desc="cube"></overview>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import info from './info_view'
import sampleSql from './sample_sql_view'
import dimensions from './dimensions_view'
import measures from './measures_view'
import refreshSetting from './refresh_setting_view'
import tableIndex from './table_index_view'
import configurationOverwrites from './configuration_overwrites_view'
import overview from './overview_view'
import { handleSuccess, handleError } from '../../../util/business'
import { removeNameSpace } from '../../../util/index'
export default {
  name: 'cubedesc',
  props: ['cube', 'index'],
  data () {
    return {
      activeStep: 1,
      selected_project: this.$store.state.project.selected_project
    }
  },
  components: {
    'info': info,
    'sample_sql': sampleSql,
    'dimensions': dimensions,
    'measures': measures,
    'refresh_setting': refreshSetting,
    'table_index': tableIndex,
    'configuration_overwrites': configurationOverwrites,
    'overview': overview
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC',
      loadModelInfo: 'LOAD_MODEL_INFO'
    }),
    step: function (num) {
      this.activeStep = num
    },
    getTables: function () {
      let _this = this
      let rootFactTable = removeNameSpace(_this.cube.modelDesc.fact_table)
      _this.$set(_this.cube.modelDesc, 'columnsDetail', {})
      _this.$store.state.datasource.dataSource[_this.selected_project].forEach(function (table) {
        if (_this.cube.modelDesc.fact_table === table.database + '.' + table.name) {
          table.columns.forEach(function (column) {
            _this.$set(_this.cube.modelDesc.columnsDetail, rootFactTable + '.' + column.name, {
              name: column.name,
              datatype: column.datatype,
              cardinality: table.cardinality[column.name],
              comment: column.comment})
          })
        }
      })
      _this.cube.modelDesc.lookups.forEach(function (lookup) {
        _this.$store.state.datasource.dataSource[_this.selected_project].forEach(function (table) {
          if (lookup.table === table.database + '.' + table.name) {
            table.columns.forEach(function (column) {
              _this.$set(_this.cube.modelDesc.columnsDetail, lookup.alias + '.' + column.name, {
                name: column.name,
                datatype: column.datatype,
                cardinality: table.cardinality[column.name],
                comment: column.comment})
            })
          }
        })
      })
    }
  },
  created () {
    let _this = this
    if (!_this.cube.desc) {
      this.loadCubeDesc(_this.cube.name).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          _this.$set(_this.cube, 'desc', data.cube)
        })
      }).catch((res) => {
        handleError(res, () => {})
      })
    }
    if (!_this.cube.modelDesc) {
      _this.loadModelInfo(_this.cube.model).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          _this.$set(_this.cube, 'modelDesc', data.model)
          _this.getTables()
        })
      }).catch((res) => {
        handleError(res, () => {})
      })
    }
  },
  locales: {
    'en': {cubeInfo: 'Cube Info', Sql: 'Sample Sql', dimensions: 'Dimensions', measures: 'Measures', refreshSetting: 'Refresh Setting', tableIndex: 'Table Index', configurationOverwrites: 'Configuration Overwrites', overview: 'Overview'},
    'zh-cn': {cubeInfo: 'Cube信息', Sql: '样例查询', dimensions: '维度', measures: '度量', refreshSetting: '更新配置', tableIndex: '表索引', configurationOverwrites: '配置覆盖', overview: '概览'}
  }
}
</script>
<style lang="less">
.cube_desc_view {
  line-height: 30px;
  .el-col-4 {
    padding-right: 10px;
    text-align: right;
  }
}
</style>
