<template>
<div class="cube_desc_view" id="cube-view">
  <el-steps :active="activeStep"  finish-status="finish" process-status="wait" center >
    <el-step :title="$t('cubeInfo')" @click.native="step(1)"></el-step>
    <!-- <el-step :title="$t('Sql')" @click.native="step(2)"></el-step> -->
    <el-step :title="$t('dimensions')" @click.native="step(2)"></el-step>
    <el-step :title="$t('measures')" @click.native="step(3)"></el-step>
    <el-step :title="$t('refreshSetting')" @click.native="step(4)"></el-step>
    <el-step :title="$t('tableIndex')" @click.native="step(5)"></el-step>
    <el-step :title="$t('AdvancedSetting')" @click.native="step(6)"></el-step>
    <el-step :title="$t('overview')" @click.native="step(7)"></el-step>
  </el-steps>
  <info v-if="activeStep===1" :cubeDesc="cube"></info>
  <!-- <sample_sql v-if="activeStep===2" :cubeDesc="cube"></sample_sql> -->
  <dimensions v-if="activeStep===2" :cubeDesc="cube"></dimensions>
  <measures v-if="activeStep===3" :cubeDesc="cube"></measures>
  <refresh_setting v-if="activeStep===4" :cubeDesc="cube"></refresh_setting>
  <table_index v-if="activeStep===5" :cubeDesc="cube" :cubeIndex="index"></table_index>
  <configuration_overwrites v-if="activeStep===6" :cubeDesc="cube"></configuration_overwrites>
  <overview v-if="activeStep===7" :desc="cube"></overview>
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
import { handleSuccess, handleError, hasRole, hasPermission } from '../../../util/business'
import { removeNameSpace } from '../../../util/index'
import { permissions } from 'config/index'
export default {
  name: 'cubedesc',
  props: ['cube', 'index'],
  data () {
    return {
      activeStep: 1,
      selected_project: this.cube.project,
      aliasMap: {}
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
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      loadModelInfo: 'LOAD_MODEL_INFO'
    }),
    step: function (num) {
      this.activeStep = num
    },
    getTables: function () {
      let rootFactTable = removeNameSpace(this.cube.modelDesc.fact_table)
      let factTables = []
      let lookupTables = []
      factTables.push(rootFactTable)
      this.$set(this.cube.modelDesc, 'columnsDetail', {})
      this.$store.state.datasource.dataSource[this.selected_project].forEach((table) => {
        if (this.cube.modelDesc.fact_table === table.database + '.' + table.name) {
          table.columns.forEach((column) => {
            this.$set(this.cube.modelDesc.columnsDetail, rootFactTable + '.' + column.name, {
              name: column.name,
              datatype: column.datatype,
              cardinality: table.cardinality[column.name],
              comment: column.comment})
          })
          this.aliasMap[table.name] = table.database + '.' + table.name
        }
      })
      this.cube.modelDesc.lookups.forEach((lookup) => {
        if (lookup.kind === 'FACT') {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          factTables.push(lookup.alias)
        } else {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          lookupTables.push(lookup.alias)
        }
        this.aliasMap[lookup.alias] = lookup.table
        this.$store.state.datasource.dataSource[this.selected_project].forEach((table) => {
          if (lookup.table === table.database + '.' + table.name) {
            table.columns.forEach((column) => {
              this.$set(this.cube.modelDesc.columnsDetail, lookup.alias + '.' + column.name, {
                name: column.name,
                datatype: column.datatype,
                cardinality: table.cardinality[column.name],
                comment: column.comment})
            })
          }
        })
      })
      if (this.cube.modelDesc.computed_columns) {
        this.cube.modelDesc.computed_columns.forEach((co) => {
          var alias = ''
          for (var i in this.aliasMap) {
            if (this.aliasMap[i] === co.tableIdentity) {
              alias = i
            }
          }
          this.$set(this.cube.modelDesc.columnsDetail, alias + '.' + co.columnName, {
            name: co.columnName,
            datatype: co.datatype,
            cardinality: 'N/A',
            comment: co.expression
          })
        })
      }
    },
    getProjectIdByName (pname) {
      var projectList = this.$store.state.project.allProject
      var len = projectList && projectList.length || 0
      var projectId = ''
      for (var s = 0; s < len; s++) {
        if (projectList[s].name === pname) {
          projectId = projectList[s].uuid
        }
      }
      return projectId
    },
    hasSomePermissionOfProject () {
      var projectId = this.getProjectIdByName(this.$store.state.project.selected_project)
      return hasPermission(this, projectId, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    }
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  created () {
    if (!this.cube.desc) {
      this.loadCubeDesc({cubeName: this.cube.name, project: this.selected_project}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.cube, 'desc', data.cube)
        })
      }).catch((res) => {
        handleError(res, () => {})
      })
    }
    var isExt = false
    if (this.isAdmin || this.hasSomePermissionOfProject()) {
      isExt = true
    }
    if (!this.cube.modelDesc) {
      this.loadDataSourceByProject({project: this.selected_project, isExt: isExt}).then(() => {
        this.loadModelInfo({modelName: this.cube.model, project: this.selected_project}).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.$set(this.cube, 'modelDesc', data.model)
            this.getTables()
          })
        }).catch((res) => {
          handleError(res, () => {})
        })
      }, (res) => {
        handleError(res)
      })
    }
  },
  locales: {
    'en': {cubeInfo: 'Cube Info', Sql: 'Sample Sql', dimensions: 'Dimensions', measures: 'Measures', refreshSetting: 'Refresh Setting', tableIndex: 'Table Index', configurationOverwrites: 'Configuration Overwrites', overview: 'Overview', AdvancedSetting: 'Advanced Setting'},
    'zh-cn': {cubeInfo: 'Cube信息', Sql: '样例查询', dimensions: '维度', measures: '度量', refreshSetting: '更新设置', tableIndex: '表索引', configurationOverwrites: '配置覆盖', overview: '概览', AdvancedSetting: '高级设置'}
  }
}
</script>
<style lang="less">
@import '../../../less/config.less';
.cube_desc_view {
  line-height: 30px;
  .el-col-4 {
    padding-right: 10px;
    text-align: right;
  }
}
#cube-view{
  .el-step__main{
    text-align: center;
    transform: translateX(-50%);
    margin-left: 14px!important;
    padding: 0;
  }
  .el-steps.is-horizontal.is-center{
    margin-left: 20px;
  }
  .el-table tr th:first-child {
    border-right: 1px solid #2c2f3c;
  }
  .el-table tr td:first-child {
    border-right: 1px solid #393e53;
  }
}
</style>
