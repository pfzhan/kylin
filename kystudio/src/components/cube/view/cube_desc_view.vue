<template>
<div class="cube_desc_view" id="cube-view">
  <el-steps class="cube-step" :active="activeStep" space="20%"  finish-status="finish" process-status="wait" center align-center >
    <el-step :title="$t('cubeInfo')" @click.native="step(1)"></el-step>
    <el-step :title="$t('dimensions')" @click.native="step(2)"></el-step>
    <el-step :title="$t('measures')" @click.native="step(3)"></el-step>
    <el-step :title="$t('tableIndex')" @click.native="step(4)"></el-step>
    <el-step :title="$t('overview')" @click.native="step(5)"></el-step>
  </el-steps>
  <info v-if="activeStep===1" :cubeDesc="extraoption"></info>
  <dimensions v-if="activeStep===2" :cubeDesc="extraoption"></dimensions>
  <measures v-if="activeStep===3" :cubeDesc="extraoption"></measures>
  <table_index v-if="activeStep===4" :cubeDesc="extraoption"></table_index>
  <overview v-if="activeStep===5" :desc="extraoption"></overview>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import info from './info_view'
import dimensions from './dimensions_view'
import measures from './measures_view'
import tableIndex from './table_index_view'
import overview from './overview_view'
import { handleSuccess, handleError } from '../../../util/business'
import { removeNameSpace } from '../../../util/index'
export default {
  name: 'cubedesc',
  props: ['extraoption'],
  data () {
    return {
      activeStep: 1,
      selected_project: this.extraoption.project,
      aliasMap: {}
    }
  },
  components: {
    'info': info,
    'dimensions': dimensions,
    'measures': measures,
    'table_index': tableIndex,
    'overview': overview
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC',
      loadModelInfo: 'LOAD_MODEL_INFO',
      loadDataSourceByProject: 'LOAD_DATASOURCE'
    }),
    step: function (num) {
      this.activeStep = num
    },
    getTables: function () {
      let rootFactTable = removeNameSpace(this.extraoption.modelDesc.fact_table)
      let factTables = []
      let lookupTables = []
      factTables.push(rootFactTable)
      this.$set(this.extraoption.modelDesc, 'columnsDetail', {})
      this.$store.state.datasource.dataSource[this.selected_project].forEach((table) => {
        if (this.extraoption.modelDesc.fact_table === table.database + '.' + table.name) {
          table.columns.forEach((column) => {
            this.$set(this.extraoption.modelDesc.columnsDetail, rootFactTable + '.' + column.name, {
              name: column.name,
              datatype: column.datatype,
              cardinality: table.cardinality[column.name],
              comment: column.comment})
          })
          this.aliasMap[table.name] = table.database + '.' + table.name
        }
      })
      this.extraoption.modelDesc.lookups.forEach((lookup) => {
        if (lookup.kind === 'FACT') {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          factTables.push(lookup.alias)
          this.aliasMap[lookup.alias] = lookup.table
        } else {
          if (!lookup.alias) {
            lookup['alias'] = removeNameSpace(lookup.table)
          }
          lookupTables.push(lookup.alias)
          this.aliasMap[lookup.alias] = lookup.table
        }
        this.$store.state.datasource.dataSource[this.selected_project].forEach((table) => {
          if (lookup.table === table.database + '.' + table.name) {
            table.columns.forEach((column) => {
              this.$set(this.extraoption.modelDesc.columnsDetail, lookup.alias + '.' + column.name, {
                name: column.name,
                datatype: column.datatype,
                cardinality: table.cardinality[column.name],
                comment: column.comment})
            })
          }
        })
      })
      if (this.extraoption.modelDesc.computed_columns) {
        this.extraoption.modelDesc.computed_columns.forEach((co) => {
          var alias = ''
          for (var i in this.aliasMap) {
            if (this.aliasMap[i] === co.tableIdentity) {
              alias = i
            }
          }
          this.$set(this.extraoption.modelDesc.columnsDetail, alias + '.' + co.columnName, {
            name: co.columnName,
            datatype: co.datatype,
            cardinality: 'N/A',
            comment: co.expression
          })
        })
      }
    }
  },
  created () {
    this.loadCubeDesc({cubeName: this.extraoption.name, project: this.selected_project, version: {version: this.extraoption.cubeVersion || null}}).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        this.$set(this.extraoption, 'desc', data.cube)
        this.extraoption.desc.name = this.extraoption.name
      })
    }, (res) => {
      handleError(res)
    })
    this.loadDataSourceByProject({project: this.selected_project, isExt: true}).then(() => {
      this.loadModelInfo({modelName: this.extraoption.model, project: this.selected_project}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$set(this.extraoption, 'modelDesc', data.model)
          this.getTables()
        })
      }, (res) => {
        handleError(res)
      })
    }, (res) => {
      handleError(res)
    })
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
  padding: 0px 30px 40px 30px;
  .el-step__main{
    width: 188px;
    transform: translateX(-50%);
    margin-left: 14px!important;
    text-align: center;
    margin-right: 0!important;
    padding: 0;
  }
  .cube-step {
    width:100%;
    margin:0 auto;
    padding-top:30px;
    margin-left: 14px;
  }
  .el-steps.is-horizontal.is-center{
    margin-left: 20px;
  }
}

</style>
