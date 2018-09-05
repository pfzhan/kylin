<template>
   <div class="tableIndex-box">
     <div class="left-part">
        <el-form label-position="top" label-width="80px">
        <el-form-item label="Shard by Column">
          <el-select v-model="shardbyColumn" size="medium" :disabled="lockRawTable">
            <el-option
              v-for="item in tableIndexDesc"
              :key="item.table + '.' + item.column"
              :label="item.table + '.' + item.column"
              :value="item.table + '.' + item.column">     
            </el-option>
            <el-option label="" value=""></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Sort by Column">
          <el-row v-for="(item, index) in sortbyColumns" :key="index" :gutter="10">
            <el-col  :span="20">
              <el-select v-model="sortbyColumns[index]" style="width:100%" size="medium" @change="sortTable" :disabled="lockRawTable">
                <el-option
                  v-for="(column, colIndex) in sortByOptitions"
                  :key="column.table + '.' + column.column"
                  :label="column.table + '.' + column.column"
                  :value="column.table + '.' + column.column">
                </el-option>
              </el-select>
            </el-col>
            <el-col :span="4">
              <el-button circle palin icon="el-icon-plus" size="medium" v-if="index === 0" @click="addSortbyCol" :disabled="lockRawTable"></el-button>
              <el-button circle palin icon="el-icon-delete" size="medium" v-else @click="delSortbyCol(index)" :disabled="lockRawTable"></el-button>
            </el-col>
          </el-row>
        </el-form-item>
      </el-form>
     </div>
     <div class="right-part">
       <el-table
        :data="convertedRawTable"
        border class="ksd-mt-14">
        <el-table-column
          :label="$t('ID')"
          header-align="center"
          align="center"
          width="80">
          <template slot-scope="scope">
            <span>{{scope.$index + 1 + 15*(currentPage-1)}}</span>
          </template>
        </el-table-column>
        <el-table-column
          show-overflow-tooltip
          :label="$t('column')"
          header-align="center"
          prop="column"
          align="center">
          <template slot-scope="scope">
            {{scope.row.table + '.' + scope.row.column}}
          </template>
        </el-table-column>
        <el-table-column
          show-overflow-tooltip
          :label="$t('dataType')"
          prop="dataType"
          header-align="center"
          align="center"
          width="110"> 
        </el-table-column>
        <el-table-column
          show-overflow-tooltip
          :label="$t('Encoding')"
          header-align="center"
          align="center"
          width="150">   
          <template slot-scope="scope">
            <el-select size="small" v-model="scope.row.encoding" :disabled="lockRawTable" >
              <el-option
                  v-for="(item, index) in scope.row.selectEncoding" :key="index"
                  :label="item.name"
                  :value="item.name + ':' + item.version">
                <el-tooltip effect="dark" :content="$t('kylinLang.cube.'+$store.state.config.encodingTip[item.name])" placement="top">
                  <span style="float: left;;width: 90%">{{ item.name }}</span>
                  <span style="float: right;width: 10%; color: #8492a6; font-size: 13px" v-if="item.version>1">{{ item.version }}</span>
                </el-tooltip>
              </el-option>              
            </el-select>
          </template>
        </el-table-column>  
        <el-table-column
          show-overflow-tooltip
          :label="$t('Length')"
          header-align="center"
          align="center"
          width="70">   
          <template slot-scope="scope">
            <el-input size="small" v-model="scope.row.valueLength"></el-input>  
          </template>  
        </el-table-column>
        <el-table-column
        :label="$t('Index')"
        header-align="center"
        align="center"
        width="120">
        <template slot-scope="scope">
          <el-select size="small" v-model="scope.row.index" :disabled="lockRawTable" @change="changeRawTable(scope.row, scope.$index)">
            <el-option
                v-for="(item, index) in rawTableIndexOptions" :key="index"
                :label="item"
                :value="item">
            </el-option>             
          </el-select>
            </template>
          </el-table-column>       
        </el-table> 
        <pager ref="pager" :perPageSize="15" :totalSize="totalRawTable"  v-on:handleCurrentChange='currentChange'></pager>
        <div class="ksd-right ksd-mt-10">
          <el-button plain>Cancel</el-button>
          <el-button plain>Save</el-button>
        </div>
     </div>
   </div>
</template>
<script>

import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import locales from './locales'

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
    })
  },
  locales
})
export default class TableIndex extends Vue {
  sortbyColumns = [{table: 'default', column: 'kylin'}]
  tableIndexDesc = [{table: 'default', column: 'kylin'}]
  sortByOptitions = [{table: 'default', column: 'kylin'}]
  convertedRawTable = [{table: 'default', column: 'kylin'}]
  rawTableIndexOptions = []
  shardbyColumn = ''
  lockRawTable = false
  totalRawTable = 10
  currentPage = 1
  mounted () {}
  addSortbyCol () {}
  currentChange () {}
  changeRawTable () {}
  sortTable () {}
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.full-cell {
  .tableIndex-box {
    height:calc(~"100vh");
  }
}
.tableIndex-box {
  display: flex;
  flex-flow:  row nowrap;
  justify-content: space-between;
  width:100%;
  .left-part {
    width: 360px;
    border-right:solid 1px #ccc;
    padding-top:26px;
  }
  .right-part {
    flex-grow:1;
    padding-left: 20px;
    padding-top: 26px;
    padding-bottom: 40px;
  }
}
</style>
