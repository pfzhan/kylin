<template>
   <div class="tableIndex-box ksd-mt-20">

     <div class="left-part">
      <div class="ksd-mb-20">
        <el-button type="primary" icon="el-icon-ksd-project_add">Create Table Index</el-button>
        <el-button type="primary" icon="el-icon-ksd-table_refresh">Refresh</el-button>
        <el-button icon="el-icon-ksd-table_delete">Delete</el-button>
        <el-input style="width:200px" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" placeholder="search index ID" class="ksd-fright ksd-mr-20"></el-input>
      </div>
      <el-steps direction="vertical" :active="1">
        <el-step title="Recently(7)">
          <div slot="description">
            <el-carousel :interval="4000" type="card" height="149px" :autoplay="false">
              <el-carousel-item v-for="item in 6" :key="item">
                <div class="slider-content-above">
                  <div class="sub-info">Table Index ID: 42323232323</div>
                  <div class="main-title">Table Index Name 001</div>
                  <div class="sub-info tableindex-timer"><i class="el-icon-ksd-elapsed-time"></i>2018-09-28 19:00:00</div>
                  <div class="actions ksd-right"><i class="el-icon-ksd-table_delete"></i></div>
                </div>
                <div class="ky-line"></div>
                <div class="slider-content-below">
                  <span class="tableindex-user">System</span>
                  <span class="tableindex-count"><span>20</span>Columns</span>
                </div>
              </el-carousel-item>
            </el-carousel>
           </div>
        </el-step>
        <el-step title="Last Day(4)">
           <div slot="description">
            <el-carousel :interval="4000" type="card" height="149px" :autoplay="false">
              <el-carousel-item v-for="item in 6" :key="item">
                <div class="slider-content-above">
                  <div class="sub-info">Table Index ID: 42323232323</div>
                  <div class="main-title">Table Index Name 001</div>
                  <div class="sub-info tableindex-timer"><i class="el-icon-ksd-elapsed-time"></i>2018-09-28 19:00:00</div>
                </div>
                <div class="ky-line"></div>
                <div class="slider-content-below">
                  <span class="tableindex-user">System</span>
                  <span class="tableindex-count"><span>20</span>Columns</span>
                </div>
              </el-carousel-item>
            </el-carousel>
           </div>
        </el-step>
        <el-step title="Last Week(4)">
           <div slot="description">
            <el-carousel :interval="4000" type="card" height="149px" :autoplay="false">
              <el-carousel-item v-for="item in 6" :key="item">
                <div class="slider-content-above">
                  <div class="sub-info">Table Index ID: 42323232323</div>
                  <div class="main-title">Table Index Name 001</div>
                  <div class="sub-info tableindex-timer"><i class="el-icon-ksd-elapsed-time"></i>2018-09-28 19:00:00</div>
                </div>
                <div class="ky-line"></div>
                <div class="slider-content-below">
                  <span class="tableindex-user">System</span>
                  <span class="tableindex-count"><span>20</span>Columns</span>
                </div>
              </el-carousel-item>
            </el-carousel>
           </div>
        </el-step>
        <el-step title="Long Ago(4)">
           <div slot="description">
            <el-carousel :interval="4000" type="card" height="149px" :autoplay="false">
              <el-carousel-item v-for="item in 6" :key="item">
                <div class="slider-content-above">
                  <div class="sub-info">Table Index ID: 42323232323</div>
                  <div class="main-title">Table Index Name 001</div>
                  <div class="sub-info tableindex-timer"><i class="el-icon-ksd-elapsed-time"></i>2018-09-28 19:00:00</div>
                </div>
                <div class="ky-line"></div>
                <div class="slider-content-below">
                  <span class="tableindex-user">System</span>
                  <span class="tableindex-count"><span>20</span>Columns</span>
                </div>
              </el-carousel-item>
            </el-carousel>
           </div>
        </el-step>
      </el-steps>
    </div>
    <div class="right-part">
      <el-card class="box-card">
        <div slot="header" class="clearfix">
          <span>Table Index Details</span>
          <el-button size="mini" @click="editTableIndex" class="ksd-fright ksd-mt-8" icon="el-icon-ksd-table_edit">Edit</el-button>
        </div>
        <div class="ksd-prl-20 ksd-ptb-20">
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
          :label="$t('Sort')"
          header-align="center"
          align="center">
            </el-table-column>
            <el-table-column
          :label="$t('Shared')"
          header-align="center"
          align="center">
            </el-table-column>         
          </el-table>
          <pager ref="pager" :perPageSize="15" :totalSize="totalRawTable"  v-on:handleCurrentChange='currentChange'></pager>
        </div> 
      </el-card>
    </div>
     <!-- <div class="left-part">
        <el-form label-position="top" label-width="80px">
        <el-form-item label="Shard by Column">
          <el-select v-model="shardbyColumn" style="width:100%" size="medium" :disabled="lockRawTable">
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
            <el-col  :span="17">
              <el-select v-model="sortbyColumns[index]" style="width:208px" size="medium" @change="sortTable" :disabled="lockRawTable">
                <el-option
                  v-for="(column, colIndex) in sortByOptitions"
                  :key="column.table + '.' + column.column"
                  :label="column.table + '.' + column.column"
                  :value="column.table + '.' + column.column">
                </el-option>
              </el-select>
            </el-col>
            <el-col :span="7">
              <el-button circle palin icon="el-icon-plus" size="small"  @click="addSortbyCol" :disabled="lockRawTable"></el-button>
              <el-button circle size="small" icon="el-icon-minus" @click="delSortbyCol(index)" :disabled="lockRawTable"></el-button>
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
          <el-button type="primary" plain>Save</el-button>
        </div>
     </div> -->
     <TableIndexEdit/>
   </div>
</template>
<script>

import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import locales from './locales'
import TableIndexEdit from '../TableIndexEdit/tableindex_edit'

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    })
  },
  methods: {
    ...mapActions({
    })
  },
  components: {
    TableIndexEdit
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
  searchLoading = false
  mounted () {}
  addSortbyCol () {}
  currentChange () {}
  changeRawTable () {}
  sortTable () {}
  editTableIndex () {
    this.showTableIndexEditModal()
  }
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
  .el-step__description {
    padding-right:20px!important;
  }
  position: relative;
   width:100%;
  .left-part {
    padding-right:489px;
    width:100%
   }
  .right-part {
    width:489px;
    position:absolute;
    bottom:0;
    right:0;
    top:0;
    .el-card {
      height:calc(~'100vh');
      // padding-bottom:20px;
    }
  }
  .el-carousel__item h3 {
    color: #475669;
    font-size: 14px;
    opacity: 0.75;
    line-height: 200px;
    margin: 0;
  }
  .slider-content-below {
    height:40px;
    line-height:40px;
    color:@text-normal-color;
    .tableindex-user {
      float:left;
      margin-left:20px;
    }
    .tableindex-count {
      float:right;
      margin-right:20px;
      span{
        font-weight:@font-medium;
      }
    }
  }
  .slider-content-above{
    height:108px;
    padding:20px;
    .sub-info {
      font-size:12px;
      color:@text-disabled-color;
    }
    .main-title {
      font-size: 16px;
      color:@text-title-color; 
    }
  }
  .el-carousel__item:nth-child(2n) {
    background-color: @fff;
    border:solid 1px @text-placeholder-color
  }
  
  .el-carousel__item:nth-child(2n+1) {
    background-color: @fff;
    border:solid 1px @text-placeholder-color
  }
}
</style>
