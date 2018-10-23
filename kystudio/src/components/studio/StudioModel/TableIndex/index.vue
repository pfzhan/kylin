<template>
   <div class="tableIndex-box ksd-mt-20">

     <div class="left-part">
      <div class="ksd-mb-20">
        <el-button type="primary" icon="el-icon-ksd-project_add">Create Table Index</el-button>
        <el-button type="primary" icon="el-icon-ksd-table_refresh">Refresh</el-button>
        <!-- <el-button icon="el-icon-ksd-table_delete">Delete</el-button> -->
        <el-input style="width:200px" v-model="tableIndexFilter" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" placeholder="search index ID" class="ksd-fright ksd-mr-20"></el-input>
      </div>
      <el-steps direction="vertical" :active="4">
        <el-step title="Today(7)" v-if="todayTableIndex.length">
          <div slot="description">
            <el-carousel :interval="4000" type="card" height="149px" :autoplay="false">
              <el-carousel-item v-for="item in todayTableIndex" :key="item" @click.native="showTableIndexDetal(item)">
                <div :class="{'empty-table-index': item.status === 'empty'}">
                  <div class="slider-content-above ">
                    <div class="sub-info">Table Index ID: {{item.id}}</div>
                    <div class="main-title" :title="item.name">Table Index Name: {{item.name|omit(10, '...')}}<i class="el-icon-warning ksd-ml-4"></i></div>
                    <div class="sub-info tableindex-timer"><i class="el-icon-ksd-elapsed-time"></i>{{transToGmtTime(item.update_time)}}</div>
                    <div class="actions ksd-right" @click="delTableIndex(item.id)"><i class="el-icon-ksd-table_delete"></i></div>
                  </div>
                  <div class="ky-line"></div>
                  <div class="slider-content-below">
                    <span class="tableindex-user">{{item.owner}}</span>
                    <span class="tableindex-count"><span>{{item.col_order.length}}</span>Columns</span>
                  </div>
                </div>
              </el-carousel-item>
            </el-carousel>
           </div>
        </el-step>
        <el-step title="This Week(4)" v-if="thisWeekTableIndex.length">
           <div slot="description">
            <el-carousel :interval="4000" type="card" height="149px" :autoplay="false">
              <el-carousel-item v-for="item in thisWeekTableIndex" :key="item" @click.native="showTableIndexDetal(item)">
                <div :class="{'empty-table-index': item.status === 'empty'}">
                  <div class="slider-content-above ">
                    <div class="sub-info">Table Index ID: {{item.id}}</div>
                    <div class="main-title" :title="item.name">Table Index Name: {{item.name|omit(10, '...')}}<i class="el-icon-warning ksd-ml-4"></i></div>
                    <div class="sub-info tableindex-timer"><i class="el-icon-ksd-elapsed-time"></i>{{transToGmtTime(item.update_time)}}</div>
                    <div class="actions ksd-right" @click="delTableIndex(item.id)"><i class="el-icon-ksd-table_delete"></i></div>
                  </div>
                  <div class="ky-line"></div>
                  <div class="slider-content-below">
                    <span class="tableindex-user">{{item.owner}}</span>
                    <span class="tableindex-count"><span>{{item.col_order.length}}</span>Columns</span>
                  </div>
                </div>
              </el-carousel-item>
            </el-carousel>
           </div>
        </el-step>
        <el-step title="Last Week(4)" v-if="lastWeekTableIndex.length">
           <div slot="description">
            <el-carousel :interval="4000" type="card" height="149px" :autoplay="false">
              <el-carousel-item v-for="item in lastWeekTableIndex" :key="item" @click.native="showTableIndexDetal(item)">
                <div :class="{'empty-table-index': item.status === 'empty'}">
                  <div class="slider-content-above ">
                    <div class="sub-info">Table Index ID: {{item.id}}</div>
                    <div class="main-title" :title="item.name">Table Index Name: {{item.name|omit(10, '...')}}<i class="el-icon-warning ksd-ml-4"></i></div>
                    <div class="sub-info tableindex-timer"><i class="el-icon-ksd-elapsed-time"></i>{{transToGmtTime(item.update_time)}}</div>
                    <div class="actions ksd-right" @click="delTableIndex(item.id)"><i class="el-icon-ksd-table_delete"></i></div>
                  </div>
                  <div class="ky-line"></div>
                  <div class="slider-content-below">
                    <span class="tableindex-user">{{item.owner}}</span>
                    <span class="tableindex-count"><span>{{item.col_order.length}}</span>Columns</span>
                  </div>
                </div>
              </el-carousel-item>
            </el-carousel>
           </div>
        </el-step>
        <el-step title="Long Ago()" v-if="longAgoTableIndex.length">
           <div slot="description">
            <el-carousel :interval="4000" type="card" height="149px" :autoplay="false">
              <el-carousel-item v-for="item in longAgoTableIndex" :key="item" @click.native="showTableIndexDetal(item)">
                <div :class="{'empty-table-index': item.status === 'empty'}">
                  <div class="slider-content-above ">
                    <div class="sub-info">Table Index ID: {{item.id}}</div>
                    <div class="main-title" :title="item.name">Table Index Name: {{item.name|omit(10, '...')}}<i class="el-icon-warning ksd-ml-4"></i></div>
                    <div class="sub-info tableindex-timer"><i class="el-icon-ksd-elapsed-time"></i>{{transToGmtTime(item.update_time)}}</div>
                    <div class="actions ksd-right" @click="delTableIndex(item.id)"><i class="el-icon-ksd-table_delete"></i></div>
                  </div>
                  <div class="ky-line"></div>
                  <div class="slider-content-below">
                    <span class="tableindex-user">{{item.owner}}</span>
                    <span class="tableindex-count"><span>{{item.col_order.length}}</span>Columns</span>
                  </div>
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
          :data="showTableIndexDetail"
          border class="ksd-mt-14">
          <el-table-column
            :label="$t('ID')"
            header-align="center"
            align="center"
            width="80">
            <template slot-scope="scope">
              
            </template>
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('column')"
            header-align="center"
            prop="column"
            align="center">
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
          <pager ref="pager" :perPageSize="15" :totalSize="totalTableIndexColumnSize"  v-on:handleCurrentChange='currentChange'></pager>
        </div> 
      </el-card>
    </div>
     <TableIndexEdit/>
   </div>
</template>
<script>

import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import locales from './locales'
import { handleSuccess, handleError, transToGmtTime } from 'util/business'
import { isToday, isThisWeek, isLastWeek } from 'util/index'
import TableIndexEdit from '../TableIndexEdit/tableindex_edit'

@Component({
  props: ['modelDesc'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    }),
    ...mapActions({
      'loadAllTableIndex': 'GET_TABLE_INDEX',
      'deleteTableIndex': 'DELETE_TABLE_INDEX'
    }),
    // 当天的数据
    todayTableIndex () {
      return this.tableIndexBaseList.filter((t) => {
        if (isToday(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      })
    },
    // 本周的数据（排除当天）
    thisWeekTableIndex () {
      return this.tableIndexBaseList.filter((t) => {
        if (!isToday(t.update_time) && isThisWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      })
    },
    // 上周的数据
    lastWeekTableIndex () {
      return this.tableIndexBaseList.filter((t) => {
        if (isLastWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      })
    },
    // 更久远的数据...
    longAgoTableIndex () {
      return this.tableIndexBaseList.filter((t) => {
        if (!isLastWeek(t.update_time) && !isThisWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      })
    },
    showTableIndexDetail () {
      if (!this.currentShowTableIndex.col_order) {
        return []
      }
      let tableIndexList = this.currentShowTableIndex.col_order.slice(15 * (this.currentPage - 1), 15 * (this.currentPage))
      let renderData = tableIndexList.map((item) => {
        let newitem = {
          column: item,
          sort: this.currentShowTableIndex.sort_by_columns.includes(item),
          shared: this.currentShowTableIndex.shared_by_columns.includes(item)
        }
        return newitem
      })
      return renderData
    }
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
  totalRawTable = 10
  currentPage = 1
  searchLoading = false
  transToGmtTime = transToGmtTime
  tableIndexFilter = ''
  currentShowTableIndex = {}
  tableIndexBaseList = [
    {
      'id': 123,
      'name': 'today',
      'project': 'default',
      'cubePlanName': 'ncube_basic',
      'status': 'EMPTY',
      'update_time': 1540271625304,
      'col_order': ['TEST_KYLIN_FACT.CAL_DT', 'TEST_KYLIN_FACT.ORDER_ID', 'TEST_KYLIN_FACT.LSTG_FORMAT_NAME'],
      'sort_by_columns': ['TEST_KYLIN_FACT.TRANS_ID'],
      'shared_by_columns': ['TEST_KYLIN_FACT.LEAF_CATEG_ID'],
      'layout_override_indices': {'TEST_KYLIN_FACT.LEAF_CATEG_ID': 'eq'},
      'owner': 'ADMIN',
      'storage_type': 20
    },
    {
      'id': 3000000,
      'name': 'thisweek',
      'project': 'default',
      'cubePlanName': 'ncube_basic',
      'status': 'AVAILABLE',
      'update_time': 1540166400000,
      'col_order': ['TEST_KYLIN_FACT.CAL_DT', 'TEST_KYLIN_FACT.ORDER_ID', 'TEST_KYLIN_FACT.LSTG_FORMAT_NAME'],
      'sort_by_columns': ['TEST_KYLIN_FACT.TRANS_ID'],
      'shared_by_columns': ['TEST_KYLIN_FACT.LEAF_CATEG_ID'],
      'layout_override_indices': {'TEST_KYLIN_FACT.LEAF_CATEG_ID': 'eq'},
      'owner': 'ADMIN',
      'storage_type': 20
    },
    {
      'id': 3000000,
      'name': 'lastweek',
      'project': 'default',
      'cubePlanName': 'ncube_basic',
      'status': 'AVAILABLE',
      'update_time': 1539820800000,
      'col_order': ['TEST_KYLIN_FACT.CAL_DT', 'TEST_KYLIN_FACT.ORDER_ID', 'TEST_KYLIN_FACT.LSTG_FORMAT_NAME'],
      'sort_by_columns': ['TEST_KYLIN_FACT.TRANS_ID'],
      'shared_by_columns': ['TEST_KYLIN_FACT.LEAF_CATEG_ID'],
      'layout_override_indices': {'TEST_KYLIN_FACT.LEAF_CATEG_ID': 'eq'},
      'owner': 'ADMIN',
      'storage_type': 20
    },
    {
      'id': 3000000,
      'name': 'longago',
      'project': 'default',
      'cubePlanName': 'ncube_basic',
      'status': 'AVAILABLE',
      'update_time': 1531843200000,
      'col_order': ['TEST_KYLIN_FACT.CAL_DT', 'TEST_KYLIN_FACT.ORDER_ID', 'TEST_KYLIN_FACT.LSTG_FORMAT_NAME'],
      'sort_by_columns': ['TEST_KYLIN_FACT.TRANS_ID'],
      'shared_by_columns': ['TEST_KYLIN_FACT.LEAF_CATEG_ID'],
      'layout_override_indices': {'TEST_KYLIN_FACT.LEAF_CATEG_ID': 'eq'},
      'owner': 'ADMIN',
      'storage_type': 20
    }
  ]
  mounted () {
    this.loadAllTableIndex()
  }
  showTableIndexDetal (item) {
    this.currentShowTableIndex = item
  }
  currentChange (curPage) {
    this.currentPage = curPage
  }
  delTableIndex (id) {
    // 删除警告
    this.deleteTableIndex({
      project: this.currentSelectedProject,
      modelName: this.modelDesc.name,
      tableIndexId: id
    }).then((res) => {
      handleSuccess(res, (data) => {
        // 成功提示
        this.getAllTableIndex()
      })
    }, (res) => {
      handleError(res)
    })
  }
  getAllTableIndex () {
    this.loadAllTableIndex({
      project: this.currentSelectedProject,
      modelName: this.modelDesc.name
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.tableIndexBaseList = data.table_indexs
      })
    }, (res) => {
      handleError(res)
    })
  }
  addSortbyCol () {}
  changeRawTable () {}
  sortTable () {}
  editTableIndex () {
    this.showTableIndexEditModal().then(() => {
    })
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
  .empty-table-index {
    background-color: @grey-4;
  }
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
      i {
        color:@warning-color-1;
      }
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
