<template>
   <div class="tableIndex-box ksd-mt-20">
     <div class="left-part">
      <div class="ksd-mb-20">
        <el-button type="primary" icon="el-icon-ksd-project_add" @click="editTableIndex(true)">Create Table Index</el-button>
        <el-button type="primary" disabled icon="el-icon-ksd-table_refresh">Refresh</el-button>
        <!-- <el-button icon="el-icon-ksd-table_delete">Delete</el-button> -->
        <el-input style="width:200px" v-model="tableIndexFilter" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" :placeholder="$t('searchTip')" class="ksd-fright ksd-mr-20"></el-input>
      </div>
      <kap-nodata v-if="tableIndexBaseList.length === 0" class="ksd-mt-40"></kap-nodata>
      <el-steps direction="vertical">
        <el-step :title="$t(key) + '(' + tableIndex.length + ')'" status="finish" v-if="tableIndex.length" v-for="(tableIndex, key) in tableIndexGroup" :key="key">
          <div slot="icon"><i class="el-icon-ksd-elapsed_time"></i></div>
          <div slot="description">
            <el-carousel :interval="4000" type="card" height="173px" :autoplay="false" :initial-index="tableIndex.length - 1">
              <el-carousel-item v-for="item in tableIndex" :key="item.name" @click.native="showTableIndexDetal(item)">
                <div :class="{'empty-table-index': item.status === 'Empty'}">
                  <div class="slider-content-above ">
                    <div class="main-title" :title="item.name">{{$t('tableIndexName')}} {{item.name|omit(10, '...')}}</div>
                    <div class="status-list">
                      <div v-if="item.status === 'Broken'" class="broken-icon">Broken</div>
                      <div v-if="item.status === 'Empty'" class="empty-icon">[Empty]</div>
                    </div>
                    <div class="sub-info">
                      <div>{{$t('tableIndexId')}}{{item.id}}</div>
                      <i class="el-icon-ksd-elapsed_time ksd-mr-4"></i>{{transToGmtTime(item.update_time)}}
                       <div class="actions ksd-fright">
                        <i class="el-icon-ksd-table_delete del-icon" @click="delTableIndex(item.id)"></i>
                      </div>
                    </div>
                  </div>
                  <div class="ky-line"></div>
                  <div class="slider-content-below">
                    <span class="tableindex-user">{{item.owner}}</span>
                    <span class="tableindex-count"><span>{{item.col_order.length}}</span> Columns</span>
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
          <span>{{$t('tableIndexDetail')}}</span>
          <el-button size="mini" v-if="currentShowTableIndex" @click="editTableIndex(false)" class="ksd-fright ksd-mt-8" icon="el-icon-ksd-table_edit">{{$t('kylinLang.common.edit')}}</el-button>
        </div>
        <div class="ksd-prl-20 ksd-ptb-20">
          <el-table
          :data="showTableIndexDetail"
          border class="ksd-mt-14">
          <el-table-column
            :label="$t('ID')"
            header-align="center"
            align="center"
            prop="id"
            width="80">
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
            prop="columnType"
            header-align="center"
            align="center"
            width="110"> 
          </el-table-column>  
          <el-table-column
          :label="$t('Sort')"
          header-align="center"
          prop="sort"
          align="center">
            </el-table-column>
          <el-table-column
          :label="$t('Shared')"
          header-align="center"
          align="center">
            <template slot-scope="scope">
                <i class="el-icon-ksd-good_health ky-success" v-show="scope.row.shared"></i>
            </template>
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
import { handleSuccess, handleError, transToGmtTime, kapConfirm } from 'util/business'
import { isToday, isThisWeek, isLastWeek } from 'util/index'
import TableIndexEdit from '../TableIndexEdit/tableindex_edit'
import NModel from '../ModelEdit/model.js'
@Component({
  props: ['modelDesc'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    modelInstance () {
      return new NModel(this.modelDesc)
    },
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
      return this.tableIndexBaseList && this.tableIndexBaseList.filter((t) => {
        if (!isToday(t.update_time) && isThisWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      }) || []
    },
    // 上周的数据
    lastWeekTableIndex () {
      return this.tableIndexBaseList && this.tableIndexBaseList.filter((t) => {
        if (isLastWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      }) || []
    },
    // 更久远的数据...
    longAgoTableIndex () {
      return this.tableIndexBaseList && this.tableIndexBaseList.filter((t) => {
        if (!isLastWeek(t.update_time) && !isThisWeek(t.update_time) && (t.id === +this.tableIndexFilter || this.tableIndexFilter === '')) {
          return t
        }
      }) || []
    },
    tableIndexGroup () {
      return {
        today: this.todayTableIndex,
        thisWeek: this.thisWeekTableIndex,
        lastWeek: this.lastWeekTableIndex,
        longAgo: this.longAgoTableIndex
      }
    },
    totalTableIndexColumnSize () {
      return this.currentShowTableIndex && this.currentShowTableIndex.col_order && this.currentShowTableIndex.col_order.length || 0
    },
    showTableIndexDetail () {
      if (!this.currentShowTableIndex || !this.currentShowTableIndex.col_order) {
        return []
      }
      let tableIndexList = this.currentShowTableIndex.col_order.slice(15 * (this.currentPage - 1), 15 * (this.currentPage))
      let renderData = tableIndexList.map((item, i) => {
        let newitem = {
          id: 15 * (this.currentPage - 1) + i + 1,
          column: item,
          columnType: this.modelInstance.getColumnType(item),
          sort: this.currentShowTableIndex.sort_by_columns.indexOf(item) + 1 || '',
          shared: this.currentShowTableIndex.shard_by_columns.includes(item)
        }
        return newitem
      })
      return renderData
    }
  },
  methods: {
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    }),
    ...mapActions({
      'loadAllTableIndex': 'GET_TABLE_INDEX',
      'deleteTableIndex': 'DELETE_TABLE_INDEX'
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
  currentShowTableIndex = null
  tableIndexBaseList = []
  mounted () {
    this.getAllTableIndex()
  }
  showTableIndexDetal (item) {
    this.currentShowTableIndex = item
  }
  currentChange (curPage) {
    this.currentPage = curPage
  }
  delTableIndex (id) {
    // 删除警告
    kapConfirm('确认要删除吗？').then(() => {
      this.deleteTableIndex({
        project: this.currentSelectedProject,
        model: this.modelDesc.name,
        tableIndexId: id
      }).then((res) => {
        handleSuccess(res, (data) => {
          if (this.currentShowTableIndex && this.currentShowTableIndex.id === id) {
            this.currentShowTableIndex = null
          }
          // 成功提示
          this.getAllTableIndex()
        })
      }, (res) => {
        handleError(res)
      })
    })
  }
  getAllTableIndex () {
    this.loadAllTableIndex({
      project: this.currentSelectedProject,
      model: this.modelDesc.name
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.tableIndexBaseList.splice(0, this.tableIndexBaseList.length - 1)
        this.tableIndexBaseList = data.table_indexs
      })
    }, (res) => {
      handleError(res)
    })
  }
  addSortbyCol () {}
  changeRawTable () {}
  sortTable () {}
  editTableIndex (isNew) {
    this.showTableIndexEditModal({
      modelInstance: this.modelInstance,
      tableIndexDesc: isNew ? null : this.currentShowTableIndex
    }).then(() => {
      this.getAllTableIndex()
      // 保存成功或者编辑成功的回调函数
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
  float:left;
  width:100%;
  padding-bottom:20px;
  .el-step__head.is-finish .el-step__icon.is-text {
    background:@fff;
    color:@base-color;
    border-width:1px;
  }
  .el-step__description {
    padding-right:20px!important;
  }
  position: relative;
  .left-part {
    min-height:200px;
    float:left;
    width:calc(~"100% - 489px");
    // padding-right:489px;
    // width:100%
   }
  .right-part {
    width:489px;
    float: right;
    bottom:0;
    right:0;
    top:0;
    .el-card {
      height:100%;
      padding-bottom:20px;
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
      font-size:14px;
    }
    .tableindex-count {
      float:right;
      margin-right:20px;
      span{
        font-weight:@font-medium;
      }
    }
  }
  .empty-table-index {
    background-color: @grey-4;
    .slider-content-above {
      .main-title {
        color:@text-disabled-color;
      }
    }
    .slider-content-below {
      .tableindex-user {
        color:@text-secondary-color;
      }
      .tableindex-count {
        color:@text-secondary-color;
      }
    }
  }
  .slider-content-above{
    .broken-icon {
      color:@error-color-1;
      border: 1px solid @error-color-1;
      border-radius: 2px;
      width:56px;
      height:24px;
      line-height:24px;
      text-align:center;
      background:@error-color-3;
    }
    .empty-icon {
      // color:@text-disabled-color;
      font-size:16px;
    }
    .status-list {
      height:34px;
      padding-top:10px;
    }
    height:132px;
    padding:20px;
    .del-icon {
      display: none;
    }
    &:hover {
      .del-icon {
        display: inline-block;
      }
    }
    .sub-info {
      font-size:12px;
      color:@text-disabled-color;
      margin-top:5px;
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
