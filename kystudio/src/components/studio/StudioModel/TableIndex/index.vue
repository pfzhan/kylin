<template>
   <div class="tableIndex-box ksd-mt-20">
     <div class="left-part">
      <div class="ksd-mb-20">
        <el-button type="primary" icon="el-icon-ksd-project_add" v-visible="!isAutoProject" @click="editTableIndex(true)">Create Table Index</el-button>
        <!-- <el-button type="primary" disabled icon="el-icon-ksd-table_refresh">Refresh</el-button> -->
        <!-- <el-button icon="el-icon-ksd-table_delete">Delete</el-button> -->
        <el-input style="width:200px" v-model="tableIndexFilter" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" :placeholder="$t('searchTip')" class="ksd-fright ksd-mr-20"></el-input>
      </div>
      <kap-nodata v-if="tableIndexBaseList.length === 0" class="ksd-mt-40"></kap-nodata>
      <el-steps direction="vertical">
        <el-step :title="$t(key) + '(' + tableIndex.length + ')'" status="finish" v-if="tableIndex.length" v-for="(tableIndex, key) in tableIndexGroup" :key="key">
          <div slot="icon"><i class="el-icon-ksd-elapsed_time"></i></div>
          <div slot="description">
            <el-carousel indicator-position="none" :arrow="tableIndex.length === 1 ? 'never' : 'hover'"  :interval="4000" type="card" height="187px" :autoplay="false" :initial-index="tableIndex.length - 1">
              <el-carousel-item class="card-box" v-for="item in tableIndex" :key="item.name" @click.native="showTableIndexDetal(item)" :class="{'table-index-active': currentShowTableIndex && currentShowTableIndex.id === item.id}">
                <img v-if="item.manual" class="icon-tableindex-type" src="../../../../assets/img/icon_model/index_manual.png"/>
                <img v-else class="icon-tableindex-type" src="../../../../assets/img/icon_model/index_auto.png"/>
                <div class="card-content" :class="{'empty-table-index': item.status === 'EMPTY', 'is-manual': item.manual}">
                  <div class="slider-content-above">
                    <div class="main-title" :title="item.name">{{item.name|omit(30, '...')}}</div>
                    <div class="status-list">
                      <!-- <div v-if="item.status === 'AVAIABLE'" class="broken-icon">{{$t('available ')}}</div> -->
                      <div v-if="item.status === 'BROKEN'" class="broken-icon">{{$t('broken')}}</div>
                      <div v-if="item.status === 'EMPTY'" class="empty-icon">{{$t('empty')}}</div>
                    </div>
                    <div class="sub-info">
                      <div>{{$t('tableIndexId')}}{{item.id}}</div>
                      <i class="el-icon-ksd-elapsed_time ksd-mr-4"></i>{{transToGmtTime(item.update_time)}}
                       <div class="actions ksd-fright">
                        <span class="del-icon" v-if="item.manual" @click="delTableIndex(item.id)">{{$t('kylinLang.common.delete')}}</span>
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
          <span><i class="el-icon-ksd-index_handy" v-if="currentShowTableIndex && currentShowTableIndex.manual"></i><i v-if="currentShowTableIndex && !currentShowTableIndex.manual" class="el-icon-ksd-index_auto"></i> {{$t('tableIndexDetail')}}</span>
          <div class="ksd-fright ksd-fs-14"><span class="ksd-mr-4" v-if="currentShowTableIndex && currentShowTableIndex.manual">{{$t('manualAdvice')}}</span><span class="ksd-mr-4" v-else>{{$t('autoAdvice')}}</span><el-button size="mini" v-if="currentShowTableIndex && currentShowTableIndex.manual" @click="editTableIndex(false)"  icon="el-icon-ksd-table_edit">{{$t('kylinLang.common.edit')}}</el-button></div>
        </div>
        <div class="ksd-prl-20 ksd-ptb-8">
          <el-table
          :data="showTableIndexDetail"
          border class="ksd-mt-14 table-index-detail">
          <el-table-column
            :label="$t('ID')"
            header-align="center"
            align="center"
            prop="id"
            width="60">
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('column')"
            header-align="center"
            prop="column"
            align="center">
          </el-table-column> 
          <el-table-column
          label="Sort"
          header-align="center"
          prop="sort"
          width="60"
          align="center">
          <template slot-scope="scope">
            <span class="ky-dot-tag" v-show="scope.row.sort">{{scope.row.sort}}</span>
          </template>
            </el-table-column>
          <el-table-column
          label="Shard"
          width="70"
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
      'currentSelectedProject',
      'isAutoProject'
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
    kapConfirm(this.$t('kylinLang.common.confirmDel')).then(() => {
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
        this.currentShowTableIndex = data.table_indexs[data.table_indexs.length - 1]
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
    }).then((res) => {
      if (res.isSubmit) {
        this.getAllTableIndex()
      }
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
  .icon-tableindex-type {
    width:24px;
    height:28px;
    position:absolute;
    left:10px;
    top:5px;
    z-index: 1;
  }
  .card-box {
    width:328px;
    &.table-index-active {
      .card-content {
        &.is-manual {
          border: solid 1px @normal-color-1!important;
        }
        border: solid 1px @warning-color-1!important;
      }
    }
  }
  
  .table-index-detail {
    th {
      background-color:@base-color-9;
    }
  }
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
    padding-left:20px!important;
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
  .el-carousel__item{
    h3 {
      color: #475669;
      font-size: 14px;
      opacity: 0.75;
      line-height: 200px;
      margin: 0;
    }
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
    // background-color: @grey-4;
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
      color:@text-normal-color;
      border: 1px solid @text-secondary-color;
      border-radius: 2px;
      width:56px;
      height:24px;
      line-height:24px;
      text-align:center;
      background:@grey-4;
      // color:@text-disabled-color;
      font-size:12px;
    }
    .status-list {
      position:absolute;
      right:11px;
      top:12px;
      height:34px;
      padding-top:10px;
    }
    height:132px;
    padding:20px;
    .del-icon {
      color:@base-color;
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
      .actions {
        position: absolute;
        right: 11px;
        bottom: 48px;
      }
    }
    .main-title {
      font-size: 16px;
      color:@text-title-color; 
      i {
        color:@warning-color-1;
      }
    }
  }
  .el-carousel__item{
    .card-content {
      width:320px;
      position:relative;
      background:@fff;
      border-radius:2px;
      margin-top:10px;
      border:solid 1px @text-placeholder-color;
      box-shadow: 0 0 4px 0 @text-placeholder-color;
      overflow: hidden;
      &:hover {
        box-shadow: 0 0 8px 0 @text-placeholder-color;
      }
    }
  }
}
</style>
