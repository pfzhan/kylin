<template>
  <div class="table-index-view">
    <div class="title-list">
      <div class="title-header">
        <el-select v-model="indexType" size="small" disabled class="index-types">
          <el-option
            v-for="item in options"
            :key="item"
            :label="$t(item)"
            :value="item">
          </el-option>
        </el-select>
        <div class="icon-group ksd-fright" v-if="isShowTableIndexActions&&!isHideEdit">
          <i class="el-icon-ksd-project_add" @click="editTableIndex()"></i>
        </div>
      </div>
      <ul v-if="indexDatas.length">
        <li class="table-index-title" @click="scrollToMatched(indexIdx)" v-for="(index, indexIdx) in indexDatas" :key="indexIdx">
          {{$t('tableIndexTitle', { indexId: index.id })}}
        </li>
      </ul>
      <kap-nodata v-else>
      </kap-nodata>
    </div>
    <div class="table-index-detail-block" v-if="indexDatas.length">
      <div class="table-index-detail ksd-mb-30" v-for="index in indexDatas" :key="index.id">
        <div class="table-index-content-title">
          <span class="ksd-fs-16">
            {{$t('tableIndexTitle', { indexId: index.id })}}
          </span><span class="index-type ksd-ml-10">
            {{$t('custom')}}</span><span class="index-time ksd-ml-15">
            <i class="el-icon-ksd-elapsed_time"></i>
            {{index.last_modified_time | toServerGMTDate}}
          </span>
          <span class="ksd-fright icon-group">
            <i class="el-icon-ksd-table_edit" @click="editTableIndex(index)"></i><i class="el-icon-ksd-table_delete ksd-ml-10" @click="removeIndex(index)"></i>
          </span>
        </div>
        <div class="table-index-content">
          <el-table
          size="medium"
          :data="getIndexDetail(index)"
          border class="index-details">
          <el-table-column
            :label="$t('ID')"
            prop="id"
            width="64">
          </el-table-column>
          <el-table-column
            show-overflow-tooltip
            :label="$t('column')"
            prop="column">
          </el-table-column>
          <el-table-column
          :label="$t('sort')"
          prop="sort"
          width="60"
          align="center">
          <template slot-scope="scope">
            <span class="ky-dot-tag" v-show="scope.row.sort">{{scope.row.sort}}</span>
          </template>
            </el-table-column>
          <el-table-column
          label="Shard"
          align="center"
          width="70">
            <template slot-scope="scope">
                <i class="el-icon-ksd-good_health ky-success" v-show="scope.row.shared"></i>
            </template>
            </el-table-column>
          </el-table>
          <!-- <kap-pager layout="prev, pager, next" :background="false" class="ksd-mt-10 ksd-center" ref="pager" :perpage_size="index.detailCurrentCount" :curPage="index.detailCurrentPage+1" :totalSize="index.col_order.length"  v-on:handleCurrentChange='(index) => currentChange(size, count, index)'></kap-pager> -->
        </div>
      </div>
    </div>
    <div class="table-index-detail-block" v-else>
      <div class="empty-block">
        <div>{{$t('aggTableIndexTips')}}</div>
        <el-button type="primary" text icon="el-icon-ksd-table_add" @click="editTableIndex()" v-if="isShowTableIndexActions&&!isHideEdit">{{$t('tableIndex')}}</el-button>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import locales from './locales'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync, handleError, kapConfirm } from 'util'
import NModel from '../../ModelEdit/model.js'
@Component({
  props: {
    model: {
      type: Object
    },
    projectName: {
      type: String
    },
    isShowTableIndexActions: {
      type: Boolean,
      default: true
    },
    isHideEdit: {
      type: Boolean,
      default: false
    }
  },
  computed: {
    ...mapGetters([
      'currentProjectData',
      'isAutoProject'
    ]),
    modelInstance () {
      this.model.project = this.currentProjectData.name
      return new NModel(this.model)
    }
  },
  methods: {
    ...mapActions({
      loadAllIndex: 'LOAD_ALL_INDEX',
      deleteIndex: 'DELETE_INDEX'
    }),
    ...mapActions('TableIndexEditModal', {
      showTableIndexEditModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class TableIndexView extends Vue {
  indexType = 'custom'
  options = ['custom']
  filterArgs = {
    page_offset: 0,
    page_size: 999,
    key: '',
    sort_by: '',
    reverse: '',
    sources: ['MANUAL_TABLE'],
    status: []
  }
  indexDatas = []
  async editTableIndex (indexDesc) {
    const isSubmit = await this.showTableIndexEditModal({
      modelInstance: this.modelInstance,
      tableIndexDesc: indexDesc || {name: 'TableIndex_1'}
    })
    isSubmit && this.loadTableIndices()
    isSubmit && this.$emit('loadModels')
  }
  async removeIndex (index) {
    try {
      await kapConfirm(this.$t('delIndexTip'), {confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('delIndex'))
      await this.deleteIndex({project: this.projectName, model: this.model.uuid, id: index.id})
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
      this.loadTableIndices()
      this.$emit('loadModels')
    } catch (e) {
      handleError(e)
    }
  }
  async loadTableIndices () {
    try {
      const res = await this.loadAllIndex(Object.assign({
        project: this.projectName,
        model: this.model.uuid
      }, this.filterArgs))
      const data = await handleSuccessAsync(res)
      this.indexDatas = data.value.map((d) => {
        d.detailCurrentPage = 0
        d.detailCurrentCount = 10
        return d
      })
    } catch (e) {
      handleError(e)
    }
  }
  scrollToMatched (index) {
    this.$nextTick(() => {
      const detailContents = this.$el.querySelectorAll('.table-index-detail-block .table-index-detail')
      this.$el.querySelector('.table-index-detail-block').scrollTop = detailContents[index].offsetTop - 15
    })
  }
  mounted () {
    this.loadTableIndices()
  }
  getIndexDetail (index) {
    // let tableIndexList = index.col_order.slice(index.detailCurrentCount * index.detailCurrentPage, index.detailCurrentCount * (index.detailCurrentPage + 1))
    let renderData = []
    renderData = index.col_order.map((item, i) => {
      let newitem = {
        id: index.detailCurrentCount * index.detailCurrentPage + i + 1,
        column: item.key,
        sort: index.sort_by_columns.indexOf(item.key) + 1 || '',
        shared: index.shard_by_columns.includes(item.key)
      }
      return newitem
    })
    return renderData
  }
  currentChange (size, count, index) {
    index.detailCurrentPage = size
    index.detailCurrentCount = count
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.table-index-view {
  height: 500px;
  overflow: hidden;
  width: 100%;
  .title-list {
    width: 220px;
    height: 100%;
    box-shadow:2px 2px 4px 0px rgba(229,229,229,1);
    background-color: @fff;
    float:left;
    overflow-y: auto;
    padding-top: 15px;
    position: relative;
    .title-header {
      padding: 0 10px 10px 10px;
      line-height: 24px;
      .index-types {
        width: 140px;
      }
    }
    .table-index-title {
      height: 40px;
      line-height: 40px;
      cursor: pointer;
      padding-left: 10px;
      font-size: 14px;
      &:hover {
        background-color: @base-color-9;
      }
    }
  }
  .table-index-detail-block {
    height: 100%;
    margin: 0 15px 0 235px;
    background-color: @fff;
    overflow-y: auto;
    padding-top: 15px;
    position: relative;
    .table-index-content-title {
      height: 40px;
      line-height: 40px;
      background-color: @background-disabled-color;
      padding: 0 20px 0 10px;
      margin-bottom: 20px;
      .index-type {
        font-size: 12px;
        border: 1px solid @text-disabled-color;
        color: @text-disabled-color;
        border-radius: 2px;
        padding: 0 2px;
      }
      .index-time {
        font-size: 14px;
        color: @text-disabled-color;
      }
    }
    .empty-block {
      text-align: center;
      color:@text-disabled-color;
      position: absolute;
      top: 50%;
      text-align: center;
      width: 100%;
    }
  }
}
</style>
