<template>
  <!-- tableindex的添加和编辑 -->
  <el-dialog :title="$t('editTableIndexTitle')" class="table-edit-dialog" width="660px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="tableIndexMeta" :rules="rules" ref="tableIndexForm" >
        <el-form-item :label="$t('tableIndexName')" prop="name">
          <el-input v-model="tableIndexMeta.name" auto-complete="off" size="medium"></el-input>
        </el-form-item>
      </el-form>
      <div class="ky-line-full"></div>
      <div class="ky-list-title sub-title">{{$t('tableIndexContent')}}</div>
      <div class="ksd-mt-20">
        <el-steps direction="vertical" :active="3">
          <el-step :title="$t('selectColumns')">
            <div slot="description"  class="ksd-mb-20">
              <div class="ksd-mt-14 ksd-mb-16">
                <div class="actions"><el-button @click="selectAll" plain  type="primary" size="medium">{{$t('selectAllColumns')}}</el-button> <el-button @click="clearAll" size="medium">{{$t('clearAll')}}</el-button> </div>
                <ul class="table-index-columns">
                  <li v-for='(col, index) in tableIndexMeta.col_order' :key='col'>
                    <span class="sort-icon ksd-mr-10">{{index + 1 + pager * 10}}</span>
                    <el-select v-model="tableIndexMeta.col_order[index]" filterable style="width:420px">
                      <el-option
                        v-for="item in allColumns"
                        :key="item.full_colname"
                        :label="item.full_colname"
                        :value="item.full_colname">
                      </el-option>
                    </el-select>
                    <!-- <el-input style="width:430px" size="medium" v-model="tableIndexMeta.col_order[index]"></el-input> -->
                    <el-button circle plain  type="primary" icon="el-icon-plus" size="small" @click="addCol('col_order', index)" class="ksd-ml-10"></el-button>
                    <el-button circle size="small" icon="el-icon-minus" @click="delCol('col_order', index)"></el-button> 
                  </li>
                </ul>
                <!-- <div class="show-pagers">
                  <ul>
                    <li v-for="x in totalPage" :key="x" @click="pagerChange(x - 1)">
                      <span>{{(x - 1)*10 + 1}}</span>
                         /
                        <span>{{ (x) * 10}}</span>
                    </li>
                  </ul>
                </div> -->
              </div>
            </div>
          </el-step>
          <el-step title="Sort By">
            <div slot="description" class="ksd-mb-20">
              <div class="ksd-mt-14 ksd-mb-16">
                <ul class="table-index-columns">
                  <li v-for='(col, index) in tableIndexMeta.sort_by_columns' :key='col'>
                    <span class="sort-icon ksd-mr-10">{{index + 1}}</span>
                    <el-select v-model="tableIndexMeta.sort_by_columns[index]" filterable style="width:420px" placeholder="请选择">
                      <el-option
                        v-for="item in sortByColumns"
                        :key="item"
                        :label="item"
                        :value="item">
                      </el-option>
                    </el-select>
                    <!-- <el-input style="width:430px" size="medium" v-model="tableIndexMeta.sort_by_columns[index]"></el-input> -->
                    <el-button circle plain type="primary" icon="el-icon-plus" size="small"  @click="addCol('sort_by_columns', index)" class="ksd-ml-10"></el-button>
                    <el-button circle size="small" icon="el-icon-minus" @click="delCol('sort_by_columns', index)"></el-button> 
                  </li>
                </ul>
              </div>
            </div>
          </el-step>
          <el-step title="Shard By">
            <div slot="title">
              Shard By
              <el-switch
                v-model="openShared"
                active-text="OFF"
                inactive-text="ON"
                :active-value="true"
                :inactive-value="false">
              </el-switch>
            </div>
            <div slot="description" v-show="openShared" class="ksd-mt-20">
              <el-select v-model="tableIndexMeta.shard_by_columns[0]" filterable style="width:100%" placeholder="请选择">
                <el-option
                  v-for="item in selectedColumns"
                  :key="item"
                  :label="item"
                  :value="item">
                </el-option>
              </el-select>
            </div>
          </el-step>
        </el-steps>
      </div>
      <div slot="footer" class="dialog-footer">
        <el-button @click="closeModal" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain :loading="btnLoading" size="medium" @click="submit">{{$t('kylinLang.common.save')}}</el-button>
      </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component, Watch } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import vuex from '../../../../store'
  import { NamedRegex } from 'config'
  import { handleError } from 'util/business'
  import { objectClone, arrSortByArr } from 'util/index'
  import locales from './locales'
  import store, { types } from './store'

  vuex.registerModule(['modals', 'TableIndexEditModal'], store)

  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('TableIndexEditModal', {
        isShow: state => state.isShow,
        modelInstance: state => state.form.data.modelInstance,
        tableIndexDesc: state => objectClone(state.form.data.tableIndexDesc),
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        editTableIndex: 'EDIT_TABLE_INDEX',
        addTableIndex: 'ADD_TABLE_INDEX'
      }),
      ...mapMutations('TableIndexEditModal', {
        setModal: types.SET_MODAL,
        hideModal: types.HIDE_MODAL,
        setModalForm: types.SET_MODAL_FORM,
        resetModalForm: types.RESET_MODAL_FORM
      })
    },
    locales
  })
  export default class TableIndexEditModal extends Vue {
    btnLoading = false
    openShared = false
    pager = 0
    tableIndexMetaStr = JSON.stringify({
      id: '',
      name: '',
      col_order: [''],
      sort_by_columns: [''],
      shard_by_columns: []
    })
    tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
    rules = {
      name: [
        {validator: this.checkName, trigger: 'blur'}
      ]
    }
    get allColumns () {
      let modelUsedTables = []
      if (this.modelInstance) {
        modelUsedTables = this.modelInstance.getTableColumns() || []
      }
      return modelUsedTables.filter((item) => {
        return !this.selectedColumns.includes(item.full_colname)
      })
    }
    // get pagerShowOrder () {
    //   return this.tableIndexMeta.col_order.slice(this.pager * 10, 10 + 10 * this.pager)
    // }
    get sortByColumns () {
      let arr = Vue.filter('filterArr')(this.tableIndexMeta.col_order, '')
      let sortColumns = this.tableIndexMeta.sort_by_columns
      return arr.filter((item) => {
        return !sortColumns.includes(item)
      })
    }
    get selectedColumns () {
      return Vue.filter('filterArr')(this.tableIndexMeta.col_order, '')
    }
    // get totalPage () {
    //   let y = this.tableIndexMeta.col_order.length % 10 ? 1 : 0
    //   return Math.floor(this.tableIndexMeta.col_order.length / 10) + y
    // }
    @Watch('isShow')
    initTableIndex (val) {
      if (val && this.tableIndexDesc) {
        Object.assign(this.tableIndexMeta, this.tableIndexDesc)
      } else {
        this.tableIndexMeta = JSON.parse(this.tableIndexMetaStr)
      }
      this.openShared = this.tableIndexMeta.shard_by_columns.length > 0
    }
    pagerChange (pager) {
      this.pager = pager
    }
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    }
    clearAll () {
      this.tableIndexMeta.col_order = ['']
      this.tableIndexMeta.sort_by_columns = ['']
      this.tableIndexMeta.shard_by_columns = []
    }
    selectAll () {
      this.tableIndexMeta.col_order = []
      this.allColumns.forEach((col) => {
        this.tableIndexMeta.col_order.push(col.full_colname)
      })
    }
    addCol (dataSet, i) {
      this.tableIndexMeta[dataSet].splice(i, 0, '')
    }
    delCol (dataSet, i) {
      var data = this.tableIndexMeta[dataSet]
      if (data.length === 1) {
        data[0] = ''
      } else {
        data.splice(i, 1)
      }
    }
    closeModal (isSubmit) {
      this.hideModal()
      this.tableIndexMeta.name = ''
      setTimeout(() => {
        this.callback && this.callback({
          isSubmit: isSubmit
        })
        this.resetModalForm()
      }, 200)
    }
    async submit () {
      this.$refs.tableIndexForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        let successCb = () => {
          this.closeModal(true)
          this.btnLoading = false
        }
        let errorCb = (res) => {
          this.btnLoading = false
          handleError(res)
        }
        // 按照sort选中列的顺序对col_order进行重新排序
        this.tableIndexMeta.col_order = arrSortByArr(this.tableIndexMeta.col_order, this.tableIndexMeta.sort_by_columns)
        this.tableIndexMeta.project = this.currentSelectedProject
        this.tableIndexMeta.model = this.modelInstance.name
        if (this.tableIndexMeta.id) {
          this.editTableIndex(this.tableIndexMeta).then(successCb, errorCb)
        } else {
          this.addTableIndex(this.tableIndexMeta).then(successCb, errorCb)
        }
      })
    }
  }
</script>
<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .table-edit-dialog {
    .sub-title {
      margin-top:60px;
    }
    .show-pagers{
      width: 42px;
      position: absolute;
      top: 100px;
      right: -16px;
      ul {
        li {
          border:solid 1px #ccc;
          margin-bottom:12px;
          text-align:center;
        }
      }
    }
    .show-more-block {
      width:120px;
      height:20px;
      line-height:20px;
      text-align:center
    }
    .sort-icon {
      .ky-square-box(32px, 32px);
      background: @text-secondary-color;
      display: inline-block;
      color:@fff;
      vertical-align: baseline;
    }
    .table-index-columns {
      li {
        margin-top:20px;
        height:32px;
      }
    }
  } 
</style>
