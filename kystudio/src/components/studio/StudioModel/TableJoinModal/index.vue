<template>
  <el-dialog append-to-body :title="$t('addJoinCondition')" @close="isShow && handleClose(false)" width="660px" :visible="isShow" class="links-dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <el-row :gutter="10">
      <el-col :span="10">
        <el-select  size="medium" style="width:100%" filterable v-model="selectF">
          <el-option  v-for="key in selectedFTables" :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
      <el-col :span="4">
        <el-select  size="medium" style="width:100%" v-model="joinType">
          <el-option :value="key" v-for="(key, i) in linkKind" :key="i">{{key}}</el-option>
        </el-select>
      </el-col>
      <el-col :span="10">
        <el-select size="medium" style="width:100%" filterable v-model="selectP">
          <el-option v-for="key in selectedPTables"  :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
    </el-row>
    <!-- 列的关联 -->
    <el-row :gutter="10"  class="ksd-mt-20" v-for="(key, val) in joinColumns.foreign_key" :key="val">
      <el-col :span="10">
         <el-select size="medium"  style="width:100%" filterable v-model="joinColumns.foreign_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option v-for="f in fColumns" :value="fTable.alias+'.'+f.name" :key="f.name" :label="f.name">
            </el-option>
          </el-select>
      </el-col>
      <el-col :span="1" class="ksd-center" style="font-size:20px;">
         =
      </el-col>
      <el-col :span="9">
        <el-select size="medium" style="width:100%" filterable v-model="joinColumns.primary_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option v-for="p in pColumns" :value="pTable.alias+'.'+p.name" :key="p.name" :label="p.name">
            </el-option>
          </el-select>
      </el-col>
      <el-col :span="4" class="ksd-left ksd-pt-2">
        <el-button  type="primary" plain icon="el-icon-ksd-add_2" size="mini" @click="addJoinConditionColumns" circle></el-button><el-button  icon="el-icon-minus" size="mini" @click="removeJoinConditionColumn(val)" circle></el-button>
      </el-col>
    </el-row>
    <span slot="footer" class="dialog-footer">
      <el-button @click="isShow && handleClose(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" plain size="medium" @click="saveJoinCondition">{{$t('kylinLang.common.ok')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'
import { modelRenderConfig } from '../ModelEdit/config'
import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { kapMessage } from 'util/business'
vuex.registerModule(['modals', 'TableJoinModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('TableJoinModal', {
      isShow: state => state.isShow,
      form: state => state.form,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('TableJoinModal', {
      hideModal: types.HIDE_MODAL,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
    })
  },
  locales
})
export default class TableJoinModal extends Vue {
  isLoading = false
  isFormShow = false
  linkKind = modelRenderConfig.joinKind // 默认可选的连接类型
  joinType = '' // 选择连接的类型
  selectF = '' // 选择的外键表的alias名
  selectP = '' // 选择的主键表的alias名
  joinColumns = {
    foreign_key: [''],
    primary_key: ['']
  } // join信息
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
      this.selectP = this.form.pid || ''
      this.selectF = this.form.fid || ''
      let ptable = this.form.tables[this.selectP]
      // let ftable = this.form.tables[this.selectF]
      let joinData = ptable && ptable.getJoinInfo() || null
      if (joinData) { // 有join数据的情况
        var joinInfo = joinData.join
        this.joinColumns.foreign_key = joinInfo.foreign_key
        this.joinColumns.primary_key = joinInfo.primary_key
        this.joinType = joinInfo.type
      } else { // 无join数据的情况,设置默认值
        this.joinType = 'INNER'
        this.$set(this.joinColumns, 'foreign_key', [''])
        this.$set(this.joinColumns, 'primary_key', [''])
      }
      if (this.form.fColumnName && !this.joinColumns.foreign_key.includes(this.form.fColumnName)) {
        if (this.joinColumns.foreign_key[0]) {
          this.joinColumns.foreign_key.push(this.form.fColumnName)
        } else {
          this.joinColumns.foreign_key[0] = this.form.fColumnName
        }
      }
    }
  }
  get selectedFTables () {
    return this.form.tables && Object.values(this.form.tables).filter((t) => {
      if (t.guid !== this.selectP) {
        return t
      }
    }) || []
  }
  get selectedPTables () {
    return this.form.tables && Object.values(this.form.tables).filter((t) => {
      if (t.guid !== this.selectF) {
        return t
      }
    }) || []
  }
  get fColumns () {
    let ntable = this.fTable
    if (ntable) {
      return ntable.columns
    }
    return []
  }
  get pColumns () {
    let ntable = this.pTable
    if (ntable) {
      return ntable.columns
    }
    return []
  }
  get fTable () {
    return this.form.tables && this.form.tables[this.selectF] || []
  }
  get pTable () {
    return this.form.tables && this.form.tables[this.selectP] || []
  }
  // 添加condition关联列的框
  addJoinConditionColumns () {
    this.joinColumns.foreign_key.unshift('')
    this.joinColumns.primary_key.unshift('')
  }
  // 删除condition关联列的框
  removeJoinConditionColumn (i) {
    if (this.joinColumns.foreign_key.length === 1) {
      this.joinColumns.foreign_key.splice(0, 1, '')
      this.joinColumns.primary_key.splice(0, 1, '')
      return
    }
    this.joinColumns.foreign_key.splice(i, 1)
    this.joinColumns.primary_key.splice(i, 1)
  }
  checkLinkCompelete () {
    if (!this.selectF || !this.selectP || this.joinColumns.foreign_key.indexOf('') >= 0 || this.joinColumns.primary_key.indexOf('') >= 0) {
      return true
    }
    return true
  }
  saveJoinCondition () {
    var joinData = this.joinColumns // 修改后的连接关系
    var selectF = this.selectF // 外键表名
    var selectP = this.selectP // 主键表名
    if (this.checkLinkCompelete()) {
      // 传出处理后的结果
      this.handleClose(true, {
        selectF: selectF,
        selectP: selectP,
        joinData: joinData,
        joinType: this.joinType
      })
    } else {
      kapMessage(this.$t('checkCompleteLink'), {
        type: 'warning'
      })
    }
  }
  handleClose (isSubmit, data) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback({
        isSubmit: isSubmit,
        data: data
      })
    }, 300)
  }
  handleClick () {
  }
  mounted () {
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.links-dialog {
  .el-button+.el-button {
    margin-left:5px;
  }
}

</style>
