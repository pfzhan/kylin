<template>
  <el-dialog :title="$t('addJoinCondition')" @close="isShow && handleClose(false)" v-event-stop  width="660px" :visible="isShow" class="links_dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <el-row :gutter="10">
      <el-col :span="10">
        <el-select :popper-append-to-body="false" style="width:100%" filterable v-model="selectF">
          <el-option  v-for="(key, val) in form.tables" :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
      <el-col :span="4">
        <el-select :popper-append-to-body="false" style="width:100%" v-model="joinType">
          <el-option :value="key" v-for="(key, value) in linkKind" :key="key">{{key}}</el-option>
        </el-select>
      </el-col>
      <el-col :span="10">
        <el-select :popper-append-to-body="false" style="width:100%" filterable v-model="selectP">
          <el-option v-for="(key, val) in form.tables"  :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
    </el-row>
    <!-- 列的关联 -->
    <el-row :gutter="10"  class="ksd-mt-20" v-for="(key, val) in joinColumns.foreign_key" :key="val">
      <el-col :span="10">
         <el-select :popper-append-to-body="false"  style="width:100%" filterable v-model="joinColumns.foreign_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option v-for="f in form.foreignTable.columns" :value="form.foreignTable.alias+'.'+f.name" :key="f.name" :label="f.name">
            </el-option>
          </el-select>
      </el-col>
      <el-col :span="1" class="ksd-center" style="font-size:20px;">
         =
      </el-col>
      <el-col :span="9">
        <el-select :popper-append-to-body="false" style="width:100%" filterable v-model="joinColumns.primary_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option v-for="p in form.primaryTable.columns" :value="form.primaryTable.alias+'.'+p.name" :key="p.name" :label="p.name">
            </el-option>
          </el-select>
      </el-col>
      <el-col :span="4" class="ksd-center">
        <el-button  icon="el-icon-plus" @click="addJoinConditionColumns" circle></el-button>
          <el-button  icon="el-icon-delete" @click="removeJoinConditionColumn(val)" circle></el-button>
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
  joinColumns = {} // join信息
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
      let joinData = this.form.primaryTable.joinInfo[this.form.primaryTable.guid]
      this.selectP = this.form.primaryTable.guid
      if (joinData) { // 有join数据的情况
        var joinInfo = joinData.join
        this.joinColumns = joinInfo
        this.joinType = joinInfo.type
      } else { // 无join数据的情况,设置默认值
        this.joinType = 'INNER'
        this.$set(this.joinColumns, 'foreign_key', [''])
        this.$set(this.joinColumns, 'primary_key', [''])
      }
      if (this.form.foreignTable) {
        this.selectF = this.form.foreignTable.guid
      }
    }
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
      return false
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
  destroyed () {
    if (!module.hot) {
      vuex.unregisterModule(['modals', 'TableJoinModal'])
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
</style>
