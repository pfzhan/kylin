<template>
  <el-dialog append-to-body limited-area :title="$t('addJoinCondition')" @close="isShow && handleClose(false)" width="720px" :visible="isShow" class="links-dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <el-row :gutter="10">
      <el-col :span="10">
        <el-select :placeholder="$t('kylinLang.common.pleaseSelect')" @change="changeFTable" style="width:100%" filterable v-model="selectF">
          <el-option  v-for="key in selectedFTables" :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
      <el-col :span="4">
        <el-select :placeholder="$t('kylinLang.common.pleaseSelect')" style="width:100%" v-model="joinType">
          <el-option :value="key" v-for="(key, i) in linkKind" :key="i">{{key}}</el-option>
        </el-select>
      </el-col>
      <el-col :span="10">
        <el-select :placeholder="$t('kylinLang.common.pleaseSelect')"  @change="changePTable" size="medium" style="width:100%" filterable v-model="selectP">
          <el-option v-for="key in selectedPTables"  :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
    </el-row>
    <div class="ky-line ksd-mt-15"></div>
    <!-- 列的关联 -->
    <el-form class="ksd-mt-10" ref="conditionForm" :model="joinColumns">
      <el-form-item v-for="(key, val) in joinColumns.foreign_key" :key="val" class="ksd-mb-6">
          <el-col :span="10">
            <el-form-item :prop="'foreign_key.' + val" :rules="[{validator: checkIsBrokenForeignKey, trigger: 'change'}]">
              <el-select size="small"  style="width:100%" filterable v-model="joinColumns.foreign_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
                <el-option :disabled="true" v-if="checkIsBroken(brokenForeignKeys, joinColumns.foreign_key[val])" :value="joinColumns.foreign_key[val]" :label="joinColumns.foreign_key[val].split('.')[1]"></el-option>
                <el-option v-for="f in fColumns" :value="fTable.alias+'.'+f.name" :key="f.name" :label="f.name">
                </el-option>
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="1" class="ksd-center" style="font-size:20px;">
            =
          </el-col>
          <el-col :span="11">
            <el-form-item :prop="'primary_key.' + val" :rules="[{validator: checkIsBrokenPrimaryKey, trigger: 'change'}]">
              <el-select size="small" style="width:100%" filterable v-model="joinColumns.primary_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
                <el-option :disabled="true" v-if="checkIsBroken(brokenPrimaryKeys, joinColumns.primary_key[val])" :value="joinColumns.primary_key[val]" :label="joinColumns.primary_key[val].split('.')[1]"></el-option>
                <el-option v-for="p in pColumns" :value="pTable.alias+'.'+p.name" :key="p.name" :label="p.name">
                </el-option>
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="2" class="ksd-right">
            <el-button  type="primary" plain icon="el-icon-ksd-add_2" size="mini" @click="addJoinConditionColumns" circle></el-button><el-button  icon="el-icon-minus" size="mini" @click="removeJoinConditionColumn(val)" circle></el-button>
          </el-col>
      </el-form-item>
    </el-form>
    <span slot="footer" class="dialog-footer">
      <!-- <el-button @click="delConn" v-if="currentConnObj" size="medium" class="ksd-fleft">{{$t('delConn')}}</el-button> -->
      <el-button plain @click="isShow && handleClose(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button v-guide.saveJoinBtn size="medium" @click="saveJoinCondition">{{$t('kylinLang.common.ok')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'
import { modelRenderConfig } from '../ModelEdit/config'
import vuex from '../../../../store'
import { objectClone } from '../../../../util'
import locales from './locales'
import store, { types } from './store'
import { kapMessage, kapConfirm } from 'util/business'
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
      this.$nextTick(() => {
        this.$refs.conditionForm && this.$refs.conditionForm.validate()
      })
      this.selectP = this.form.pid || ''
      this.selectF = this.form.fid || ''
      this.joinType = this.form.joinType || 'INNER'
      let ptable = this.form.tables[this.selectP]
      // let ftable = this.form.tables[this.selectF]
      let joinData = ptable && ptable.getJoinInfoByFGuid(this.selectF) || null
      if (joinData) { // 有join数据的情况
        var joinInfo = joinData.join
        this.joinColumns.foreign_key = objectClone(joinInfo.foreign_key)
        this.joinColumns.primary_key = objectClone(joinInfo.primary_key)
        this.joinType = joinInfo.type
      } else { // 无join数据的情况,设置默认值
        this.$set(this.joinColumns, 'foreign_key', [''])
        this.$set(this.joinColumns, 'primary_key', [''])
      }
      // 拖动添加默认填充
      // 如果添加的是重复的连接条件不做处理
      if (this.form.fColumnName && this.form.pColumnName) {
        let findex = this.joinColumns.foreign_key.indexOf(this.form.fColumnName)
        let pindex = this.joinColumns.primary_key.indexOf(this.form.pColumnName)
        if (findex === pindex && findex >= 0) {
          return
        }
      }
      if (this.form.fColumnName) {
        if (this.joinColumns.foreign_key[0]) {
          this.joinColumns.foreign_key.push(this.form.fColumnName)
        } else {
          this.joinColumns.foreign_key[0] = this.form.fColumnName
        }
      }
      if (this.form.pColumnName) {
        if (this.joinColumns.primary_key[0]) {
          this.joinColumns.primary_key.push(this.form.pColumnName)
        } else {
          this.joinColumns.primary_key[0] = this.form.pColumnName
        }
      }
    }
  }
  changeFTable () {
    this.joinColumns.foreign_key = ['']
  }
  changePTable () {
    this.joinColumns.primary_key = ['']
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
  checkIsBrokenPrimaryKey (rule, value, callback) {
    if (value) {
      if (this.checkIsBroken(this.brokenPrimaryKeys, value)) {
        return callback(new Error(this.$t('noColumnFund')))
      }
    }
    callback()
  }
  checkIsBrokenForeignKey (rule, value, callback) {
    if (value) {
      if (this.checkIsBroken(this.brokenForeignKeys, value)) {
        return callback(new Error(this.$t('noColumnFund')))
      }
    }
    callback()
  }
  checkIsBroken (brokenList, key) {
    if (key) {
      return ~brokenList.indexOf(key)
    }
    return false
  }
  get brokenPrimaryKeys () {
    let ntable = this.pTable
    if (ntable && this.isShow) {
      return this.form.modelInstance.getBrokenModelLinksKeys(ntable.guid, this.joinColumns.primary_key)
    }
    return []
  }
  get brokenForeignKeys () {
    let ntable = this.fTable
    if (ntable && this.isShow) {
      return this.form.modelInstance.getBrokenModelLinksKeys(ntable.guid, this.joinColumns.foreign_key)
    }
    return []
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
  removeDuplicateCondition (fkeys, pkeys) {
    let obj = {}
    for (let i = fkeys.length - 1; i >= 0; i--) {
      if (obj[fkeys[i] + pkeys[i]]) {
        fkeys.splice(i, 1)
        pkeys.splice(i, 1)
      } else {
        obj[fkeys[i] + pkeys[i]] = true
      }
    }
  }
  checkLinkCompelete () {
    if (!this.selectF || !this.selectP || this.joinColumns.foreign_key.indexOf('') >= 0 || this.joinColumns.primary_key.indexOf('') >= 0) {
      return false
    }
    return true
  }
  get currentConnObj () {
    if (this.form.modelInstance) {
      return this.form.modelInstance.getConn(this.selectP, this.selectF)
    }
  }
  delConn () {
    kapConfirm(this.$t('delConnTip'), null, this.$t('delConnTitle')).then(() => {
      if (this.form.modelInstance) {
        this.form.modelInstance
        if (this.currentConnObj) {
          this.form.modelInstance.removeRenderLink(this.currentConnObj)
          this.handleClose(false)
        }
      }
    })
  }
  async saveJoinCondition () {
    await this.$refs.conditionForm.validate()
    var joinData = this.joinColumns // 修改后的连接关系
    var selectF = this.selectF // 外键表名
    var selectP = this.selectP // 主键表名
    if (this.checkLinkCompelete()) {
      // 校验是否链接层环状
      if (this.form && this.form.modelInstance.checkLinkCircle(selectF, selectP)) {
        kapMessage(this.$t('kylinLang.model.cycleLinkTip'), {type: 'warning'})
        return
      }
      // 删除重复的条件
      this.removeDuplicateCondition(this.joinColumns.foreign_key, this.joinColumns.primary_key)
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
  .error-msg {display:none}
  .is-broken {
    .el-input__inner{
      border:solid 1px @color-danger;
    }
    .error-msg {
      color:@color-danger;
      display:block;
    }
  }
}

</style>
