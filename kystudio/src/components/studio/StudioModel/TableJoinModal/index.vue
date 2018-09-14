<template>
  <el-dialog :title="$t('addJoinCondition')" @close="isShow && handleClose(false)" v-event-stop  width="660px" :visible="isShow" class="links_dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <el-row :gutter="10">
      <el-col :span="10">
        <el-select :popper-append-to-body="false" style="width:100%" v-model="selectF">
          <el-option  v-for="(key, val) in form.tables" :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
      <el-col :span="4">
        <el-select :popper-append-to-body="false" style="width:100%" v-model="joinType">
          <el-option :value="key" v-for="(key, value) in linkKind" :key="key">{{key}}</el-option>
        </el-select>
      </el-col>
      <el-col :span="10">
        <el-select :popper-append-to-body="false" style="width:100%" v-model="selectP">
          <el-option v-for="(key, val) in form.tables"  :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
    </el-row>
    <el-row :gutter="10"  class="ksd-mt-20" v-for="(key, val) in joinColumns.foreign_key" :key="val">
      <el-col :span="10">
         <el-select :popper-append-to-body="false"  style="width:100%" v-model="joinColumns.foreign_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option v-for="f in form.foreignTable.columns" :value="form.foreignTable.alias+'.'+f.name" :key="f.name" :label="f.name">
            </el-option>
          </el-select>
      </el-col>
      <el-col :span="1" class="ksd-center" style="font-size:20px;">
         =
      </el-col>
      <el-col :span="9">
        <el-select :popper-append-to-body="false" style="width:100%" v-model="joinColumns.primary_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option v-for="p in form.primaryTable.columns" :value="form.primaryTable.alias+'.'+p.name" :key="p.name" :label="p.name">
            </el-option>
          </el-select>
      </el-col>
      <el-col :span="4" class="ksd-center">
        <el-button  icon="el-icon-plus"></el-button>
          <el-button  icon="el-icon-delete"></el-button>
      </el-col>
    </el-row>
    <span slot="footer" class="dialog-footer">
      <el-button @click="isShow && handleClose(false)" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" plain size="medium">{{$t('kylinLang.common.ok')}}</el-button>
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
vuex.registerModule(['modals', 'TableJoinModal'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('TableJoinModal', {
      isShow: state => state.isShow,
      form: state => state.form
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
  linkKind = modelRenderConfig.joinKind
  joinType = '' // 选择类型
  selectF = '' // 选择的外键表
  selectP = '' // 选择的主键表
  joinColumns = {} // join信息
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
      if (Object.keys(this.form.primaryTable).length) {
        var joinInfo = this.form.primaryTable.joinInfo[this.form.primaryTable.guid].join
        this.selectP = this.form.primaryTable.alias
        this.joinColumns = joinInfo
        this.joinType = joinInfo.type
      } else {
        this.joinType = 'INNER'
        this.joinColumns.foreign_key = this.form.ftableName ? [this.form.ftableName] : []
        this.joinColumns.primary_key = ['']
      }
      if (this.form.foreignTable) {
        this.selectF = this.form.foreignTable.alias
      }
    }
  }

  handleClose (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
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
