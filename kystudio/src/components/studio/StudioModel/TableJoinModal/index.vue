<template>
  <el-dialog :title="$t('addJoinCondition')" @close="isShow && handleClose(false)" v-event-stop  width="660px" :visible.sync="isShow" class="links_dialog" :close-on-press-escape="false" :close-on-click-modal="false">
    <el-row :gutter="10">
      <el-col :span="10">
        <el-select style="width:100%" v-model="selectF">
          <el-option  v-for="(key, val) in form.tables" :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
      <el-col :span="4">
        <el-select style="width:100%" v-model="joinType">
          <el-option :value="key" v-for="(key, value) in linkKind" :key="key">{{key}}</el-option>
        </el-select>
      </el-col>
      <el-col :span="10">
        <el-select style="width:100%" v-model="selectP">
          <el-option v-for="(key, val) in form.tables"  :value="key.guid" :key="key.alias" :label="key.alias"></el-option>
        </el-select>
      </el-col>
    </el-row>
    <el-row :gutter="10"  class="ksd-mt-20" v-for="(key, val) in joinColumns.foreign_key" :key="val">
      <el-col :span="10">
         <el-select  style="width:100%" v-model="joinColumns.foreign_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option v-for="f in form.foreignTable.columns" :value="form.foreignTable.alias+'.'+f.name" :key="f.name" :label="f.name">
            </el-option>
          </el-select>
      </el-col>
      <el-col :span="1" class="ksd-center" style="font-size:20px;">
         =
      </el-col>
      <el-col :span="10">
        <el-select style="width:100%" v-model="joinColumns.primary_key[val]" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option v-for="p in form.primaryTable.columns" :value="form.primaryTable.alias+'.'+p.name" :key="p.name" :label="p.name">
            </el-option>
          </el-select>
      </el-col>
      <el-col :span="3" class="ksd-center">
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
// import { sourceTypes } from '../../../../config'
// import { titleMaps, cancelMaps, confirmMaps, getSubmitData } from './handler'
// import { handleSuccessAsync, handleError } from '../../../util'

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
      this.isFormShow = true
      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
      var joinInfo = this.form.primaryTable.joinInfo[this.form.primaryTable.guid].join
      this.joinType = joinInfo.type
      this.selectF = this.form.foreignTable.alias
      this.selectP = this.form.primaryTable.alias
      this.joinColumns = joinInfo
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 200)
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
  beforeCreate () {
    if (!this.$store.state.modals.TableJoinModal) {
      vuex.registerModule(['modals', 'TableJoinModal'], store)
    }
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
