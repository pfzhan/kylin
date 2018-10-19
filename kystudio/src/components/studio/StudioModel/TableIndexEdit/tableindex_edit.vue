<template>
  <!-- tableindex的添加和编辑 -->
  <el-dialog :title="$t('Edit Table Index')" class="table-edit-dialog" width="660px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
      <el-form :model="modelClone" :rules="rules" ref="cloneForm" >
        <el-form-item :label="$t('Table Index Name')" prop="newName">
          <el-input v-model="modelClone.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
      </el-form>
      <div class="ky-line"></div>
      <h3 class="ksd-mt-20">Table Index Content:</h3>
      <div class="ksd-mt-20">
        <el-steps direction="vertical" :active="1">
          <el-step title="Select Columns">
            <div slot="description">
              <div class="ksd-mt-14 ksd-mb-16">
                <div class="actions"><el-button>Select All Columns</el-button> <el-button>Clear All</el-button> </div>
                <ul class="table-index-columns">
                  <li>
                    <span class="sort-icon ksd-mr-10">1</span>
                    <el-input style="width:430px" size="medium"></el-input>
                    <el-button circle palin icon="el-icon-plus" size="small"  @click="addSortbyCol" :disabled="lockRawTable"></el-button>
                    <el-button circle size="small" icon="el-icon-minus" @click="delSortbyCol(index)" :disabled="lockRawTable"></el-button> 
                  </li>
                  <li>
                    <span class="sort-icon ksd-mr-10">2</span>
                    <el-input style="width:430px" size="medium"></el-input>
                    <el-button circle palin icon="el-icon-plus" size="small"  @click="addSortbyCol" :disabled="lockRawTable"></el-button>
                    <el-button circle size="small" icon="el-icon-minus" @click="delSortbyCol(index)" :disabled="lockRawTable"></el-button> 
                  </li>
                </ul>
              </div>
            </div>
          </el-step>
          <el-step title="Sort By">
            <div slot="description">
              <div class="ksd-mt-14 ksd-mb-16">
                <ul class="table-index-columns">
                  <li>
                    <span class="sort-icon ksd-mr-10">1</span>
                    <el-input style="width:430px" size="medium"></el-input>
                    <el-button circle palin icon="el-icon-plus" size="small"  @click="addSortbyCol" :disabled="lockRawTable"></el-button>
                    <el-button circle size="small" icon="el-icon-minus" @click="delSortbyCol(index)" :disabled="lockRawTable"></el-button> 
                  </li>
                  <li>
                    <span class="sort-icon ksd-mr-10">2</span>
                    <el-input style="width:430px" size="medium"></el-input>
                    <el-button circle palin icon="el-icon-plus" size="small"  @click="addSortbyCol" :disabled="lockRawTable"></el-button>
                    <el-button circle size="small" icon="el-icon-minus" @click="delSortbyCol(index)" :disabled="lockRawTable"></el-button> 
                  </li>
                </ul>
              </div>
            </div>
          </el-step>
          <el-step title="Shard By" description="这是一段很长很长很长的描述性文字">
            <div slot="description">
              <el-select v-model="value" style="width:100%" placeholder="请选择">
                <el-option
                  v-for="item in options"
                  :key="item.value"
                  :label="item.label"
                  :value="item.value">
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
  import { handleError, kapMessage } from 'util/business'
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
        data: state => state.form.data,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapActions({
        cloneModel: 'CLONE_MODEL'
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
    modelClone = {
      newName: ''
    }
    rules = {
      newName: [
        {validator: this.checkName, trigger: 'blur'}
      ]
    }
    @Watch('modelDesc')
    initModelName () {
      this.modelClone.newName = this.modelDesc.alias + '_clone'
    }
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    }
    closeModal (isSubmit) {
      this.hideModal()
      this.modelClone.newName = ''
      setTimeout(() => {
        this.callback && this.callback(isSubmit)
        this.resetModalForm()
      }, 200)
    }
    async submit () {
      this.$refs.cloneForm.validate((valid) => {
        if (!valid) { return }
        this.btnLoading = true
        this.cloneModel({modelName: this.modelDesc.name, newModelName: this.modelClone.newName, project: this.currentSelectedProject}).then(() => {
          this.btnLoading = false
          kapMessage(this.$t('cloneSuccessful'))
          this.closeModal(true)
        }, (res) => {
          this.btnLoading = false
          res && handleError(res)
        })
      })
    }
  }
</script>
<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .table-edit-dialog {
    .sort-icon {
      .ky-square-box(32px, 32px);
      background: @text-secondary-color;
      display: inline-block;
      color:@fff;
      ertical-align: bottom;
    }
    .table-index-columns {
      li {
        margin-top:20px;
        height:32px;
      }
    }
  } 
</style>
