<template>
  <el-dialog class="data-srouce-modal" :width="modelWidth" v-guide.dataSourceSelectBox
    :title="$t(modalTitle)"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="() => handleClose()"
    @closed="handleClosed">
    <template v-if="isFormShow">
      <SourceSelect
        v-if="editType === editTypes.SELECT_SOURCE"
        :source-type="sourceType"
        @input="handleInputDatasource">
      </SourceSelect>
      <SourceHiveSetting
        ref="source-hive-setting-form"
        v-if="[editTypes.CONFIG_SOURCE, editTypes.VIEW_SOURCE].includes(editType) && [editTypes.HIVE].includes(sourceType)"
        :form="form.settings"
        :edit-type="editType"
        :is-editable="editType !== editTypes.VIEW_SOURCE"
        @input="(key, value) => handleInput(`settings.${key}`, value)">
      </SourceHiveSetting>
      <SourceHive
        v-if="[editTypes.HIVE, editTypes.RDBMS, editTypes.RDBMS2].includes(editType)"
        :source-type="sourceType"
        :selected-tables="form.selectedTables"
        :selected-databases="form.selectedDatabases"
        @input="handleInputTableOrDatabase">
      </SourceHive>
    </template>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="handleCancel" v-if="cancelText">{{cancelText}}</el-button>
      <el-button size="medium" :key="editType" plain type="primary" @click="handleSubmit" v-guide.saveSourceType v-if="confirmText" :loading="isLoading">{{confirmText}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { titleMaps, cancelMaps, confirmMaps, getSubmitData, editTypes } from './handler'
import { handleSuccessAsync, handleError } from '../../../util'
import { set } from '../../../util/object'

import SourceSelect from './SourceSelect/SourceSelect.vue'
import SourceHiveSetting from './SourceHiveSetting/SourceHiveSetting.vue'
import SourceHive from './SourceHive/SourceHive.vue'

vuex.registerModule(['modals', 'DataSourceModal'], store)

@Component({
  components: {
    SourceSelect,
    SourceHiveSetting,
    SourceHive
  },
  computed: {
    ...mapState('DataSourceModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      firstEditType: state => state.firstEditType
    })
  },
  methods: {
    ...mapMutations('DataSourceModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      initForm: types.INIT_FORM,
      setModalForm: types.SET_MODAL_FORM
    }),
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      importTable: 'LOAD_HIVE_IN_PROJECT',
      saveSourceConfig: 'SAVE_SOURCE_CONFIG'
    })
  },
  locales
})
export default class DataSourceModal extends Vue {
  isLoading = false
  isDisabled = false
  isFormShow = false
  editTypes = editTypes
  get modalTitle () { return titleMaps[this.editType] }
  get modelWidth () { return this.editType === editTypes.HIVE ? '960px' : '780px' }
  get confirmText () { return this.$t(confirmMaps[this.editType]) }
  get cancelText () { return ![editTypes.SELECT_SOURCE, editTypes.VIEW_SOURCE].includes(this.firstEditType) ? this.$t('kylinLang.common.cancel') : this.$t(cancelMaps[this.editType]) }
  get sourceType () { return this.form.project.override_kylin_properties['kylin.source.default'] }
  handleInput (key, value) {
    this.setModalForm(set(this.form, key, value))
  }
  handleInputTableOrDatabase (payload) {
    this.setModalForm(payload)
  }
  handleInputDatasource (value) {
    const properties = { ...this.form.project.override_kylin_properties }
    properties['kylin.source.default'] = value
    this.handleInput('project.override_kylin_properties', properties)
  }
  handleOpen () {
    this._showForm()
  }
  handleClose (isSubmit) {
    this._hideLoading()
    this.hideModal()
    this.callback && this.callback(isSubmit)
  }
  handleClosed () {
    this._hideForm()
    this.initForm()
  }
  handleCancel () {
    // for datasource config
    // if (this.firstEditType !== editTypes.SELECT_SOURCE || this.editType === editTypes.SELECT_SOURCE) {
    //   this.handleClose(false)
    // } else if (this.editType === editTypes.CONFIG_SOURCE) {
    //   this.setModal({ editType: editTypes.SELECT_SOURCE })
    // } else {
    //   this.setModal({ editType: editTypes.CONFIG_SOURCE })
    // }
    if (this.firstEditType !== editTypes.SELECT_SOURCE || this.editType === editTypes.SELECT_SOURCE) {
      this.handleClose(false)
    } else {
      this.setModal({ editType: editTypes.SELECT_SOURCE })
    }
  }
  async handleSubmit () {
    this._showLoading()
    try {
      if (await this._validate()) {
        const results = await this._submit()
        if (results) {
          this.handleClose(results)
        }
      }
    } catch (e) {
      handleError(e)
    }
    this._hideLoading()
  }
  _hideForm () {
    this.isFormShow = false
  }
  _showForm () {
    this.isFormShow = true
  }
  _hideLoading () {
    this.isLoading = false
    this.isDisabled = false
  }
  _showLoading () {
    this.isLoading = true
    this.isDisabled = true
  }
  async _submit () {
    const submitData = getSubmitData(this.form, this.editType)
    switch (this.editType) {
      // for datasource config
      // case editTypes.SELECT_SOURCE: {
      //   await this.updateProject(submitData)
      //   return this.setModal({ editType: editTypes.CONFIG_SOURCE })
      // }
      // case editTypes.CONFIG_SOURCE: {
      //   await this.saveSourceConfig(submitData)
      //   return this.setModal({ editType: this.form.project.override_kylin_properties['kylin.source.default'] })
      // }
      case editTypes.SELECT_SOURCE: {
        await this.updateProject(submitData)
        return this.setModal({ editType: this.form.project.override_kylin_properties['kylin.source.default'] })
      }
      case editTypes.VIEW_SOURCE: {
        return this.handleClose(false)
      }
      case editTypes.HIVE:
      case editTypes.RDBMS:
      case editTypes.RDBMS2: {
        const response = await this.importTable(submitData)
        return await handleSuccessAsync(response)
      }
    }
  }
  async _validate () {
    switch (this.editType) {
      case editTypes.SELECT_SOURCE: {
        const isValid = this.form.project.override_kylin_properties['kylin.source.default']
        !isValid && this.$message(this.$t('pleaseSelectSource'))
        return isValid
      }
      case editTypes.CONFIG_SOURCE: {
        return await this.$refs['source-hive-setting-form'].$refs.form.validate()
      }
      case editTypes.HIVE:
      case editTypes.RDBMS:
      case editTypes.RDBMS2: {
        const isValid = this.form.selectedTables.length || this.form.selectedDatabases.length
        !isValid && this.$message(this.$t('pleaseSelectTableOrDatabase'))
        return isValid
      }
      default:
        return true
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.data-srouce-modal {
  .el-dialog__body {
    padding: 0;
  }
  .create-kafka {
    padding: 20px;
  }
}
</style>
