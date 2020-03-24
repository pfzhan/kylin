<template>
  <el-dialog class="models-import-modal"
    width="480px"
    :title="$t('importModelsMetadata')"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="handleClose"
    @closed="handleClosed">
    <!-- 上传元数据zip包界面 -->
    <el-form ref="form" :model="form" :rules="rules" v-loading="isSubmiting" v-if="isBodyShow && !isFinishParse">
      <el-form-item class="file-upload" prop="file" :label="$t('selectFile')">
        <el-input :value="form.file && form.file.name" :placeholder="$t('placeholder')" />
        <input class="file-input" type="file" accept="application/zip" @change="handleSelectFile" />
      </el-form-item>
    </el-form>
    <!-- 解析zip元数据包界面 -->
    <div v-loading="isSubmiting" v-else-if="isBodyShow && isFinishParse">
      <div class="header clearfix">
        <span class="title">
          {{$t('chooseModels', { count: models.length })}}
        </span>
      </div>
      <el-collapse class="model-type-list" v-model="activePannels">
        <!-- 无冲突可导入模型列表 -->
        <el-collapse-item name="NO_CONFLICT_MODELS" class="no-conflict-models">
          <div slot="title" class="model-type-header">
            <el-checkbox
              :indeterminate="form.ids.length > 0 && form.ids.length < unConflictModels.length"
              :value="isSelectAllUnConflictModels"
              :disabled="!unConflictModels.length"
              @input="handleSelectModels(isSelectAllUnConflictModels ? [] : noConflictModelIds, noConflictModelIds)"
            />
            <span>{{$t('unConflictModels')}}</span>
            <span>({{form.ids.length}} / {{unConflictModels.length}})</span>
          </div>
          <el-checkbox-group v-if="unConflictModels.length" class="model-list" :value="form.ids" @input="ids => handleSelectModels(ids, noConflictModelIds)">
            <el-checkbox
              class="model_item"
              v-for="noConflictModel in unConflictModels"
              :key="noConflictModel.id"
              :label="noConflictModel.id">
              {{noConflictModel.name}}
            </el-checkbox>
          </el-checkbox-group>
          <div class="empty-text" v-else>{{$t('unConflictModelsIsEmpty')}}</div>
        </el-collapse-item>
        <!-- 有冲突不可导入模型列表 -->
        <el-collapse-item name="CONFLICT_MODELS" class="conflict-models">
          <div slot="title" class="model-type-header">
            {{$t('conflictModels', { count: conflictModels.length })}}
          </div>
          <el-collapse v-if="conflictModels.length">
            <el-collapse-item
              v-for="conflictModel of conflictModels"
              :key="conflictModel.id"
              :name="conflictModel.id">
              <div slot="title" class="model-name">
                <el-tooltip :content="getModelIconMessage(conflictModel.conflicts)" effect="dark" placement="top" >
                  <i :class="getModelIcon(conflictModel.conflicts)" />
                </el-tooltip>
                {{conflictModel.name}}
              </div>
              <RenderModelConflicts :conflicts="conflictModel.conflicts" />
            </el-collapse-item>
          </el-collapse>
          <div class="empty-text" v-else>{{$t('conflictModelsIsEmpty')}}</div>
        </el-collapse-item>
      </el-collapse>
    </div>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <!-- 确认/取消: 上传元数据zip包界面 -->
      <template v-if="!isFinishParse">
        <el-button plain size="medium" :disabled="isSubmiting" @click="handleCancel">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" :disabled="!form.file" :loading="isSubmiting" @click="handleUploadFile">{{$t('parseFile')}}</el-button>
      </template>
      <!-- 确认/取消: 解析zip元数据包界面 -->
      <template v-else>
        <el-button plain size="medium" :disabled="isSubmiting" @click="handlePrev">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button size="medium" :disabled="!form.ids.length" :loading="isSubmiting" @click="handleSubmit">{{$t('kylinLang.common.submit')}}</el-button>
      </template>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'

import store from './store'
import locales from './locales'
import { conflictTypes } from './handler'
import vuex, { actionTypes } from '../../../store'
import RenderModelConflicts from './RenderModelConflicts'

vuex.registerModule(['modals', 'ModelsImportModal'], store)

@Component({
  components: {
    RenderModelConflicts
  },
  computed: {
    ...mapState('ModelsImportModal', {
      project: state => state.project,
      models: state => state.models,
      conflicts: state => state.conflicts,
      form: state => state.form,
      isShow: state => state.isShow,
      callback: state => state.callback
    }),
    ...mapGetters('ModelsImportModal', [
      'conflictModels',
      'unConflictModels',
      'brokenConflictModels',
      'importableConflictModels'
    ])
  },
  methods: {
    ...mapMutations('ModelsImportModal', {
      setModalForm: actionTypes.SET_MODAL_FORM,
      hideModal: actionTypes.HIDE_MODAL,
      initModal: actionTypes.INIT_MODAL
    }),
    ...mapActions('ModelsImportModal', {
      uploadMetadataFile: actionTypes.UPLOAD_MODEL_METADATA_FILE,
      importModelsMetadata: actionTypes.IMPORT_MODEL_METADATA_FILE
    })
  },
  locales
})
export default class ModelsImportModal extends Vue {
  isBodyShow = false
  isSubmiting = false
  isFinishParse = false
  activePannels = ['NO_CONFLICT_MODELS']

  get rules () {
    return {
      file: [{ trigger: 'blur', required: true, message: this.$t('pleaseSelectFile') }]
    }
  }

  get isSelectAllUnConflictModels () {
    return this.unConflictModels.length && this.unConflictModels.every(model => this.form.ids.includes(model.id))
  }

  get noConflictModelIds () {
    return this.unConflictModels.map(m => m.id)
  }

  getIsNodeShowCheckbox (data) {
    return data.nodeType === 'model'
  }

  getModelIcon (conflictGroups) {
    const isDiffModel = !conflictGroups.some(group => group.type === conflictTypes.DUPLICATE_MODEL_NAME)
    return isDiffModel ? 'el-icon-ksd-diff_metadata' : 'el-icon-ksd-model_repetition'
  }

  getModelIconMessage (conflictGroups) {
    const isDiffModel = !conflictGroups.some(group => group.type === conflictTypes.DUPLICATE_MODEL_NAME)
    return isDiffModel ? this.$t('METADATA_CONFLICT') : this.$t('DUPLICATE_MODEL_NAME')
  }

  resetState () {
    this.isBodyShow = false
    this.isSubmiting = false
    this.isFinishParse = false
  }

  async handleOpen () {
    this.isBodyShow = true
  }

  handleClose (isSubmit = false) {
    this.hideModal()
    this.resetState()
    this.callback && this.callback(isSubmit)
  }

  handleClosed () {
    this.isBodyShow = false
  }

  handleSelectFile (event) {
    const [file] = event.target.files
    this.setModalForm({ file })
  }

  handleSelectModels (valueIds = [], removeIds = []) {
    const clearIds = this.form.ids.filter(id => !removeIds.includes(id))
    this.setModalForm({ ids: [...clearIds, ...valueIds] })
  }

  handleSelectUnconflictModels () {
    const { unConflictModels } = this
    const modelIds = unConflictModels.map(model => model.id)
    this.handleSelectModels(modelIds)
  }

  handleCancel () {
    this.handleClose()
  }

  handlePrev () {
    this.isFinishParse = false
  }

  handleUploadFile () {
    this.isSubmiting = true

    this.$refs.form.validate(async isValid => {
      if (isValid) {
        try {
          const { project, form } = this
          await this.uploadMetadataFile({ project, form })
          this.isFinishParse = true
          this.handleSelectUnconflictModels()
        } catch (e) {}
      }

      this.isSubmiting = false
    })
  }

  async handleSubmit () {
    if (this.form.ids.length < 0) {
      return this.$message.error('pleaseSelectModels')
    }

    try {
      const { project, form } = this
      this.isSubmiting = true

      await this.importModelsMetadata({ project, form })
      this.handleClose(true)
      this.$message.success(this.$t('submitSuccess'))
    } catch (e) {}
    this.isSubmiting = false
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.models-import-modal {
  .el-dialog__body {
    max-height: 463px;
    overflow: auto;
  }
  .file-upload {
    position: relative;
  }
  .file-input {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    left: 0;
    opacity: 0;
    width: 100%;
    cursor: pointer;
  }
  .header {
    margin-bottom: 10px;
  }
  .model-type-list {
    border: none;
    & > .el-collapse-item {
      border: 1px solid @line-split-color;
      &:not(:last-child) {
        margin-bottom: 10px;
      }
    }
    & > .el-collapse-item > div > .el-collapse-item__header {
      height: 32px;
      line-height: 32px;
      border: none;
    }
    & > .el-collapse-item > div > .el-collapse-item__header > .el-collapse-item__arrow {
      line-height: 32px;
    }
    & .el-collapse-item__wrap {
      border: none;
    }
  }
  .el-collapse-item__content {
    padding: 0;
  }
  .model-list {
    border-top: 1px solid @line-split-color;
  }
  .model-type-header {
    padding: 0 10px;
    background-color: @background-disabled-color;
  }
  .model-list .model_item {
    display: block;
    padding: 0 10px;
    margin-left: 0;
    font-weight: 500;
    .el-checkbox__label {
      color: @text-normal-color;
    }
    &:not(:last-child) {
      border-bottom: 1px solid @background-disabled-color;
    }
  }
  .model-name {
    font-weight: 500;
    i {
      font-size: 12px;
    }
  }
  .el-collapse-item__header {
    font-size: 14px;
  }
  .conflict-models {
    .el-collapse {
      border: none;
      & > .el-collapse-item:not(:last-child):after {
        content: ' ';
        height: 1px;
        background: @background-disabled-color;
        display: block;
        margin: 0 10px;
      }
      & > .el-collapse-item .el-collapse-item__header {
        padding: 0 0 0 10px;
      }
      & > .el-collapse-item > div > .el-collapse-item__header {
        height: 25px;
        line-height: 25px;
        border: none;
      }
      & > .el-collapse-item > div > .el-collapse-item__header > .el-collapse-item__arrow {
        line-height: 25px;
      }
    }
  }
  .model-conflicts {
    font-size: 12px;
    padding: 0 10px;
    .message {
      margin: 0 0 6px 0;
      line-height: 12px;
    }
  }
  .conflict-item {
    margin-bottom: 10px;
  }
  .conflict-title {
    line-height: 14px;
    margin-bottom: 5px;
  }
  .empty-text {
    padding: 10px 0;
    text-align: center;
    color: @text-disabled-color;
  }
  .conflict-box textarea {
    background-color: @base-background-color-1;
  }
}
</style>
