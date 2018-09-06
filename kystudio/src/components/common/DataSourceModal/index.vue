<template>
  <el-dialog class="data-srouce-modal" :width="!isSourceSetting ? '780px' : '440px'"
    :title="$t(modalTitle)"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="isShow && handleClose(false)">
    <template v-if="isFormShow">
      <SourceNew
        v-if="isNewSource"
        :selected-type="form.project.override_kylin_properties['kylin.source.default']"
        @input="selectDataSource">
      </SourceNew>
      <SourceHive
        v-if="isTableTree"
        :selected-type="sourceType"
        :selected-tables="form.selectedTables"
        @input="handleInput">
      </SourceHive>
      <SourceKafka
        v-if="isKafka"
        ref="kafkaForm"
        class="create-kafka"
        @validSuccess="submit">
      </SourceKafka>
      <SourceSetting
        v-if="isSourceSetting"
        :form="form"
        @input="handleInput">
      </SourceSetting>
    </template>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="handleClose(false)" v-if="cancelText">{{cancelText}}</el-button>
      <el-button size="medium" plain type="primary" @click="handleClick" v-if="confirmText" :disabled="isLoading">{{confirmText}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { sourceTypes } from '../../../config'
import { titleMaps, cancelMaps, confirmMaps, getSubmitData } from './handler'
import { handleSuccessAsync, handleError } from '../../../util'

import SourceNew from './SourceNew/index.vue'
import SourceHive from './SourceHive/index.vue'
import SourceKafka from '../../kafka/create_kafka'
import SourceSetting from './SourceSetting/index.vue'

@Component({
  components: {
    SourceNew,
    SourceHive,
    SourceKafka,
    SourceSetting
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('DataSourceModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      sourceType: state => state.sourceType,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('DataSourceModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      loadHiveInProject: 'LOAD_HIVE_IN_PROJECT',
      saveKafka: 'SAVE_KAFKA',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      saveSampleData: 'SAVE_SAMPLE_DATA'
    })
  },
  locales
})
export default class DataSourceModal extends Vue {
  isLoading = false
  isFormShow = false
  sourceTypes = sourceTypes

  get modalTitle () {
    return titleMaps[this.sourceType]
  }
  get cancelText () {
    return this.$t(cancelMaps[this.sourceType])
  }
  get confirmText () {
    return this.$t(confirmMaps[this.sourceType])
  }
  get isNewSource () {
    return this.sourceType === sourceTypes.NEW
  }
  get isTableTree () {
    return this.sourceType === sourceTypes.HIVE ||
      this.sourceType === sourceTypes.RDBMS ||
      this.sourceType === sourceTypes.RDBMS2
  }
  get isSourceSetting () {
    return this.sourceType === sourceTypes.SETTING
  }
  get isKafka () {
    return this.sourceType === sourceTypes.KAFKA
  }
  isSourceShow (sourceType) {
    return this.sourceType === sourceType
  }

  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true

      if (!this.currentSelectedProject) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        this.handleClose(false)
      }
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 200)
    }
  }

  selectDataSource (value) {
    const project = JSON.parse(JSON.stringify(this.form.project))
    project.override_kylin_properties['kylin.source.default'] = value

    this.setModalForm({ project })
  }

  handleInput (data) {
    this.setModalForm(data)
  }

  handleClose (isSubmit) {
    this.hideModal()

    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 300)
  }

  handleClick () {
    // 是Kafka的走表单验证，不是Kafka的走递交逻辑
    if (this.sourceType !== sourceTypes.KAFKA) {
      this.submit()
    } else {
      this.$refs['kafkaForm'].$emit('kafkaFormValid')
    }
  }

  async submit (kafkaData) {
    this.isLoading = true
    try {
      const { currentSelectedProject: project } = this
      const data = getSubmitData(this, kafkaData)
      // 后台保存：选择数据源
      if (this.isNewSource) {
        await this.updateProject(data)
        this.setModal({ sourceType: this.form.project.override_kylin_properties['kylin.source.default'] })
        // 后台保存：Hive和RDBMS导入
      } else if (this.isTableTree) {
        const res = await this.loadHiveInProject(data)
        const resData = await handleSuccessAsync(res)
        // Todo: data success 提示
        console.log(resData)
        this.loadDataSourceByProject({ project, isExt: true })
        this.handleClose(true)
        // 后台保存：Kafka导入
      } else if (this.isKafka) {
        const tableName = `${data.database}.${data.tableName}`

        await this.saveKafka(data)
        this.$message({ type: 'success', message: this.$t('kylinLang.common.saveSuccess') })
        this.saveSampleData({ tableName, sampleData: data.sampleData, project })
        this.loadDataSourceByProject({ project, isExt: true })
        this.handleClose(true)
      } else if (this.isSourceSetting) {
        this.handleClose(true)
      }
    } catch (e) {
      // 异常处理
      e && handleError(e)
    }
    this.isLoading = false
  }

  beforeCreate () {
    if (!this.$store.state.modals.DataSourceModal) {
      vuex.registerModule(['modals', 'DataSourceModal'], store)
    }
  }
  destroyed () {
    if (!module.hot) {
      vuex.unregisterModule(['modals', 'DataSourceModal'])
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
