<template>
  <el-dialog class="models-export-modal"
    width="480px"
    :title="$t('exportModel')"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="handleClose"
    @closed="handleClosed">
    <div v-if="isBodyShow" v-loading="isLoading || isSubmiting">
      <div class="header clearfix">
        <div class="ksd-fleft">
          <span class="title">{{$t('chooseModels')}}</span>
        </div>
        <div class="ksd-fright">
          <el-input class="filter" size="small" :placeholder="$t('placeholder')" @input="handleFilter" />
        </div>
      </div>
      <el-tree
        highlight-current
        check-strictly
        class="model-tree"
        ref="tree"
        node-key="id"
        v-show="!isTreeEmpty"
        :data="models"
        :props="{ children: 'children', label: 'name' }"
        :show-checkbox="getIsNodeShowCheckbox"
        :render-content="renderContent"
        :filter-node-method="handleFilterNode"
        @check="handleSelectModels"
      />
      <div class="model-tree" v-show="isTreeEmpty">
        <div class="no-data">{{$t('kylinLang.common.noData')}}</div>
      </div>
    </div>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button plain size="medium" :disabled="isSubmiting" @click="handleCancel">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button size="medium" :disabled="!form.ids.length" @click="handleSubmit" :loading="isSubmiting">{{$t('export')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions } from 'vuex'
import { downloadFileByXMLHttp } from '../../../util/business'
import vuex, { actionTypes } from '../../../store'
import { apiUrl } from '../../../config'
import locales from './locales'
import store from './store'
import OverflowTextTooltip from '../OverflowTextTooltip/OverflowTextTooltip.vue'

vuex.registerModule(['modals', 'ModelsExportModal'], store)

@Component({
  components: {
    OverflowTextTooltip
  },
  computed: {
    ...mapState('ModelsExportModal', {
      project: state => state.project,
      models: state => state.models,
      form: state => state.form,
      isShow: state => state.isShow,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapMutations('ModelsExportModal', {
      setModalForm: actionTypes.SET_MODAL_FORM,
      hideModal: actionTypes.HIDE_MODAL,
      initModal: actionTypes.INIT_MODAL
    }),
    ...mapActions('ModelsExportModal', {
      getModelsMetadataStructure: actionTypes.GET_MODELS_METADATA_STRUCTURE
    }),
    ...mapActions({
      downloadModelsMetadata: actionTypes.DOWNLOAD_MODELS_METADATA,
      downloadModelsMetadataBlob: actionTypes.DOWNLOAD_MODELS_METADATA_BLOB
    })
  },
  locales
})
export default class ModelsExportModal extends Vue {
  isLoading = false
  isBodyShow = false
  isSubmiting = false
  isTreeEmpty = false

  getIsNodeShowCheckbox (data) {
    return data.nodeType === 'model'
  }

  async handleOpen () {
    try {
      const { project } = this
      this.isLoading = true
      await this.getModelsMetadataStructure({ project })
      this.isBodyShow = true
      this.isLoading = false
    } catch (e) {
      this.handleClose()
      this.$message.error(this.$t('fetchModelsFailed'))
    }
  }

  handleClose (isSubmit = false) {
    this.hideModal()
    this.callback && this.callback(isSubmit)
  }

  handleClosed () {
    this.isBodyShow = false
  }

  handleSelectModels (data, { checkedKeys }) {
    this.setModalForm({ ids: checkedKeys })
  }

  handleCancel () {
    this.handleClose()
  }

  handleFilter (value) {
    if (!this.$refs.tree) return
    this.$refs.tree.filter(value)
    setTimeout(() => {
      const allNodes = this.$refs.tree.getAllNodes()
      this.isTreeEmpty = !allNodes.some(node => node.visible)
    })
  }

  handleFilterNode (inputValue, data) {
    if (!inputValue) return true

    const value = inputValue.toLowerCase()

    return data.search.some(search => search.toLowerCase().includes(value))
  }

  async handleSubmit () {
    const { project, form } = this
    this.isSubmiting = true
    if (this.$store.state.config.platform === 'iframe') {
      // this.downloadResouceData(project, form)
      let apiUrlStr = apiUrl + `metastore/backup/models?project=${project}`
      downloadFileByXMLHttp(apiUrlStr, {form}, 'POST', 'application/x-www-form-urlencoded').then(() => {
        this.isSubmiting = false
        this.handleClose(true)
        this.$message.success(this.$t('exportSuccess'))
      })
    } else {
      try {
        await this.downloadModelsMetadata({ project, form })
        this.handleClose(true)
        this.$message.success(this.$t('exportSuccess'))
      } catch (e) {
        this.$message.error(this.$t('exportFailed'))
      }
      this.isSubmiting = false
    }
  }
  downloadResouceData (project, form) {
    const params = {}
    for (const [key, value] of Object.entries(form)) {
      if (value instanceof Array) {
        value.forEach((item, index) => {
          params[`${key}[${index}]`] = item
        })
      } else if (typeof value === 'object') {
        params[key] = JSON.stringify(value)
      } else {
        params[key] = value
      }
    }
    try {
      this.downloadModelsMetadataBlob({project, params}).then(res => {
        this.isSubmiting = false
        this.handleClose(true)
        let str = res && res.headers.map['content-disposition'][0]
        let fileName1 = str.split('filename=')[1]
        let fileName = fileName1.includes('"') ? JSON.parse(fileName1) : fileName1
        if (res && res.body) {
          let data = res.body
          const blob = new Blob([data], {type: 'application/json;charset=utf-8'})
          if (window.navigator.msSaveOrOpenBlob) {
            navigator.msSaveBlob(data, fileName)
          } else {
            let link = document.createElement('a')
            link.href = window.URL.createObjectURL(blob)
            link.download = fileName
            link.click()
            window.URL.revokeObjectURL(link.href)
          }
        }
        this.$message.success(this.$t('exportSuccess'))
      })
    } catch (e) {
      this.isSubmiting = false
      this.handleClose(true)
      this.$message.error(this.$t('exportFailed'))
    }
  }

  renderNodeIcon (h, { node, data }) {
    switch (data.nodeType) {
      case 'table': return data.type === 'FACT'
        ? <i class="tree-icon el-icon-ksd-fact_table" />
        : <i class="tree-icon el-icon-ksd-lookup_table" />
      case 'model':
      default: return null
    }
  }

  renderNodeText (h, { node, data }) {
    switch (data.nodeType) {
      case 'model': return <span v-custom-tooltip={{ text: node.label, w: 50 }}>{node.label}</span>
      case 'table': return <span v-custom-tooltip={{ text: node.label, w: 80 }}>{node.label}</span>
      default: return null
    }
  }

  renderContent (h, { node, data }) {
    return (
      <span class={['tree-item', data.nodeType]}>
        {this.renderNodeIcon(h, { node, data })}
        {this.renderNodeText(h, { node, data })}
      </span>
    )
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.models-export-modal {
  .filter {
    width: 200px;
  }
  .header {
    margin-bottom: 10px;
  }
  .title {
    line-height: 24px;
    font-weight: bold;
    color: #1A1A1A;
  }
  .model-tree {
    border: 1px solid @line-border-color;
    height: 316px;
    overflow-x: hidden;
    overflow-y: auto;
    position: relative;
    .table {
      margin-left: 5px;
    }
  }
  .no-data {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    color: @text-disabled-color;
  }
  .tree-icon {
    margin-right: 5px;
  }
  // .tree-item.model {
  //   display: inline-block;
  //   width: calc(~'100% - 24px - 22px');
  // }
  // .tree-item.table {
  //   display: inline-block;
  //   width: calc(~'100% - 18px - 24px - 5px');
  // }
  // .model-tree > .el-tree-node > .el-tree-node__children > .el-tree-node > .el-tree-node__content,
  // .model-tree > .el-tree-node > .el-tree-node__children > .el-tree-node > .el-tree-node__content:hover {
  //   background-color: unset;
  //   color: inherit;
  //   cursor: unset;
  // }
  .tree-item.table {
    display: flex;
    align-items: center;
    width: 100%;
  }
  .tree-item {
    width: 100%;
    .custom-tooltip-layout {
      display: block;
      .el-tooltip {
        display: block;
      }
    }
  }
}
</style>
