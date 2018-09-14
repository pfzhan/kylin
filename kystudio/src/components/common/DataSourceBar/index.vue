<template>
  <aside class="data-source-bar">
    <section class="header clearfix" v-if="isShowActionGroup">
      <div class="header-text font-medium">
        {{$t('kylinLang.common.dataSource')}}
      </div>
      <div class="header-icons">
        <i class="el-icon-ksd-add_data_source" v-if="isShowLoadSource" @click="loadDataSource(sourceTypes.NEW, currentProjectData)"></i>
        <i class="el-icon-ksd-table_setting" v-if="isShowSettings" @click="loadDataSource(sourceTypes.SETTING, currentProjectData)"></i>
      </div>
    </section>

    <section class="body">
      <div v-if="isShowBtnLoad" class="btn-group">
        <el-button plain size="medium" type="primary" icon="el-icon-ksd-load" @click="loadDataSource(sourceTypes.NEW, currentProjectData)">
          {{$t('kylinLang.common.dataSource')}}
        </el-button>
      </div>
      <TreeList
        :data="datasourceTree"
        :placeholder="$t('searchTable')"
        :default-expanded-keys="defaultExpandedKeys"
        :draggable-node-keys="draggableNodeKeys"
        :searchable-node-keys="searchableNodeKeys"
        :is-expand-all="isExpandAll"
        :is-show-filter="isShowFilter"
        :is-expand-on-click-node="isExpandOnClickNode"
        @click="handleClick"
        @drag="handleDrag">
      </TreeList>
    </section>
  </aside>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import { sourceTypes } from '../../../config'
import TreeList from '../TreeList/index.vue'
import locales from './locales'
import { getDatasourceTree, getAutoCompleteWords } from './handler'

@Component({
  props: {
    datasource: {
      type: Array,
      default: () => []
    },
    expandNodeTypes: {
      type: Array,
      default: () => []
    },
    searchableNodeTypes: {
      type: Array,
      default: () => []
    },
    draggableNodeTypes: {
      type: Array,
      default: () => []
    },
    isShowActionGroup: {
      type: Boolean,
      default: true
    },
    isShowLoadSource: {
      type: Boolean,
      default: false
    },
    isShowSettings: {
      type: Boolean,
      default: false
    },
    isExpandOnClickNode: {
      type: Boolean,
      default: true
    },
    isShowFilter: {
      type: Boolean,
      default: true
    },
    isExpandAll: {
      type: Boolean,
      default: false
    }
  },
  components: {
    TreeList
  },
  computed: {
    ...mapGetters([
      'isAdminRole',
      'isProjectAdmin',
      'currentProjectData'
    ])
  },
  methods: {
    ...mapActions('DataSourceModal', {
      callDataSourceModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class DataSourceBar extends Vue {
  sourceTypes = sourceTypes
  autoCompleteWords = []
  defaultExpandedKeys = []
  draggableNodeKeys = []
  searchableNodeKeys = []

  get foreignKeyArray () {
    let foreignKeyArray = []
    this.datasource.forEach((table) => {
      foreignKeyArray = foreignKeyArray.concat(table.foreign_key)
    })
    return foreignKeyArray
  }
  get primaryKeyArray () {
    let primaryKeyArray = []
    this.datasource.forEach((table) => {
      primaryKeyArray = primaryKeyArray.concat(table.primary_key)
    })
    return primaryKeyArray
  }
  get datasourceTree () {
    const { datasource, currentProjectData } = this
    return getDatasourceTree(this, datasource, currentProjectData)
  }
  get isShowBtnLoad () {
    return (this.isAdminRole || this.isProjectAdmin) && !this.datasourceTree.length
  }

  @Watch('datasourceTree')
  onDatasourceTreeChange (datasourceTree) {
    const autoCompleteWords = getAutoCompleteWords(datasourceTree)
    this.defaultExpandedKeys = autoCompleteWords
      .filter(word => this.expandNodeTypes.includes(word.meta))
      .map(word => `${word.meta}-${word.caption}`)
    this.draggableNodeKeys = autoCompleteWords
      .filter(word => this.draggableNodeTypes.includes(word.meta))
      .map(word => `${word.meta}-${word.caption}`)
    this.searchableNodeKeys = autoCompleteWords
      .filter(word => this.searchableNodeTypes.includes(word.meta))
      .map(word => `${word.meta}-${word.caption}`)
    this.$emit('autoComplete', autoCompleteWords)
  }

  handleClick (data, node) {
    this.$emit('click', data, node)
  }
  handleDrag (node, data) {
    this.$emit('drag', data, node)
  }
  async loadDataSource (sourceType, project, event) {
    event && event.stopPropagation()
    event && event.preventDefault()

    const isSubmit = await this.callDataSourceModal({ sourceType, project })

    if (isSubmit) {
      this.$emit('source-update')
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.data-source-bar {
  height: 100%;
  .header,
  .body {
    padding: 20px;
    width: 250px;
    box-sizing: border-box;
  }
  .header {
    font-size: 16px;
    color: #263238;
    border-bottom: 1px solid @line-split-color;
  }
  .header-text {
    float: left;
  }
  .header-icons {
    float: right;
    position: relative;
    transform: translateY(4px);
    i {
      margin-right: 4px;
    }
    i:last-child {
      margin-right: 0;
    }
  }
  .body {
    height: calc(~"100% - 63px");
    overflow: auto;
  }
  .body .btn-group {
    text-align: center;
  }
  // datasource tree样式
  .el-tree {
    margin-bottom: 20px;
    *[draggable="true"] {
      cursor: move;
    }
    .left {
      float: left;
      margin-right: 4px;
    }
    .right {
      position: absolute;
      right: 10px;
      top: 50%;
      transform: translateY(-50%);
    }
    .tree-icon {
      margin-right: 4px;
      &:last-child {
        margin-right: 0;
      }
    }
    .tree-item {
      position: relative;
      width: calc(~'100% - 24px');
      .table {
        padding-right: 30px;
      }
      .column {
        padding-right: 10px;
      }
      > div {
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
      }
    }
    .datasource {
      color: #263238;
    }
    .el-tree-node .el-tree-node__content:hover > .tree-item {
      color: #087AC8;
    }
    .table-date-tip {
      color: #8E9FA8;
      &:hover {
        color: #087AC8;
      }
    }
    .table-action {
      color: #000000;
      &:hover {
        color: #087AC8;
      }
    }
    .table-tag {
      display: inline-block;
      width: 14px;
      height: 14px;
      line-height: 14px;
      margin-right: 2px;
      font-style: normal;
    }
    .column-tag {
      display: inline-block;
      font-size: 16px;
      color: #087AC8;
      margin-right: 2px;
      font-style: normal;
    }
    & > .el-tree-node {
      border: 1px solid #CFD8DC;
      overflow: hidden;
      margin-bottom: 10px;
      &[aria-expanded="true"] > .el-tree-node__content {
        border-bottom: 1px solid #CFD8DC;
      }
      & > .el-tree-node__content {
        padding: 10px 9px !important; // important用来去掉el-tree的内联样式
        height: auto;
        background: #E2ECF1;
        &:hover > .tree-item > span {
          color: #263238;
        }
      }
      // datasource的样式
      & > .el-tree-node__content {
        cursor: default;
        & > .tree-item {
          width: 100%;
          i {
            cursor: pointer;
          }
          .right {
            right: 0;
          }
        }
      }
      & > .el-tree-node__content .el-tree-node__expand-icon {
        display: none;
      }
      & > .el-tree-node__children {
        margin-left: -18px;
      }
      & > .el-tree-node__children > .el-tree-node {
        border-bottom: 1px solid #CFD8DC;
        &:last-child {
          border-bottom: 0;
        }
      }
    }
  }
}
</style>
