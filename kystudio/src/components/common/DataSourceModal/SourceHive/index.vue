<template>
  <div class="source-hive">
    <div class="left-tree-part">
      <div class="dialog_tree_box">
        <tree
          @lazyload="loadChildNode"
          :multiple="true"
          @nodeclick="clickHiveTable"
          :lazy="true"
          :treedata="hiveData"
          :emptyText="$t('dialogHiveTreeLoading')"
          maxlevel="3"
          ref="subtree"
          :maxLabelLen="20"
          :showfilter="false"
          :allowdrag="false" >
        </tree>
      </div>
    </div>

    <div class="right-tree-part">
      <div class="tree_check_content ksd-mt-20">
        <arealabel
          :validateRegex="regex"
          @validateFail="$message($t('selectedHiveValidateFailText'))"
          @refreshData="changeHiveData"
          splitChar=","
          :selectedlabels="selectedTableNames"
          :allowcreate="true"
          placeholder=" "
          @removeTag="removeSelectedHive"
          :datamap="{label: 'label', value: 'value'}">
        </arealabel>
        <div class="ksd-mt-22 ksd-extend-tips" v-html="selectedType === '0' ? $t('loadHiveTip') : $t('loadTip')"></div>
        <div class="ksd-mt-20">
          <slider @changeBar="changeBar" class="ksd-mr-20 ksd-mb-20">
            <span slot="checkLabel">{{$t('sampling')}}</span>
              <span slot="tipLabel">
                <common-tip placement="right" :content="$t('kylinLang.dataSource.collectStatice')" >
                  <i class="el-icon-question"></i>
                </common-tip>
              </span>
          </slider>
        </div>
      </div> 
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import arealabel from '../../area_label'
import { objectClone } from '../../../../util'
import { handleError, handleSuccess } from '../../../../util/business'

@Component({
  props: {
    selectedTables: {
      default: () => []
    },
    selectedType: Number
  },
  components: {
    arealabel
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'selectedProjectDatasource'
    ])
  },
  methods: {
    ...mapActions({
      loadDatabase: 'LOAD_HIVEBASIC_DATABASE',
      loadTablesByDatabse: 'LOAD_HIVE_TABLES'
    })
  },
  locales
})
export default class SourceHive extends Vue {
  hiveData = []
  regex = /^\s*;?(\w+\.\w+)\s*(,\s*\w+\.\w+)*;?\s*$/

  get selectedTableNames () {
    return this.selectedTables.map(selectedTable => selectedTable.value)
  }

  mounted () {
    this.$emit('input', {
      selectedTables: [],
      tableStaticsRange: 0,
      openCollectRange: false
    })
  }

  changeHiveData (selectedNames) {
    const newSelectedTableNames = selectedNames.filter(selectedName => {
      return !this.selectedTables.some(selectedTable => selectedTable.value === selectedName)
    })
    const newSelectedTables = newSelectedTableNames.map(name => ({label: name, value: name}))
    const selectedTables = [ ...this.selectedTables, ...newSelectedTables ]

    this.$emit('input', { selectedTables })
  }

  removeSelectedHive (val) {
    const selectedTables = this.selectedTables.filter(selectedTable => selectedTable.id === val)

    this.$refs.subtree.cancelNodeChecked(val)
    this.$emit('input', { selectedTables })
  }

  changeBar (val) {
    this.$emit('input', {
      tableStaticsRange: val,
      openCollectRange: !!val
    })
  }

  loadChildNode (node, resolve) {
    if (node.level === 0) {
      return resolve([{label: this.selectedProjectDatasource === '0' ? 'Hive Table' : 'Table'}])
    } else if (node.level === 1) {
      this.loadDatabase({ projectName: this.currentSelectedProject, sourceType: this.selectedType }).then((res) => {
        handleSuccess(res, (data) => {
          var datasourceTreeData = []
          for (var i = 0; i < data.length; i++) {
            datasourceTreeData.push({id: data[i], label: data[i], children: [], fullData: data})
          }
          resolve(datasourceTreeData)
        })
      }, (res) => {
        node.loading = false
        handleError(res)
      })
    } else if (node.level === 2) {
      var subData = []
      this.loadTablesByDatabse({
        databaseName: node.label,
        projectName: this.currentSelectedProject,
        sourceType: this.selectedType
      }).then((res) => {
        handleSuccess(res, (data) => {
          var len = data && data.length || 0
          var pagerLen = len > this.treePerPage ? this.treePerPage : len
          for (var k = 0; k < pagerLen; k++) {
            subData.push({
              id: node.label + '.' + data[k],
              label: data[k]
            })
          }
          if (pagerLen < len) {
            subData.push({
              id: node.label + '...',
              label: '。。。',
              children: [],
              parentNode: node,
              parentLabel: node.label,
              fullData: data,
              index: this.treePerPage,
              isMore: true
            })
          }
          resolve(subData)
        })
      }, (res) => {
        node.loading = false
        handleError(res)
      })
    } else {
      resolve([])
    }
  }

  clickHiveTable (data, vnode) {
    if (data.id && data.id.indexOf('.') > 0 && !data.isMore) {
      const newArr = this.selectedTables.filter(item => item.value === data.id)
      if (!newArr || newArr.length <= 0) {
        const selectedTables = [ ...this.selectedTables, { label: data.id, value: data.id } ]
        this.$emit('input', { selectedTables })
      }
    }
    var node = data
    if (node.index) {
      // 加载更多
      vnode.store.remove(vnode.data)
      var addData = node.fullData.slice(0, node.index + this.treePerPage)
      var moreNodes = []
      for (var k = 0; k < addData.length; k++) {
        moreNodes.push({
          id: node.parentLabel + '.' + addData[k],
          label: addData[k]
        })
      }
      var renderChildrens = objectClone(moreNodes)
      if (node.index + this.treePerPage < node.fullData.length) {
        renderChildrens.push({
          id: node.parentLabel + '...',
          label: '。。。',
          parentLabel: node.parentLabel,
          fullData: node.fullData,
          children: [],
          index: node.index + this.treePerPage,
          isMore: true
        })
      }
      node.parentNode.children = renderChildrens
    }
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.source-hive {
  display:flex;
  .el-dialog__body {
    padding: 0;
  }
  .left-tree-part{
    width:218px;
    max-height:600px;
    overflow-y:auto;
    overflow-x:hidden;
  }
  .right-tree-part{
    border-left: solid 1px @line-border-color;
    flex:1;
    padding: 0 20px 20px;
  }
}
</style>
