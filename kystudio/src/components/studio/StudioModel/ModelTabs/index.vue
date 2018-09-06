<template>
  <div class="mode-edit-tabs">
    <kap-tab class="studio-top-tab" v-on:addtab="addTab" v-on:reload="reloadTab" v-on:removetab="delTab" :tabslist="modelEditPanels"  :active="activeName" v-on:clicktab="checkTab">
    </kap-tab>
    <component :is="p.content" v-for="p in modelEditPanels" :key="p.name" v-on:addtabs="addTab" v-on:reload="reloadTab" v-on:removetabs="delTab" :extraoption="p.extraoption" :ref="p.content" v-if="p.name === activeName"></component>
    <div class="footer">
      <div class="btn-group">
        <el-button @click="goModelList">Cancel</el-button>
          <el-button type="primary" @click="openSaveDialog">Save</el-button>
        </div>
    </div>
    <PartitionModal/>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations } from 'vuex'
// import { sampleGuid } from '../../../../config'
import locales from './locales'
import { sampleGuid } from 'util/index'
import ModelEdit from '../ModelEdit/index.vue'
import PartitionModal from '../ModelPartitionModal/index.vue'

@Component({
  // props: {
  //   modelName: {
  //     type: String,
  //     default: ''
  //   }
  // },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'modelsPagerRenderData'
    ])
  },
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST',
      cloneModel: 'CLONE_MODEL',
      delModel: 'DELETE_MODEL',
      checkModelName: 'CHECK_MODELNAME'
    }),
    ...mapActions('ModelPartitionModal', {
      showPartitionDialog: 'CALL_MODAL'
    }),
    ...mapMutations({
      toggleFullScreen: 'TOGGLE_SCREEN'
    })
  },
  components: {
    ModelEdit,
    PartitionModal
  },
  locales
})
export default class ModelTabs extends Vue {
  activeName = ''
  modelEditPanels = []

  get currentModel () {
    return this.$route.params.modelName
  }
  get currentAction () {
    return this.$route.params.action
  }

  addTab () {}
  reloadTab () {}
  delTab () {
    this.goModelList()
  }
  checkTab (name) {
    this.toggleFullScreen(false)// 关闭全屏模式
    this.activeName = name
  }
  goModelList () {
    this.toggleFullScreen(false)
    this.$router.push({name: 'ModelList'})
  }
  openSaveDialog () {
    this.showPartitionDialog()
  }
  mounted () {
    this.activeName = this.currentModel
    this.modelEditPanels.push({
      title: this.currentModel,
      name: this.currentModel,
      content: 'ModelEdit',
      guid: sampleGuid(),
      closable: true,
      extraoption: {
        project: this.currentSelectedProject,
        modelName: this.currentModel,
        action: this.currentAction
      }
    })
    // this.loadUsers(this.currentGroup)
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.mode-edit-tabs {
  .studio-top-tab {
    background-color:@breadcrumbs-bg-color;
    .el-tabs__item.is-active {
      border:solid 1px @line-split-color;
      border-bottom:none;
      background: #fff;
    }
    &>.el-tabs--card {
      &>.el-tabs__header{
        border-bottom:solid 1px @line-split-color;
      }
    }
    .el-tabs__nav-wrap {
      margin-bottom: 0;
    }
  }
  height: 100%;
  .footer {
    height:60px;
    background:#fff;
    position: absolute;
    bottom: 0;
    width:100%;
    .btn-group {
      float: right;
      margin-top: 14px;
      margin-right: 15px;
    }
  }
}
</style>
