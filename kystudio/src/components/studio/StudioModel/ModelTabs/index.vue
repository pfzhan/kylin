<template>
  <div class="mode-edit-tabs ksd-mt-20">
    <kap-tab class="studio-top-tab" type="card" v-on:addtab="addTab" v-on:reload="reloadTab" v-on:removetab="delTab" :tabslist="modelEditPanels"  :active="activeName" v-on:clicktab="checkTab">
    </kap-tab>
    <component :is="p.content" v-for="p in modelEditPanels" :key="p.name" v-on:saveRequestEnd="requestEnd" v-on:addtabs="addTab" v-on:reload="reloadTab" v-on:removetabs="delTab" :extraoption="p.extraoption" :ref="p.content" v-if="p.name === activeName"></component>
    <div class="footer">
      <div class="btn-group">
        <el-button @click="goModelList" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" v-guide.saveModelBtn type="primary" icon="el-icon-ksd-table_save" @click="saveModel" :loading="saveBtnLoading">{{$t('kylinLang.common.save')}}</el-button>
      </div>
    </div>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations } from 'vuex'
// import { sampleGuid } from '../../../../config'
import locales from './locales'
import { sampleGuid, cacheSessionStorage } from 'util/index'
import ModelEdit from '../ModelEdit/index.vue'
import ElementUI from 'kyligence-ui'
let MessageBox = ElementUI.MessageBox
@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      // 在添加模型页面刷新，跳转到列表页面
      if (to.name === 'ModelEdit' && to.params.action === 'add' && from.name === null) {
        vm.$router.replace({name: 'ModelList', params: { ignoreIntercept: true }})
      }
    })
    next()
  },
  beforeRouteLeave (to, from, next) {
    if (!to.params.ignoreIntercept) {
      next(false)
      setTimeout(() => {
        MessageBox.confirm(window.kapVm.$t('kylinLang.common.willGo'), window.kapVm.$t('kylinLang.common.notice'), {
          confirmButtonText: window.kapVm.$t('kylinLang.common.exit'),
          cancelButtonText: window.kapVm.$t('kylinLang.common.cancel'),
          type: 'warning'
        }).then(() => {
          if (to.name === 'refresh') { // 刷新逻辑下要手动重定向
            next()
            this.$nextTick(() => {
              this.$router.replace('studio/model')
            })
            return
          }
          next()
        }).catch(() => {
          if (to.name === 'refresh') { // 取消刷新逻辑，所有上一个project相关的要撤回
            let preProject = cacheSessionStorage('preProjectName') // 恢复上一次的project
            this.setProject(preProject)
            this.getUserAccess({project: preProject})
          }
          next(false)
        })
      })
    } else {
      next()
    }
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'modelsPagerRenderData'
    ])
  },
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST',
      delModel: 'DELETE_MODEL',
      checkModelName: 'CHECK_MODELNAME',
      getUserAccess: 'USER_ACCESS'
    }),
    ...mapMutations({
      toggleFullScreen: 'TOGGLE_SCREEN',
      setProject: 'SET_PROJECT'
    })
  },
  components: {
    ModelEdit
  },
  locales
})
export default class ModelTabs extends Vue {
  activeName = ''
  modelEditPanels = []
  saveBtnLoading = false
  get currentModel () {
    return this.$route.params.modelName
  }
  get currentAction () {
    return this.$route.params.action
  }
  get modelDesc () {
    return this.$route.params.modelDesc
  }
  addTab () {}
  reloadTab () {}
  delTab () {
    this.goModelList()
  }
  requestEnd () {
    this.saveBtnLoading = false
  }
  checkTab (name) {
    this.toggleFullScreen(false)// 关闭全屏模式
    this.activeName = name
  }
  goModelList () {
    this.toggleFullScreen(false)
    this.$router.push({name: 'ModelList'})
  }
  saveModel () {
    this.saveBtnLoading = true
    this.$refs[this.modelEditPanels[0].content][0].$emit('saveModel', null)
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
        modelDesc: this.modelDesc,
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
    .el-tabs__item.is-active {
      border:solid 1px @line-split-color;
      border-bottom:none;
      background: #fff;
    }
    .el-tabs__nav-wrap {
      margin-bottom: 0;
    }
  }
  height: calc(~'100% - 54px');
  .footer {
    height:60px;
    background:transparent;
    position: absolute;
    bottom: 0;
    right:0;
    .btn-group {
      float: right;
      margin-top: 14px;
      margin-right: 15px;
    }
  }
}
</style>
