<template>
  <div>
    <kap-tab class="studio-top-tab" v-on:addtab="addTab" v-on:reload="reloadTab" :isedit="editable" v-on:removetab="delTab"   :tabslist="editableTabs"  :active="activeName" v-on:clicktab="checkTab">
       <template slot-scope="props">
        <component :is="props.item.content" v-on:addtabs="addTab" v-on:reload="reloadTab" v-on:removetabs="delTab" :extraoption="props.item.extraoption" :ref="props.item.content"></component>
       </template>
    </kap-tab>
  </div>
</template>
<script>
  // import tab from '../common/tab'
  import modelList from '../model/model_list'
  import modelSubMenu from '../model/model_sub_menu'
  import modelEdit from '../model/model_edit'
  import cubeView from 'components/cube/cube_list'
  import cubeEdit from 'components/cube/edit/cube_desc_edit'
  import cubeManage from 'components/cube/manage'
  import { sampleGuid } from '../../util/index'
  export default {
    data () {
      return {
        editable: true,
        editableTabs: [{
          title: 'overview',
          name: 'Overview',
          content: 'modelSubMenu',
          closable: false,
          guid: sampleGuid(),
          i18n: 'overview'
        }],
        extraoption: {},
        currentView: 'modelList',
        activeName: 'Overview',
        tabIndex: 1,
        editTabNameList: ['cube', 'cubes', 'model']
      }
    },
    watch: {
      'activeName' () {
        this.$store.state.model.activeMenuName = this.activeName
      }
    },
    beforeRouteLeave (to, from, next) {
    // 导航离开该组件的对应路由时调用
    // 可以访问组件实例 `this`
      if (this.hasEditTab()) {
        this.$confirm(this.$t('willGo'), this.$t('kylinLang.common.tip'), {
          confirmButtonText: this.$t('go'),
          cancelButtonText: this.$t('kylinLang.common.cancel'),
          type: 'warning'
        }).then(() => {
          if (to.name === 'refresh') {
            next()
            this.$nextTick(() => {
              this.$router.replace('studio/model')
            })
            return
          }
          next()
        }).catch(() => {
          next(false)
        })
      } else {
        next()
      }
    },
    methods: {
      hasEditTab () {
        var hasEditTab = false
        this.editableTabs.forEach((tab) => {
          if (this.editTabNameList.indexOf(tab.tabType) !== -1) {
            hasEditTab = true
          }
        })
        return hasEditTab
      },
      addTab (tabType, title, componentName, extraData) {
        let tabs = this.editableTabs
        let hasTab = false
        tabs.forEach((tab, index) => {
          if (tab.name === tabType + title && index !== 0) {
            hasTab = true
          }
        })
        if (!hasTab) {
          this.tabIndex = this.tabIndex + 1
          this.editableTabs.push({
            title: title,
            tabType: tabType,
            name: tabType + title,
            content: componentName,
            extraoption: extraData,
            i18n: extraData.i18n,
            guid: sampleGuid(),
            icon: tabType.indexOf('model') !== -1 ? 'el-icon-ksd-model' : 'el-icon-ksd-cube'
          })
        }
        this.activeName = tabType + title
      },
      reloadTab (moduleName) {
        this.$refs.modelSubMenu.reload(moduleName)
      },
      delTab (targetName, stayTabName, closeCheck) {
        if (targetName === 'OverView') {
          return
        }
        if (closeCheck === '_close') {
          if (!this.hasEditTab()) {
            this.removeTab(targetName, stayTabName)
          } else {
            this.$confirm(this.$t('willClose'), this.$t('kylinLang.common.tip'), {
              confirmButtonText: this.$t('close'),
              cancelButtonText: this.$t('kylinLang.common.cancel'),
              type: 'warning'
            }).then(() => {
              this.removeTab(targetName, stayTabName)
            })
          }
        } else {
          this.removeTab(targetName, stayTabName)
        }
      },
      removeTab (targetName, stayTabName) {
        let tabs = this.editableTabs
        let activeName = this.activeName
        let activeView = this.currentView
        if (activeName === targetName) {
          tabs.forEach((tab, index) => {
            if (stayTabName) {
              if (tab.name === stayTabName) {
                activeName = tabs[index].name
                activeView = tabs[index].content
              }
            } else if (tab.name === targetName) {
              let nextTab = tabs[index + 1] || tabs[index - 1]
              if (nextTab) {
                activeName = nextTab.name
                activeView = nextTab.content
              }
            }
          })
        }
        this.activeName = activeName
        this.currentView = activeView
        this.editableTabs = tabs.filter(tab => tab.name !== targetName)
      },
      checkTab (name) {
        this.activeName = name
      }
    },
    components: {
      'modelList': modelList,
      'modelEdit': modelEdit,
      'modelSubMenu': modelSubMenu,
      'cubeView': cubeView,
      'cubeEdit': cubeEdit,
      'cubeManage': cubeManage
    },
    mounted () {
      this.$store.state.model.activeMenuName = 'Overview'
    },
    locales: {
      'en': {'willGo': 'You have unsaved information detected, do you want to continue?', 'go': 'Continue', willClose: 'You have unsaved information detected, do you want to continue?', close: 'Continue'},
      'zh-cn': {'willGo': '检测到您有未保存的信息，是否继续跳转？', 'go': '跳转', willClose: '检测到您有未保存的信息，是否继续关闭？', close: '关闭'}
    }
  }
</script>
<style lang="less" scope="">
</style>
