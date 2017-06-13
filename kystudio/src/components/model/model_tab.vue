<template>
  <div class="modeltab">
    <tab id="modeltab" class="modeltab ksd-common-tab" v-on:addtab="addTab" v-on:reload="reloadTab" :isedit="editable" v-on:removetab="delTab"   :tabslist="editableTabs"  :active="activeName" v-on:clicktab="checkTab">
       <template scope="props">
        <component :is="props.item.content" v-on:addtabs="addTab" v-on:reload="reloadTab" v-on:removetabs="delTab" :extraoption="props.item.extraoption" :ref="props.item.content"></component>
       </template>
    </tab>
  </div>
</template>
<script>
  import tab from '../common/tab'
  import modelList from '../model/model_list'
  import modelSubMenu from '../model/model_sub_menu'
  import modelEdit from '../model/model_edit'
  import cubeEdit from 'components/cube/edit/cube_desc_edit'
  import cubeView from 'components/cube/cube_single_show'
  import cubeMetadata from 'components/cube/cube_metadata'
  import { sampleGuid } from '../../util/index'
  export default {
    data () {
      return {
        editable: true,
        editableTabs: [{
          title: 'Overview',
          name: 'Overview',
          content: 'modelSubMenu',
          closable: false,
          guid: sampleGuid()
        }],
        extraoption: {},
        currentView: 'modelList',
        activeName: 'Overview',
        tabIndex: 1
      }
    },
    beforeRouteLeave (to, from, next) {
    // 导航离开该组件的对应路由时调用
    // 可以访问组件实例 `this`
      var hasEditTab = false
      this.editableTabs.forEach((tab) => {
        if (['cube', 'cubes', 'model'].indexOf(tab.tabType) !== -1) {
          hasEditTab = true
        }
      })
      if (hasEditTab) {
        this.$confirm(this.$t('willGo'), this.$t('kylinLang.common.tip'), {
          confirmButtonText: this.$t('go'),
          cancelButtonText: this.$t('kylinLang.common.cancel'),
          type: 'warning'
        }).then(() => {
          next()
        }).catch(() => {
          next(false)
        })
      } else {
        next()
      }
    },
    methods: {
      addTab (tabType, title, componentName, extraData) {
        console.log(arguments)
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
            guid: sampleGuid(),
            icon: tabType.indexOf('model') !== -1 ? 'cube' : 'cubes'
          })
        }
        this.activeName = tabType + title
        console.log(this.activeName)
      },
      reloadTab (moduleName) {
        this.$refs.modelSubMenu.reload(moduleName)
      },
      delTab (targetName, stayTabName) {
        let tabs = this.editableTabs
        let activeName = this.activeName
        let activeView = this.currentView
        if (targetName === 'OverView') {
          return
        }
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
      'tab': tab,
      'modelSubMenu': modelSubMenu,
      'cubeEdit': cubeEdit,
      'cubeMetadata': cubeMetadata,
      'cubeView': cubeView
    },
    mounted () {
    },
    locales: {
      'en': {'willGo': 'You have unsaved information detected, Do you want to continue?', 'go': 'Continue go'},
      'zh-cn': {'willGo': '检测到您有未保存的信息，是否继续跳转？', 'go': '继续跳转'}
    }
  }
</script>
<style lang="less" scope="">
@import '../../less/config.less';
.modeltab{
  .el-tabs__header{
    border-bottom: 1px solid @grey-color;
  }
  .el-tabs__nav-scroll{
    border-bottom: 1px solid @grey-color;
  }
  .el-tabs--card>.el-tabs__header{
    .el-tabs__nav div:nth-child(1){
      &.is-active{
       background-color: @base-color;
       border: 0;
      }
      background-color: #475568
    }
  }
  .el-tabs__nav div:nth-child(1) .el-icon-close{
    visibility: hidden;
    display: none;
  }
  .el-tabs__nav{
    margin-left: 0;
  }
} 
.el-tabs:not(.el-tabs--card) .el-tabs__header{
  border-top: 0;
}
.is-closable{
  margin-left: 30px;
}
#modeltab .el-tabs__content .el-tabs__nav-scroll{
  margin-left: 30px;
}
</style>
