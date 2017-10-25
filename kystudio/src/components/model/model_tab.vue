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
  import cubeView from 'components/cube/cube_list'
  import cubeMetadata from 'components/cube/cube_metadata'
  import cubeManage from 'components/cube/manage'
  import cubeSegment from 'components/cube/segmentDetail'
  import { sampleGuid } from '../../util/index'
  export default {
    data () {
      return {
        editable: true,
        editableTabs: [{
          title: this.$t('kylinLang.common.overview'),
          name: 'Overview',
          content: 'modelSubMenu',
          closable: false,
          guid: sampleGuid()
        }],
        extraoption: {},
        currentView: 'modelList',
        activeName: 'Overview',
        tabIndex: 1,
        editTabNameList: ['cube', 'cubes', 'model']
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
            guid: sampleGuid(),
            icon: tabType.indexOf('model') !== -1 ? 'cube' : 'cubes'
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
      'tab': tab,
      'modelSubMenu': modelSubMenu,
      'cubeEdit': cubeEdit,
      'cubeMetadata': cubeMetadata,
      'cubeView': cubeView,
      'cubeManage': cubeManage,
      'cubeSegment': cubeSegment
    },
    mounted () {
    },
    locales: {
      'en': {'willGo': 'You have unsaved information detected, do you want to continue?', 'go': 'Continue', willClose: 'You have unsaved information detected, do you want to continue?', close: 'Continue'},
      'zh-cn': {'willGo': '检测到您有未保存的信息，是否继续跳转？', 'go': '跳转', willClose: '检测到您有未保存的信息，是否继续关闭？', close: '关闭'}
    }
  }
</script>
<style lang="less" scope="">
@import '../../less/config.less';
.modeltab{
  .is-closable{
    background: @grey-color;
  }
  .is-closable:first-child{
    margin-left: 30px;
  }
  .el-tabs__header{
    border-bottom: 1px solid @grey-color;
  }
  .el-tabs__nav-scroll{
    border-bottom: 1px solid @grey-color;
  }
  &>.el-tabs--card>.el-tabs__header{
    &>.el-tabs__nav-wrap{
      &>.el-tabs__nav-scroll{
        &.el-tabs__nav div:nth-child(1){
          &.is-active{
           background-color: @base-color;
           border: 0;
          }
          background-color: #475568
        }
      }
    }
  }
  &>.el-tabs--card>.el-tabs__header>.el-tabs__nav-wrap>.el-tabs__nav-scroll>.el-tabs__nav>.el-tabs__item{
     transition: all .3s cubic-bezier(.645,.045,.355,1);
     border:none;
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
#modeltab{
  .el-tabs__content{
    .el-tabs__nav-scroll{
      margin-left: 30px;
    }
  }
  #cube-view{
    .el-tabs__nav-scroll{
      margin-left: 0;
    }
  }
}
// #modeltab .el-tabs__nav:nth-child(1){
//   margin-left: 30px;
// }
</style>
