<template>
  <div class="modeltab">
    <tab class="modeltab ksd-common-tab" v-on:addtab="addTab" v-on:reload="reloadTab" :isedit="editable" v-on:removetab="delTab"   :tabslist="editableTabs"  :active="activeName" v-on:clicktab="checkTab" >
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
        if (['cube', 'cubes'].indexOf(tab.tabType) !== -1) {
          hasEditTab = true
        }
      })
      if (hasEditTab) {
        this.$confirm('将要离开该页面，请保存未完成的编辑?', '提示', {
          confirmButtonText: '继续跳转',
          cancelButtonText: '取消',
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
      delTab (targetName) {
        console.log(targetName)
        let tabs = this.editableTabs
        let activeName = this.activeName
        let activeView = this.currentView
        if (targetName === 'OverView') {
          return
        }
        if (activeName === targetName) {
          tabs.forEach((tab, index) => {
            if (tab.name === targetName) {
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
      console.log(this)
    }
  }
</script>
<style lang="less" scope="">
.modeltab{
  .el-tabs--card>.el-tabs__header{
    .el-tabs__nav div:nth-child(1){
      &.is-active{
       background-color: #475568
      }
      background-color: #475568
    }
  }
  .el-tabs__nav div:nth-child(1) .el-icon-close{
    visibility: hidden;
    display: none;
  }
  .el-tabs__nav{
    margin-left: 26px;
  }
} 
</style>
