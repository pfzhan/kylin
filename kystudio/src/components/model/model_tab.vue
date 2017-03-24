<template>
 <!--  <div style="margin-bottom: 20px;">
    <el-button
      size="small"
      @click="addTab(editableTabsValue2)"
    >
      add tab
    </el-button>
  </div> -->
  <!-- <el-tabs v-model="editableTabsValue2"  closable @tab-remove="removeTab">
    <el-tab-pane
      v-for="(item, index) in editableTabs2"
      :label="item.title"
      :name="item.name"
    >
     
    </el-tab-pane>
    <div>
      <component :is="currentView" keep-alive></component>
    </div>
  </el-tabs> -->
  <div>
    <tab :isedit="editable"  :tabslist="editableTabs"  :active="activeName" v-on:clicktab="checkTab" v-on:removetab="delTab"></tab>
    <div id="tagBox">
        <component :is="currentView" v-on:addtabs="addTab" keep-alive></component>
    </div>
  </div>
</template>
<script>
  import tab from '../common/tab'
  import modelList from '../model/model_list'
  import modelEdit from '../model/model_edit'
  export default {
    data () {
      return {
        editable: true,
        editableTabs: [{
          title: 'OverView',
          name: 'OverView',
          content: 'modelList',
          closable: false
        }],
        currentView: 'modelList',
        activeName: 'OverView',
        tabIndex: 1
      }
    },
    methods: {
      addTab (targetName, componentName) {
        let tabs = this.editableTabs
        let hasTab = false
        console.log(tabs)
        tabs.forEach((tab, index) => {
          if (tab.name === targetName) {
            this.activeName = targetName
            this.currentView = ''
            this.currentView = tab.content
            hasTab = true
          }
        })
        if (!hasTab) {
          this.tabIndex = this.tabIndex + 1
          this.editableTabs.push({
            title: targetName,
            name: targetName,
            content: componentName
          })
          this.currentView = ''
          this.currentView = componentName
          this.activeName = targetName
        }
      },
      delTab (targetName) {
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
        // this.editableTabs = Object.assign([], this.editableTabs)
      },
      checkTab (name) {
        let tabs = this.editableTabs
        tabs.forEach((tab, index) => {
          if (tab.name === name) {
            this.currentView = ''
            this.currentView = tab.content
            this.activeName = name
          }
        })
      }
    },
    components: {
      'modelList': modelList,
      'modelEdit': modelEdit,
      'tab': tab
    },
    mounted () {
      console.log(this)
    }

  }
</script>
<style >
  .el-tabs__item.is-active{
    background-color: #475568;
    color:#fff;
  }
  .el-tabs__nav div:nth-child(2) .el-icon-close{
    visibility: hidden;
  }
  #tagBox{
    width: 100%;
    height: 100%;
  }
</style>
