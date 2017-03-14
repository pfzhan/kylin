<template>
  <div>
 <!--  <div style="margin-bottom: 20px;">
    <el-button
      size="small"
      @click="addTab(editableTabsValue2)"
    >
      add tab
    </el-button>
  </div> -->
  <el-tabs v-model="editableTabsValue2" type="card" closable @tab-remove="removeTab">
    <el-tab-pane
      v-for="(item, index) in editableTabs2"
      :label="item.title"
      :name="item.name"
    >
     
    </el-tab-pane>
    <div>
      <component :is="currentView" keep-alive></component>
    </div>
  </el-tabs>
</div>
</template>
<script>
  import modelList from '../model/model_list'
  export default {
    data () {
      return {
        editableTabsValue2: '1',
        editableTabs2: [{
          title: 'OverView',
          name: '1',
          content: '<component :is="currentView" keep-alive></component>'
        }],
        currentView: 'model_ist',
        tabIndex: 1
      }
    },
    methods: {
      addTab (targetName) {
        let newTabName = ++this.tabIndex + ''
        this.editableTabs2.push({
          title: 'New Tab',
          name: newTabName,
          content: ''
        })
        this.editableTabsValue2 = newTabName
      },
      removeTab (targetName) {
        let tabs = this.editableTabs2
        let activeName = this.editableTabsValue2
        if (activeName === targetName) {
          tabs.forEach((tab, index) => {
            if (tab.name === targetName) {
              let nextTab = tabs[index + 1] || tabs[index - 1]
              if (nextTab) {
                activeName = nextTab.name
              }
            }
          })
        }
  
        this.editableTabsValue2 = activeName
        this.editableTabs2 = tabs.filter(tab => tab.name !== targetName)
      }
    },
    components: {
      'model_ist': modelList
    }
  }
</script>
<style >
  .el-tabs__item.is-active{
    background-color: #475568;
    color:#fff;
  }
</style>
