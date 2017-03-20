<template>
 <!--  <div style="margin-bottom: 20px;">
    <el-button
      size="small"
      @click="addTab(editableTabsValue2)"
    >
      add tab
    </el-button>
  </div> -->
  <el-tabs v-model="activeName"  :editable="editable" @tab-click="handleClick" @edit="handleTabsEdit">
    <el-tab-pane
      v-for="(item, index) in tabs"
      :label="item.title"
      :name="item.name"
      :closable=item.closable

    > 
    </el-tab-pane>
  </el-tabs>
</template>
<script>
  export default {
    props: ['tabslist', 'isedit', 'active'],
    data () {
      return {
        currentView: '',
        editable: this.isedit
      }
    },
    computed: {
      activeName () {
        return this.active
      },
      tabs () {
        return this.tabslist
      }
    },
    methods: {
      handleClick (tab, event) {
        this.$emit('clicktab', tab.name)
      },
      handleTabsEdit (targetName, action) {
        console.log(targetName, action)
        if (action === 'remove') {
          this.$emit('removetab', targetName)
        }
      }
    },
    created () {
    }

  }
</script>
<style >
  .el-tabs__item.is-active{
    background-color: #475568;
    color:#fff;
  }
</style>
