<template>
<div >
  <el-tabs v-model="activeName" :type="type||'card'" :editable="editable" @tab-click="handleClick" @edit="handleTabsEdit">
    <el-tab-pane
      v-for="(item, index) in tabs" :key="index"
      :label="item.title"
      :name="item.name"
      v-show="!item.disabled"
      :closable="item.closable"> 
      <span slot="label" v-show="!item.disabled"><icon :name="item.icon" :spin="item.spin" scale="0.8"></icon> {{item.title}}</span>
      <slot :item = "item" v-show="!item.disabled"></slot>
    </el-tab-pane>
  </el-tabs>
  </div>
</template>
<script>
  export default {
    props: ['tabslist', 'isedit', 'active', 'type'],
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
        if (action === 'remove') {
          this.$emit('removetab', targetName)
        }
      }
    },
    created () {
    }

  }
</script>
<style lang="less">

</style>
