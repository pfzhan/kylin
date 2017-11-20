<template>
<div >
  <el-tabs v-model="activeName" :type="type||'card'" :editable="editable" @tab-click="handleClick" @edit="handleTabsEdit">
    <slot name="defaultPane"></slot>
    <el-tab-pane
      v-for="(item, index) in tabs" :key="index"
      :label="item.i18n? item.title.replace(item.i18n,$t('kylinLang.common.'+item.i18n)): item.title"
      :name="item.name"
      v-show="!item.disabled"
      :closable="item.closable"> 
      <span slot="label" v-show="!item.disabled">
        <icon :name="item.icon" :spin="item.spin" scale="0.8" :class="item.icon"></icon> {{ item.i18n? item.title.replace(item.i18n,$t('kylinLang.common.'+item.i18n)): item.title }}
      </span>
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
        editable: this.isedit,
        activeName: this.active
      }
    },
    computed: {
      tabs () {
        return this.tabslist
      }
    },
    watch: {
      active () {
        this.activeName = this.active
      }
    },
    methods: {
      handleClick (tab, event) {
        this.$emit('clicktab', tab.name)
      },
      handleTabsEdit (targetName, action) {
        if (action === 'remove') {
          this.$emit('removetab', targetName, '', '_close')
        }
      }
    },
    created () {
    }

  }
</script>
<style lang="less">

</style>
