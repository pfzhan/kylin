<template>
	<div class="sub_menu">
    <el-tabs v-model="subMenu" @tab-click="handleClick" class="el-tabs--default">
      <el-tab-pane :label="$t('kylinLang.common.dataSource')" name="datasource">
        <div v-if="subMenu === 'datasource'">
          <component is="dataSource" v-on:addtabs="addTab" ref="datsources" ></component>
        </div>
      </el-tab-pane>
      <el-tab-pane :label="$t('kylinLang.common.model')" name="model">
        <div v-if="subMenu === 'model'">
          <component is="modelList" v-on:addtabs="addTab" ref="models" ></component>
        </div>
      </el-tab-pane>
      <el-tab-pane :label="$t('kylinLang.common.cube')" name="cube" >
        <div v-if="subMenu === 'cube'">
          <component is="cubeList"  v-on:addtabs="addTab" ref="cubes" ></component>
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>
<script>
import dataSource from '../datasource/data_source'
import modelList from '../model/model_list'
import cubeList from '../cube/cube_list'
export default {
  data () {
    return {
      subMenu: 'datasource'
    }
  },
  components: {
    'dataSource': dataSource,
    'modelList': modelList,
    'cubeList': cubeList
  },
  methods: {
    addTab (a, b, c, d) {
      this.$emit('addtabs', a, b, c, d)
    },
    removeTab (a) {
      this.$emit('removetabs', a)
    },
    handleClick (a) {
      this.$router.push(this.subMenu)
    },
    // 刷新子组件
    reload (moduleName) {
      if (moduleName === 'modelList') {
        this.$refs.models.reloadModelList()
      } else if (moduleName === 'cubeList') {
        this.$refs.cubes.reloadCubeList()
      }
    }
  },
  mounted () {
    var hash = location.hash
    var subRouter = hash.replace(/.*\/(.*)$/, '$1')
    if (subRouter === 'model' || subRouter === 'cube') {
      this.subMenu = subRouter
    }
  }
}
</script>
<style lang="less">
  .sub_menu {
    .el-tabs__nav{
      margin-left: 0px;
    }
  }

</style>
