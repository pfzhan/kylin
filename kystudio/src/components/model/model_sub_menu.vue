<template>
	<div class="sub_menu">
    <el-tabs v-model="subMenu" @tab-click="handleClick" class="el-tabs--default">
      <el-tab-pane :label="$t('kylinLang.common.dataSource')" name="datasource" v-if="isAdmin || hasPermissionOfProject">
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
import { permissions } from '../../config'
import { hasPermission, hasRole } from '../../util/business'
export default {
  data () {
    return {
      subMenu: 'model',
      hasPermissionOfProject: this.hasSomePermissionOfProject()
      // ,hasGetPermission: false
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
    },
    hasSomePermissionOfProject () {
      return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    }
  },
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  mounted () {
    var hash = location.hash
    var subRouter = hash.replace(/.*\/(.*)$/, '$1')
    this.subMenu = subRouter
  }
  /* watch: {
    '$store.state': {
      handler: function (value, oldValue) {
        if (!this.hasGetPermission) {
          if (value.project && value.user && value.user.currentUser && value.project.selected_project) {
            var projectId = this.getProjectIdByName(value.project.selected_project)
            if (value.project.projectEndAccess[projectId]) {
              this.hasPermissionOfProject = this.hasSomePermissionOfProject()
              this.hasGetPermission = true
            }
          }
        }
      },
      deep: true
    }
  } */
}
</script>
<style lang="less">
  .sub_menu {
    .el-tabs__nav{
      margin-left: 0px;
    }
  }

</style>
