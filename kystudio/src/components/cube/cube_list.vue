<template>
  <el-table
    :data="cubesList"
    style="width: 100%">
    <el-table-column type="expand">
      <template scope="props">
        <el-tabs activeName="first" type="card" >
          <el-tab-pane label="Grid" name="first">
            <cube_desc :cube="props.row" :index="props.$index"></cube_desc>
          </el-tab-pane>
          <el-tab-pane label="SQL" name="second">
          </el-tab-pane>
          <el-tab-pane label="JSON" name="third">
            <show_json :json="props.row"></show_json>
          </el-tab-pane>
          <el-tab-pane :label="$t('storage')" name="fourth">
              Storage
          </el-tab-pane>          
        </el-tabs>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('name')"
      prop="name">
    </el-table-column>
    <el-table-column
      :label="$t('model')"
      prop="model">
    </el-table-column>
    <el-table-column
      :label="$t('status')"
      prop="status">
      <template scope="scope">
        <el-tag  :type="scope.row.status === 'DISABLED' ? 'danger' : 'success'">{{scope.row.status}}</el-tag>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('cubeSize')">
      <template scope="scope">
        <span>{{scope.row.size_kb*1024}}KB</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('sourceRecords')"
      prop="input_records_count">
    </el-table-column>
    <el-table-column
      :label="$t('lastBuildTime')"
      prop="last_build_time">
    </el-table-column>  
    <el-table-column
      :label="$t('owner')"
      prop="owner">
    </el-table-column>  
    <el-table-column
      :label="$t('createTime')"
      prop="create_time_utc">
    </el-table-column>   
    <el-table-column
      :label="$t('actions')">
      <template scope="scope">
        <el-dropdown trigger="click">
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item>{{$t('drop')}}</el-dropdown-item>
            <el-dropdown-item>{{$t('edit')}}</el-dropdown-item>
            <el-dropdown-item>{{$t('build')}}</el-dropdown-item>
            <el-dropdown-item>{{$t('merge')}}</el-dropdown-item>
            <el-dropdown-item>{{$t('enable')}}</el-dropdown-item>
            <el-dropdown-item>{{$t('purge')}}</el-dropdown-item>
            <el-dropdown-item>{{$t('clone')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column>    
    <el-table-column
      label="Admin">
      <template scope="scope">
        <el-dropdown trigger="click">
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item>{{$t('editCubeDesc')}}</el-dropdown-item>
            <el-dropdown-item>{{$t('viewCube')}}</el-dropdown-item>
            <el-dropdown-item>{{$t('backupCube')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column>   
    <el-table-column
      label="Streaming">
    </el-table-column>          
  </el-table>
</template>

<script>
import { mapActions } from 'vuex'
import showJSON from '../common/show_json'
import cubeDesc from './cube_desc'
export default {
  name: 'cubeslist',
  methods: {
    ...mapActions({
      loadCubesList: 'LOAD_CUBES_LIST'
    })
  },
  data () {
    return {
      selected_project: this.$store.state.project.selected_project
    }
  },
  computed: {
    cubesList () {
      return this.$store.state.cube.cubesList
    }
  },
  watch: {
    selected_project (val) {
      this.loadCubesList()
    }
  },
  components: {
    'show_json': showJSON,
    'cube_desc': cubeDesc
  },
  created () {
    this.loadCubesList()
  },
  locales: {
    'en': {name: 'Name', model: 'Model', status: 'Status', cubeSize: 'Cube Size', sourceRecords: 'Source Records', lastBuildTime: 'Last Build Time', owner: 'Owner', createTime: 'Create Time', actions: 'Action', drop: 'Drop', edit: 'Edit', build: 'Build', merge: 'Merge', enable: 'Enable', purge: 'Purge', clone: 'Clone', disabled: 'Disabled', editCubeDesc: 'Edit CubeDesc', viewCube: 'View Cube', backupCube: 'Backup Cube', storage: 'Storage'},
    'zh-cn': {name: '名称', model: '模型', status: '状态', cubeSize: 'Cube大小', sourceRecords: '源数据条目', lastBuildTime: '最后构建时间', owner: '所有者', createTime: '创建时间', actions: '操作', drop: '删除', edit: '编辑', build: '构建', merge: '合并', enable: '启用', purge: '清理', clone: '克隆', disabled: '禁用', editCubeDesc: '编辑 Cube详细信息', viewCube: '查看 Cube', backupCube: '备份cube', storage: '存储'}
  }
}
</script>
<style scoped="">
  .demo-table-expand {
    font-size: 0;
  }
  .demo-table-expand label {
    width: 90px;
    color: #99a9bf;
  }
  .demo-table-expand .el-form-item {
    margin-right: 0;
    margin-bottom: 0;
    width: 50%;
  }
</style>
