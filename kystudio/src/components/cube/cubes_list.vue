<template>
  <el-table
    :data="cubesList"
    style="width: 100%">
    <el-table-column type="expand">
      <template scope="props">
         <el-tabs v-model="activeName" type="card" >
           <el-tab-pane label="Grid" name="first">
                <cube_desc :cube="props.row"></cube_desc>
           </el-tab-pane>
           <el-tab-pane label="JSON" name="second">
            <show_JSON :model="props.row"></show_JSON>
         </el-tab-pane>
        </el-tabs>
      </template>
    </el-table-column>
    <el-table-column
      label="Name"
      prop="name">
    </el-table-column>
    <el-table-column
      label="Project">
        <template scope="scope">
        <span style="margin-left: 10px">{{ selected_project }}</span>
      </template>
    </el-table-column>
    <el-table-column
      label="Owner"
      prop="owner">
    </el-table-column>
    <el-table-column
      label="Fact Table"
      prop="fact_table">
    </el-table-column>
    <el-table-column
      label="Last Modified Time"
      prop="last_modified">
    </el-table-column>   
        <el-table-column
      label="Action">
      <template scope="scope">
      <el-dropdown trigger="click">
      <el-button class="el-dropdown-link">
        <i class="el-icon-more"></i>
      </el-button >
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item>Edit</el-dropdown-item>
        <el-dropdown-item>Clone</el-dropdown-item>
        <el-dropdown-item>Drop</el-dropdown-item>
      </el-dropdown-menu>
      </el-dropdown>
      </template>      
    </el-table-column>     
  </el-table>
</template>

<script>
import { mapActions } from 'vuex'
import showJSON from '../common/show_JSON'
import cubeDesc from './cube_desc'
export default {
  name: 'cubeslist',
  methods: {
    ...mapActions({
      loadCubes: 'LOAD_CUBES_LIST'
    })
  },
  data () {
    return {
      activeName: 'first',
      selected_project: localStorage.getItem('selected_project')
    }
  },
  computed: {
    cubesList () {
      return this.$store.state.cube.cubesList
    }
  },
  components: {
    'show_JSON': showJSON,
    'cube_desc': cubeDesc
  },
  created () {
    this.loadCubes()
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
