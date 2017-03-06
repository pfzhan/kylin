<template>
  <el-table
    :data="modelList"
    style="width: 100%">
    <el-table-column type="expand">
      <template scope="props">
         <el-tabs v-model="activeName" type="card" >
           <el-tab-pane label="Grid" name="first">
             <model_view :model="props.row"></model_view>
           </el-tab-pane>
           <el-tab-pane label="Visualization" name="second">Visualization
         </el-tab-pane>
           <el-tab-pane label="JSON" name="third">
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
import modelView from './model_view'
import modelVisualization from './model_visualization'
import showJSON from '../common/show_JSON'
export default {
  name: 'modellist',
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST'
    })
  },
  data () {
    return {
      activeName: 'first',
      selected_project: localStorage.getItem('selected_project')
    }
  },
  computed: {
    modelList () {
      return this.$store.state.model.modelsList
    }
  },
  components: {
    'model_view': modelView,
    'model_visualization': modelVisualization,
    'show_JSON': showJSON
  },
  created () {
    this.loadModels()
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
  .el-dropdown-link {

  }
</style>
