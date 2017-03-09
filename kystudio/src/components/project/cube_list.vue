<template>
  <el-table
    :data="cubeList"
    style="width: 100%">
    </el-table-column>
    <el-table-column
      label="Cube Name"
      prop="realization">
    </el-table-column>
    <el-table-column
      label="Action">
      <template scope="scope">
        detail
      </template>      
    </el-table-column>     
  </el-table>
</template>

<script>
export default {
  name: 'cubelist',
  props: ['cubeList'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
    }
  }
}
</script>
<style scoped="">
</style>
