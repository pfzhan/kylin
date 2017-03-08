<template>
  <div>
    <el-form >
    <el-form-item label="Project Name" >
      <el-input v-model="project.name" auto-complete="off"></el-input>
    </el-form-item>
    <el-form-item label="Description" >
      <el-input v-model="project.description" auto-complete="off"></el-input>
    </el-form-item>
  </el-form>
</div>
</template>
<script>
export default {
  name: 'project_edit',
  props: ['project'],
  methods: {
    step: function (num) {
      this.activeStep = num
    }
  }
}
</script>
<style scoped="">

</style>
