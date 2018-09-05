<template>
  <div>
    <el-input
    type="textarea"
    :rows="14"
    :readonly="true"
    v-model="stepDetail">
    </el-input>   
  </div>
</template>

<script>
export default {
  name: 'jobDialog',
  props: ['stepDetail']
}
</script>
<style>
</style>
