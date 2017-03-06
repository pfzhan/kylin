<template>
<div>
<el-card class="box-card">
  {{model}}
</el-card>
</div>
</template>
<script>
export default {
  name: 'showJSON',
  props: ['model']
}
</script>
<style scoped="">
.box-card {
  width: 90%
}
</style>
