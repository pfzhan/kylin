<template>
<div>
<el-card class="box-card">
  {{json}}
</el-card>
</div>
</template>
<script>
export default {
  name: 'showjson',
  props: ['json']
}
</script>
<style scoped="">
.box-card {
  width: 90%
}
</style>
