<template>
<div>
<el-input
  v-model="getJSON"
  type="textarea"
  :rows="18"
  :readonly="true">
</el-input>
</div>
</template>
<script>
export default {
  name: 'showjson',
  props: ['json', 'index'],
  computed: {
    getJSON () {
      return JSON.stringify(this.json)
    }
  }
}
</script>
<style scoped="">
.box-card {
  width: 90%
}
</style>
