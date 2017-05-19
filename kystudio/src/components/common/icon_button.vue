<template>
  <el-button @click="clickBtn" :type="type" :size="size" :loading="loading" :plain="plain" :disabled="disabled"><icon scale="0.8" :name="icon" v-if="!loading && icon"></icon>
    <span><slot></slot></span>
  </el-button>
</template>
<script>
export default {
  name: 'iconbtn',
  props: ['icon', 'type', 'disabled', 'size', 'plain', 'useload'],
  data () {
    return {
      loading: false
    }
  },
  methods: {
    clickBtn () {
      if (this.useload) {
        this.loading = true
      }
    }
  },
  created () {
  }
}
</script>
<style scoped="">
  span{
    vertical-align:text-top;
  }
</style>
