<template>
  <el-button class="icon_button" @click="clickBtn" :type="type" :size="size" :loading="loading" :plain="plain" :disabled="disabled"><i :class="icon" v-if="!loading && icon"></i>
    <slot></slot>
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
<style lang="less">
.icon_button {
  svg{
    vertical-align: middle;
  }
}  
</style>
