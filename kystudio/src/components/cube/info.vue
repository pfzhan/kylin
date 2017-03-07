<template>
<div>
</div>
</template>

<script>
export default {
  name: 'cubedesc',
  props: ['model'],
  data () {
    return {
      activeStep: 1,
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    step: function (num) {
      this.activeStep = num
    }
  }
}
</script>
<style scoped="">

</style>
