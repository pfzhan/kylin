<template>
<div>
<el-steps :active="activeStep"  finish-status="finish" process-status="wait" center align-center>
    <el-step title="Cube Info" @click.native="step(1)">
    </el-step>
    <el-step title="Dimensions" @click.native="step(2)"></el-step>
    <el-step title="Measures" @click.native="step(3)"></el-step>
    <el-step title="Refresh Setting" @click.native="step(4)"></el-step>
    <el-step title="Advanced Setting" @click.native="step(5)"></el-step>
    <el-step title="Configuration Overwrites" @click.native="step(6)"></el-step>
    <el-step title="Overview" @click.native="step(7)"></el-step>
  </el-steps>
  <info v-if="activeStep==1"></info>
</div>
</template>

<script>
import info from './info'
export default {
  name: 'cubedesc',
  props: ['model'],
  data () {
    return {
      activeStep: 1,
      selected_project: localStorage.getItem('selected_project')
    }
  },
  components: {
    'info': info
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
