<template>
<div>
  <el-steps :active="activeStep"  finish-status="finish" process-status="wait" center >
    <el-step :title="$t('cubeInfo')" @click.native="step(1)"></el-step>
    <el-step :title="$t('dimensions')" @click.native="step(2)"></el-step>
    <el-step :title="$t('measures')" @click.native="step(3)"></el-step>
    <el-step :title="$t('refreshSetting')" @click.native="step(4)"></el-step>
    <el-step :title="$t('advancedSetting')" @click.native="step(5)"></el-step>
    <el-step :title="$t('configurationOverwrites')" @click.native="step(6)"></el-step>
    <el-step :title="$t('overview')" @click.native="step(7)"></el-step>
  </el-steps>
  <info_view v-if="activeStep===1" :info="cube"></info_view>
</div>
</template>

<script>
import infoView from './info_view'
export default {
  name: 'cubedesc',
  props: ['cube'],
  data () {
    return {
      activeStep: 1,
      selected_project: localStorage.getItem('selected_project')
    }
  },
  components: {
    'info_view': infoView
  },
  methods: {
    step: function (num) {
      this.activeStep = num
    }
  },
  locales: {
    'en': {cubeInfo: 'Cube Info', dimensions: 'Dimensions', measures: 'Measures', refreshSetting: 'Refresh Setting', advancedSetting: 'Advanced Setting', configurationOverwrites: 'Configuration Overwrites', overview: 'Overview'},
    'zh-cn': {cubeInfo: 'Cube信息', dimensions: '维度', measures: '度量', refreshSetting: '更新配置', advancedSetting: '高级设置', configurationOverwrites: '配置覆盖', overview: '概览'}
  }
}
</script>
<style scoped="">

</style>
