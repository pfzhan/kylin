e<template>
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
  <info v-if="activeStep===1" :desc="selected_cube"></info>
  <dimensions v-if="activeStep===2" :desc="selected_cube"></dimensions>
  <measures v-if="activeStep===3" :desc="selected_cube"></measures>
  <refresh_setting v-if="activeStep===4" :desc="selected_cube"></refresh_setting>
  <advanced_setting v-if="activeStep===5" :desc="selected_cube"></advanced_setting>
  <configuration_overwrites v-if="activeStep===6" :desc="selected_cube"></configuration_overwrites>
  <overview v-if="activeStep===7" :desc="selected_cube"></overview>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import info from './info_view'
import dimensions from './dimensions_view'
import measures from './measures_view'
import refreshSetting from './refresh_setting_view'
import advancedSetting from './advanced_setting_view'
import configurationOverwrites from './configuration_overwrites_view'
import overview from './overview_view'
export default {
  name: 'cubedesc',
  props: ['cube', 'index'],
  data () {
    return {
      activeStep: 1,
      selected_project: this.$store.state.project.selected_project
    }
  },
  components: {
    'info': info,
    'dimensions': dimensions,
    'measures': measures,
    'refresh_setting': refreshSetting,
    'advanced_setting': advancedSetting,
    'configuration_overwrites': configurationOverwrites,
    'overview': overview
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC'
    }),
    step: function (num) {
      this.activeStep = num
    }
  },
  created () {
    this.loadCubeDesc({name: this.cube.name, index: this.index})
  },
  computed: {
    selected_cube () {
      return this.$store.state.cube.cubesDescList[this.index]
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
