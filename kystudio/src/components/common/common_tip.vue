<template>
	<span class="tip_box">
		<el-tooltip :content="tips" :placement="placement||'top'" :disabled="visible" :open-delay="tooltipDelayTime">
      <div slot="content" v-if="!tips"><slot name="content"></slot></div>
		  <span class="icon"><slot></slot></span>
		</el-tooltip>
	</span>
</template>
<script>
  import { tooltipDelayTime } from '../../config/index'
  export default {
    name: 'common_tip',
    data () {
      return {
        tooltipDelayTime: tooltipDelayTime,
        visible: this.disabled
      }
    },
    props: ['tips', 'trigger', 'placement', 'disabled'],
    watch: {
      disabled (val) {
        this.visible = val
      }
    }
  }
</script>
<style lang="less">
.tip_box{
  .icon{
    font-size: 12px;
  }
} 
</style>
