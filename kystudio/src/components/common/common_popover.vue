<template>
    <div class="kap_popover">
     <el-popover ref="popoverGrant" placement="right" trigger="hover" :open-delay="tooltipDelayTime">
      <div class="grant-popover">
        <slot name="content"></slot>
      </div>
    </el-popover>
    <div v-popover:popoverGrant class="trigger"><slot></slot></div>
    </div>
</template>
<script>
    import { tooltipDelayTime } from '../../config/index'
    export default {
      data () {
        return {
          tooltipDelayTime: tooltipDelayTime
        }
      }
    }
</script>
<style>
.kap_popover {
    display: inline-block;
    vertical-align:middle;
   .trigger {
    display: inline-block;
  } 
} 
</style>
