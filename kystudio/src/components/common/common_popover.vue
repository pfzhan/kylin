<template>
    <div class="kap_popover">
     <el-popover ref="popoverGrant" placement="right" trigger="hover">
      <div class="grant-popover">
        <slot name="content"></slot>
      </div>
    </el-popover>
    <div v-popover:popoverGrant class="trigger"><slot></slot></div>
    </div>
</template>
<script>
    export default {
      data () {
        return {
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
