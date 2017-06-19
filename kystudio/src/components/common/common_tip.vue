<template>
	<span>
		<el-tooltip :content="tips" :placement="placement||'top'">
     <div slot="content" v-html="content" v-if="content"></div>
		  <span class="icon"><slot></slot></span>
		</el-tooltip>
	</span>
</template>
<script>
  export default {
    name: 'common_tip',
    data () {
      return {
        visible: true
      }
    },
    props: ['tips', 'content', 'trigger', 'placement']
  }
</script>
<style scoped>
  .icon{
    font-size: 12px;
    vertical-align: sub;
  }
</style>
