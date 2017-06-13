<template>
	<span>
		<el-tooltip :content="tips" :placement="placement||'top'">
     <div slot="content" v-html="content" v-if="content"></div>
		  <span><slot></slot></span>
		</el-tooltip>
	</span>
</template>
<script>
  export default {
    name: 'confirm-btn',
    data () {
      return {
        visible: false
      }
    },
    props: ['tips', 'content', 'trigger', 'placement']
  }
</script>
