<template>
	<span>
		<el-tooltip :content="tips" placement="top">
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
    props: ['tips', 'trigger']
  }
</script>
