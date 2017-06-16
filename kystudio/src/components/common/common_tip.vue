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
    name: 'common_tip',
    data () {
      return {
        visible: true
      }
    },
    props: ['tips', 'content', 'trigger', 'placement']
  }
</script>
