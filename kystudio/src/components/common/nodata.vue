<template>
	<div>
	   {{tips}}
	</div>
</template>
<script>
export default {
  name: 'noData',
  props: ['content'],
  data () {
    return {
      tips: this.content || 'no data'
    }
  }
}
</script>
<style scoped="">
div{
 text-align: center;
 color:#ccc;
}
</style>
