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
      tips: this.content || this.$t('kylinLang.common.noData')
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
