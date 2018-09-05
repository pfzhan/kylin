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
 color:#d4d7e3;
/* background-color: #292b38;
 font-size: 12px;
*/
}
</style>
