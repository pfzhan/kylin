<template>
	<div>
	   {{tips || $t('kylinLang.common.noData') }}
	</div>
</template>
<script>
export default {
  name: 'noData',
  props: ['content'],
  data () {
    return {
      tips: this.content
    }
  }
}
</script>
<style scoped="" lang="less">
@import "../../assets/styles/variables.less";
div{
 text-align: center;
 color:@text-disabled-color;
 position: absolute;
 top: 50%;
 text-align: center;
 width: 100%;
}
</style>
