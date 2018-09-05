<template>
	<div style="display:inline-block">
		<el-popover
		  ref="popover"
		  placement="top"
		  width="160"
		  v-model="visible">
		  <p>{{tips}}</p>
		  <div style="text-align: right; margin: 0">
		    <el-button size="mini" type="text" @click="visible = false">{{$t('kylinLang.common.cancel')}}</el-button>
		    <el-button type="primary" size="mini" @click="visible = false;okFunc()">{{$t('kylinLang.common.ok')}}</el-button>
		  </div>
		</el-popover>
		<div v-popover:popover><slot></slot></div>
	</div>
</template>
<script>
  export default {
    name: 'confirm-btn',
    data () {
      return {
        visible: false
      }
    },
    props: ['tips'],
    methods: {
      okFunc: function () {
        this.$emit('okFunc')
      }
    }
  }
</script>
