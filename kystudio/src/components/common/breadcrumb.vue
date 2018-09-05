<template>
	<div>
		<el-breadcrumb separator="/" >
	    <el-breadcrumb-item :to="{ path: '/overview' }"></el-breadcrumb-item>
	   	<el-breadcrumb-item v-if="currentPathName!=''">{{$t('kylinLang.menu.' + currentPath.toLowerCase())}}</el-breadcrumb-item>
	    <!-- <el-breadcrumb-item v-if="currentPathNameParent!=''" >{{currentPathNameParent}}</el-breadcrumb-item>	 -->
	  </el-breadcrumb>
	</div>
</template>
<script>
	export default {

	}
</script>
<style>
	
</style>
