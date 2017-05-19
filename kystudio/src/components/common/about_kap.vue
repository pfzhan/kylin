<template>
	<div class="about-kap">
		<div class="header">
			<img src="../../assets/img/test.png" alt="" />
			<el-row><label for="">版本：</label>{{license(serverAboutKap['kap.version'])}}</el-row>
					<el-row><label for="">使用期限：</label>{{license(serverAboutKap['kap.dates'])}}</el-row>
			<el-row><label for="">许可声明：</label>{{license(serverAboutKap['kap.license.statement'])}}</el-row>		
		</div>
		<div class="container">
			<h3>服务声明</h3>
			<p>您正在使用KAP试用版，如果您对我们的产品满意，需要专业的产品、咨询或服务，请联系我们，<br/>
				您将获得来自Apache Kylin核心小组的帮助。
			</p>
		</div>
		<div class="footer">
			<p class="details">kyAccount.xxxx@xxxx.com<br />未在Kylin properties 中配置KyAccount</p>
			<el-button type="primary">生成许可申请文件</el-button>
			<el-row class="gray">申请许可请将许可申请文件发送到Kyligence销售支持渠道</el-row>
			<el-row class="gray">All Rights Reserved. Kyligence Inc.</el-row>
		</div>
	</div>
</template>
<script>
	export default {
	  name: 'about_kap',
	  data () {
	    return {
	      aboutKap: this.about
	    }
	  },
	  computed: {
	    serverAboutKap () {
	      return this.$store.state.system.serverAboutKap
	    }
	  },
	  methods: {
	    license (obj) {
	      if (!obj) {
	        return 'N/A'
	      } else {
	        return obj
	      }
	    }
	  }
	}
</script>
<style lang="less">
	.about-kap {
		line-height:30px;
		font-size:14px;
		img {width: 100px;height:100px;}
		label {font-weight:bold;}
		.header, 
		.container {padding-bottom:20px;border-bottom:1px solid #D3DCE6;}
		h3 {margin-top:20px;font-size:14px;}
		.details {line-height:24px;margin:20px 0 30px;}
		.gray {margin:6px 0 30px;color:#a2a2a2;font-size:12px;}
	}
</style>

