<template>
	<div class="about-kap">
		<div class="header">
			<a href="http://kyligence.io/" target="_blank">
				<img src="../../assets/img/test.png" alt="" />
			</a>
			<el-row><label for="">{{$t('version')}}</label>{{license(serverAboutKap['kap.version'])}}</el-row>
					<el-row><label for="">{{$t('validPeriod')}}</label>{{license(serverAboutKap['kap.dates'])}}</el-row>
		<el-row><label for="">{{$t('licenseStatement')}}</label>{{license(serverAboutKap['kap.license.statement'])}}</el-row>		
		</div>
		<div class="container">
			<h3>{{$t('statement')}}</h3>
			<p>{{$t('statementContent')}}</p>
			<el-row>
				<label for="">{{$t('serviceEnd')}}</label>
				{{license(serverAboutKap['kap.license.serviceEnd'])}}
			</el-row>
			<el-row>
			  <label for="">KAP Commit:</label>
			  {{license(serverAboutKap['kap.commit'])}}
			</el-row>
		</div>
		<div class="footer">
			<p class="details">{{kyAccount}}</p>
			<el-button type="primary" @click="getLicense">生成许可申请文件</el-button>
			<el-row class="gray">{{$t('sendFile')}}</el-row>
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
    },
    kyAccount () {
      return this.$store.state.system.kyAccount || '未在Kylin properties中配置KyAccount账号'
    },
    statement () {
      return this.$store.state.system.statement
    }
  },
  methods: {
    license (obj) {
      if (!obj) {
        return 'N/A'
      } else {
        return obj
      }
    },
    getLicense () {
      location.href = './api/kap/system/requestLicense'
    }
  },
  locales: {
    'en': {version: 'Version: ', validPeriod: 'Valid Period: ', serviceEnd: 'Service End Time:', statement: 'Service Statement', statementContent: 'You are using KAP enterprise product and service. If you have any issues about KAP, please contact us. We will continue to provide you with quality products and services from Apache Kylin core team.', licenseStatement: 'License Statement: ', sendFile: 'To request license, please contact with Kyligence sales support channel with the License Request file.'},
    'zh-cn': {version: '版本: ', validPeriod: '使用期限: ', serviceEnd: '服务截止日期:', statement: '服务申明', statementContent: '您正在使用KAP试用版，如果您对我们的产品满意，需要专业的产品、咨询或服务，请联系我们，您将获得来自Apache Kylin核心小组的帮助。', licenseStatement: '许可声明: ', sendFile: '申请许可请将许可申请文件发送到Kyligence销售支持渠道'}
  }
}
</script>
<style lang="less">
	.about-kap {
		line-height:30px;
		font-size:14px;
		text-align: center;
		img {width: 100px;height:100px;}
		label {font-weight:bold;}
		.header, 
		.container {padding-bottom:20px;border-bottom:1px solid #D3DCE6;}
		h3 {margin-top:20px;font-size:14px;}
		.details {line-height:24px;margin:20px 0 30px;}
		.gray {margin:6px 0 30px;color:#a2a2a2;font-size:12px;}
	}
</style>
