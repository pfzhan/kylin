<template>
	<el-row class="panel">
		<el-col :span="24" class="panel-top">
			<el-col :span="20" style="font-size:26px;">
				<img src="../../assets/logo.png" class="logo"> <span>KY<i style="color:#20a0ff">Studio</i></span>
			</el-col>
			<el-col :span="4">
				<el-tooltip class="item tip-logout" effect="dark" content="退出" placement="bottom" style="padding:0px;">
					<!--<i class="logout" v-on:click="logout"></i>-->
					<i class="fa fa-sign-out" aria-hidden="true" v-on:click="logout"></i>
				</el-tooltip>
			</el-col>
		</el-col>
		<el-col :span="24" class="panel-center">
			<!--<el-col :span="4">-->
			<aside style="width:230px;">
				<el-menu style="border-top: 1px solid #475669;" default-active="/table" class="el-menu-vertical-demo" @open="handleopen"
					@close="handleclose" @select="handleselect" theme="dark" unique-opened router>
					<template v-for="(item,index) in menus" >
						<el-menu-item :index="item.path"  >{{item.name}}</el-menu-item>
					</template>
				</el-menu>
			</aside>
			<!--</el-col>-->
			<!--<el-col :span="20" class="panel-c-c">-->
			<section class="panel-c-c">
				<div class="grid-content bg-purple-light">
					<el-col :span="24" style="margin-bottom:15px;">
						<strong style="width:200px;float:left;color: #475669;">{{currentPathName}}</strong>
						<el-breadcrumb separator="/" style="float:right;">
							<el-breadcrumb-item :to="{ path: '/table' }">首页</el-breadcrumb-item>
							<el-breadcrumb-item v-if="currentPathNameParent!=''">{{currentPathNameParent}}</el-breadcrumb-item>
							<el-breadcrumb-item v-if="currentPathName!=''">{{currentPathName}}</el-breadcrumb-item>
						</el-breadcrumb>
					</el-col>
					<el-col :span="24" style="background-color:#fff;box-sizing: border-box;">
						<!--<transition name="fade">-->
							<router-view></router-view>
						<!--</transition>-->
					</el-col>
				</div>

				<project_list></project_list>
<!-- 			            <total name="Projects"></total> -->
<!--                                                   <model_list></model_list> -->
 <!--                                                 <cubes_list></cubes_list> -->
			</section>
			<!--</el-col>-->
		</el-col>
	</el-row>
</template>

<script>
  import projectList from '../project/project_list'
  import total from '../common/total'
  import modelList from '../model/model_list'
  import cubesList from '../cube/cubes_list'
  export default {
    data () {
      return {
        currentPathName: 'DesignModel',
        currentPathNameParent: 'Model',
        form: {
          name: '',
          region: '',
          date1: '',
          date2: '',
          delivery: false,
          type: [],
          resource: '',
          desc: ''
        },
        menus: [
          {name: 'Dashbord', path: '/model'},
          {name: 'KyStudio', path: '/hehe'},
          {name: 'KyAnalyzer', path: ''},
          {name: 'KyManager', path: ''},
          {name: 'System', path: ''}
        ]
      }
    },
    components: {
      'project_list': projectList,
      'total': total,
      'model_list': modelList,
      'cubes_list': cubesList
    },
    watch: {
      '$route' (to, from) { // 监听路由改变
        this.currentPathName = to.name
        this.currentPathNameParent = to.matched[0].name
      }
    },
    methods: {
      onSubmit () {
        console.log('submit!')
      },
      handleopen () {
      },
      handleclose () {
      },
      handleselect: function (a, b) {
      },
      logout: function () {
        var _this = this
        this.$confirm('确认退出吗?', '提示', {
        }).then(() => {
          _this.$router.replace('/login')
        }).catch(() => {
        })
      }
    }
  }
</script>

<style scoped>
	.fade-enter-active,
	.fade-leave-active {
		transition: opacity .5s
	}
	
	.fade-enter,
	.fade-leave-active {
		opacity: 0
	}
	
	.panel {
		position: absolute;
		top: 0px;
		bottom: 0px;
		width: 100%;
	}
	
	.panel-top {
		height: 60px;
		line-height: 60px;
		background: #1F2D3D;
		color: #c0ccda;
	}
	
	.panel-center {
		background: #324057;
		position: absolute;
		top: 60px;
		bottom: 0px;
		overflow: hidden;
	}
	
	.panel-c-c {
		background: #f1f2f7;
		position: absolute;
		right: 0px;
		top: 0px;
		bottom: 0px;
		left: 230px;
		overflow-y: scroll;
		padding: 20px;
	}
	
	.logout {
		/*background: url(../assets/logout_36.png);*/
		background-size: contain;
		width: 20px;
		height: 20px;
		float: left;
	}
	
	.logo {
		height: 40px;
		float: left;
		margin: 10px 10px 10px 18px;
	}
	
	.tip-logout {
		float: right;
		margin-right: 20px;
		padding-top: 5px;
	}
	
	.tip-logout i {
		cursor: pointer;
	}
	
	.admin {
		color: #c0ccda;
		text-align: center;
	}


	
</style>
<style lang="less">
	.el-icon-arrow-down:before{
		content: ''
	}
</style>
