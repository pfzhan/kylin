<template>
	<el-row class="panel">
		<el-col :span="24" class="panel-center">
			<!--<el-col :span="4">-->
			<aside style="width:230px;" class="left_menu">
			    <img src="../../assets/logo.png" class="logo">
				<el-menu style="border-top: 1px solid #475669;" default-active="/table" class="el-menu-vertical-demo" @open="handleopen"
					@close="handleclose" @select="handleselect" theme="dark" unique-opened router>
					<template v-for="(item,index) in menus" >
						<el-menu-item :index="item.path"  >{{item.name}}</el-menu-item>
					</template>
				</el-menu>
			</aside>
			<!--</el-col>-->
			<!--<el-col :span="20" class="panel-c-c">-->
			<div class="topbar">
				<icon name="bars"></icon>
			    <!-- <div class="topbar_option"> -->
					<!-- <el-dropdown>
					  <span class="el-dropdown-link">
					    bob<i class="el-icon-caret-bottom el-icon--right"></i>
					  </span>
					  <el-dropdown-menu slot="dropdown">
					    <el-dropdown-item>黄金糕</el-dropdown-item>
					    <el-dropdown-item>狮子头</el-dropdown-item>
					    <el-dropdown-item>螺蛳粉</el-dropdown-item>
					    <el-dropdown-item disabled>双皮奶</el-dropdown-item>
					    <el-dropdown-item divided>蚵仔煎</el-dropdown-item>
					  </el-dropdown-menu>
					</el-dropdown> -->
					<ul>
						<li>bingo</li>
						<li>
							<el-dropdown>
							  <span class="el-dropdown-link">
							    bob<icon name="angle-down"></icon>
							  </span>
							  <el-dropdown-menu slot="dropdown">
							    <el-dropdown-item>注销</el-dropdown-item>
							  </el-dropdown-menu>
							</el-dropdown>
						</li>
					</ul>
				<!-- </div> -->
			</div>
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

				<!-- <project_list></project_list> -->
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
          {name: 'Dashbord', path: '/dashbord/index'},
          {name: 'KyStudio', path: '/model'},
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
        console.log(to)
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

<style lang="less" scoped>
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
		
		top: 0px;
		bottom: 0px;
		overflow: hidden;
	}
	
	.panel-c-c {
		background: #f1f2f7;
		position: absolute;
		right: 0px;
		top: 66px;
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
		z-index:999;
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
	.topbar{
		height: 66px;
		width: 100%;
		background-color: #fff;
		
		position: fixed;
        top:0;
        >svg{
        	margin-left:245px;
        	margin-top:24px;
        	cursor:pointer;
        }
        .el-dropdown{
        	cursor:pointer;
          svg{
        	vertical-align:middle;
        	margin-left:2px;
          }
        }
        ul{
        	float:right;
        	li{
        		min-width:150px;
        		display:inline-block;
        		border-left:solid 1px #ccc;
        		height:66px;
        		line-height:66px;
        		text-align:center;
        	}

        }
        

	}
	.el-menu{
		li{
			text-align:center;
		}
		.is-active{
			background-color:#58b7ff;
			color:#fff;
		}
	}
	.left_menu{
		position:relative;
		z-index:999;
		background-color: #324157;
	}
    .el-icon-arrow-down:before{
		content: ''
	}
	
</style>

