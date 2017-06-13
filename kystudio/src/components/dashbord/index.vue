<template>
<div id="dashbord" class="paddingbox">
  <el-row :gutter="30">
    <el-col :span="6">
      <div class="grid-content grid-project bg-purple">
	    	<el-card class="box-card box-project">
				  <div slot="header" class="clearfix" @click="goProject">
				    {{$t('kylinLang.common.project')}}
				  </div>
			    <a class="btn-addProject" href="javascript:;" @click="addProject">+{{$t('kylinLang.common.project')}}</a>
					<section data-scrollbar id="project_scroll_box">
					  <div v-for="o in projectList" :key="o.uuid" class="text item" @click="selectProject(o.name)" style="cursor:pointer">
					    {{o.name}}
					  </div>
				  </section>
				</el-card>
      </div>
    </el-col>
    <!-- <el-col :span="16" class="slider_bpx">
       	<div class="grid-content bg-purple">
    	<div class="block">
		    <el-carousel trigger="click" height="280px">
		      <el-carousel-item v-for="img in sliderImgs" :key="img.src">
		        <img :src="img.src">
		      </el-carousel-item>
		    </el-carousel>
	  	</div>
    	</div>
    </el-col> -->
    	<el-col :span="9">
    		<div class="counter-list counter-list1">
    			<el-card @click.native="goto('Studio','model', '/studio/datasource')">
    				<div slot="header" class="clearfix">
		    			{{$t('kylinLang.common.models')}}
					</div>
					<section>
						{{totalModels}}
					</section>
    			</el-card>
    		</div>
    		<div class="counter-list counter-list2">
    			<el-card @click.native="goto('Monitor', '', '/monitor')">
    				<div slot="header" class="clearfix">
		    			{{$t('kylinLang.common.jobs')}}
					</div>
					<section>
						{{totalJobs}}
					</section>
    			</el-card>
    		</div>
    	</el-col>
    	<el-col :span="9">
    		<div class="counter-list counter-list3">
    			<el-card @click.native="goto('Studio', 'cube', '/studio/datasource')">
					<div slot="header" class="clearfix">
		    			{{$t('kylinLang.common.cubes')}}
					</div>
					<section>
						{{totalCubes}}
					</section>
				</el-card>
    		</div>
    		<div class="counter-list counter-list4">
    			<el-card @click.native="goto('System', 'user', '/system/config')">
					<div slot="header" class="clearfix">
		    			{{$t('kylinLang.common.users')}}
					</div>
					<section>
						{{totalUsers}}
					</section>
				</el-card>
    		</div>
    	</el-col>
    </el-row>
  </el-row>
  <el-row :gutter="30">
    <el-col :span="6">
      <div class="grid-content bg-purple">
    	<el-card class="box-card">
		  <div slot="header" class="clearfix">
		    <span>{{$t('kylinLang.common.manual')}}</span>
		  </div>
		  <div v-for="o in manualList" :key="o.title" class="text item">
		    <a :href="o.link" target="_blank">{{o.title }}</a>
		  </div>
		</el-card>
      </div>
    </el-col>
    <el-col :span="9"><div class="grid-content bg-purple video_box">
    	<el-card class="box-card">
		  <div slot="header" class="clearfix">
		    <span>{{$t('kylinLang.common.tutorial')}}</span>
		  </div>
		  <div >
		    <video controls="controls" src="http://kyligence.io/public/assets/video/video.mp4" data-current="--" data-duration="--">
				Your browser does not support HTML5 video.
			</video>
		  </div>
		</el-card>
    </div></el-col>
    <el-col :span="9">
      <div class="grid-content bg-purple">
        <el-card class="box-card">
          <div slot="header" class="clearfix">
            <span>{{$t('kylinLang.common.qa')}}</span>
          </div>
          <div v-for="o in blogsList" :key="o.title" class="text item">
            <a :href="o.link" target="_blank">{{o.title }}</a>
          </div>
        </el-card>
      </div>
    </el-col>
  </el-row>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import Scrollbar from 'smooth-scrollbar'
export default {
  methods: {
    ...mapActions({
      loadProjects: 'LOAD_ALL_PROJECT',
      getCubesList: 'GET_CUBES_LIST',
      loadModels: 'LOAD_MODEL_LIST',
      loadJobsList: 'LOAD_JOBS_LIST',
      loadUsersList: 'LOAD_USERS_LIST'
    }),
    addProject () {
      this.$emit('addProject')
    },
    goto (routername, to, path) {
      this.$router.push({name: routername, params: {subaction: to}})
      this.$emit('changeCurrentPath', path)
    },
    goProject () {
      this.$router.push('/project')
    },
    selectProject (projectName) {
      this.$store.state.project.selected_project = projectName
      localStorage.setItem('selected_project', projectName)
      this.$emit('changeCurrentPath', '/studio/datasource')
      this.$router.push('studio/datasource')
    }
  },
  data () {
    return {
      sliderImgs: [{index: 0, src: require('../../assets/img/banner.png')}, {index: 1, src: require('../../assets/img/banner.png')}, {index: 2, src: require('../../assets/img/banner.png')}, {index: 3, src: require('../../assets/img/banner.png')}],
      newsList: [{id: 0, title: '大数据初创企业Kyligence亮相硅谷顶级大数据峰会Strata+Hadoop World', time: '3/14/2017'}, {id: 0, title: 'Kyligence智能分析平台助力国泰君安构建互联网级大数据分析平台', time: '3/14/2017'}, {id: 0, title: '华人顶级开源项目强强联合，Kyligence与Alluxio达成战略合作协议', time: '3/14/2017'}, {id: 0, title: 'Kyligence成功入选【微软加速器·上海】，成为“黄埔一期”成员', time: '3/14/2017'}],
      blogsList: [{id: 0, title: 'Query returns incorrect date via JDBC', time: '3/14/2017', link: 'https://kybot.io/#/kybot/searchDetail/115003630227'}, {id: 0, title: 'How to clean up hive temporary tables', time: '3/14/2017', link: 'https://kybot.io/#/kybot/searchDetail/115004004868'}, {id: 0, title: 'What latency should I expect while streaming from Kafka?', time: '3/14/2017', link: 'https://kybot.io/#/kybot/searchDetail/115003632207'}, {id: 0, title: 'Size of table snapshot exceeds the limitation', time: '3/14/2017', link: 'https://kybot.io/#/kybot/searchDetail/115003988308'}],
      manualList: [{id: 0, title: 'KAP 操作手册', time: '3/14/2017', link: 'https://kyligence.gitbooks.io/kap-manual/content/zh-cn/'}, {id: 0, title: 'Apache Kylin 文档', time: '3/14/2017', link: 'http://kylin.apache.org/docs16/'}]

    }
  },
  computed: {
    projectList () {
      return this.$store.state.project.allProject
    },
    totalModels () {
      return this.$store.state.model.modelsTotal || 0
    },
    totalCubes () {
      return this.$store.state.cube.totalCubes || 0
    },
    totalJobs () {
      return this.$store.state.monitor.totalJobs || 0
    },
    totalUsers () {
      return this.$store.state.user.usersSize || 0
    }
  },
  mounted () {
    this.$nextTick(() => {
      Scrollbar.init(document.getElementById('project_scroll_box'))
    })
  },
  created () {
    var params = {pageSize: 1, pageOffset: 0}
    this.loadProjects()
    this.getCubesList(params)
    this.loadModels(params)
    this.loadUsersList(params)
    this.loadJobsList({pageSize: 1, pageOffset: 0, timeFilter: 1})
  }
}
</script>
<style lang="less">
	@import url(../../less/config.less);
	.title-bar {
		background: @bg-top;
		border:0 none;
		color: @fff;
	}
	#dashbord{
		margin: 0 30px 0 30px;
		.el-card.box-project {
			height: 590px;
		}
		.grid-content {
			.el-card {
				.title-bar;
				background: @bg-top;
				border:0 none;
				color: @fff;
			}
			.el-card__header {
				background: @grey-color;
				color: @fff;
				// font-size: 14px;
				border-bottom: 0;
			}
            .el-card__body{
                position: relative;
            }
		}
		.el-col {
			.counter-list:first-child {
				height: 260px;
				margin-bottom: 50px;
			}
		}
		.counter-list {
			.el-card {
				border: 0 none;
				background: transparent;
			}
			.el-card__header {
				background: @grey-color;
			}
			.el-card__body {
				height: 200px;
				line-height: 200px;
				font-size: 100px;
				color: @fff;
				text-align: center;
			}
		}
		.counter-list1 {
			.el-card__body {
				background: -webkit-linear-gradient(to bottom,#3296e9,#1275c6);
				background: -moz-linear-gradient(to bottom,#3296e9,#1275c6);
				background: linear-gradient(to bottom,#3296e9,#1275c6);
			}
			.el-card__body:hover{
				background: #1275c6;
			}
		}
		.counter-list2 {
			.el-card__body {
				background: -webkit-linear-gradient(to bottom,#ecb860,#cf9532);
				background: -moz-linear-gradient(to bottom,#ecb860,#cf9532);
				background: linear-gradient(to bottom,#ecb860,#cf9532);
			}
			.el-card__body:hover{
				background: #cf9532;
			}
		}
		.counter-list3 {
			.el-card__body {
				background: -webkit-linear-gradient(to bottom,#59ce5e,#33a638);
				background: -moz-linear-gradient(to bottom,#59ce5e,#33a638);
				background: linear-gradient(to bottom,#59ce5e,#33a638);
			}
			.el-card__body:hover{
				background: #33a638;
			}
		}
		.counter-list4 {
			.el-card__body {
				background: -webkit-linear-gradient(to bottom,#f35f56,#cb3a30);
				background: -moz-linear-gradient(to bottom,#f35f56,#cb3a30);
				background: linear-gradient(to bottom,#f35f56,#cb3a30);
			}
			.el-card__body:hover{
				background: #cb3a30;
			}
		}

		#project_scroll_box {
		  height: 446px;
          padding-left: 10px;
		}
		.single-line {
			display: inline-block;
			width: 80%;
		}
		.video_box{
			.el-card__body{
			   padding: 0
		    }
			height: 100%;
			width: 100%;
			video{
				width: 100%;
				height: 208px;
				background: #000;
			}
		}
		.statis_box{
			display: inline-block;
			width: 140px;
			height: 80px;
			margin: 0;
		}
		table{
			margin: 14px auto;
			width: 80%;
			height: 180px;
			td{
			  text-align: center;
			  p{
			  	font-size: 10px;
			  	color: #8492a6;
			  }
			  h2{
			  	font-size: 48px;
			  	color: #1f2d3d;
			  	font-weight: normal;
			  }
			}
		}
		.left_border {
           border-right: solid 1px #ccc;
           border-bottom:solid 1px #ccc;
		}
		.right_border{
			border-top: solid 1px #ccc;
           border-left:solid 1px #ccc;
		}
		.el-row{
			margin-bottom: 30px;
		}
		.el-card{
			height: 280px;
            cursor: pointer;
		}
	  .item{
	 	height: 50px;
	 	line-height: 50px;
        padding-left: 10px;
	 	border-bottom:solid 1px @line-color;
	 	font-size: 14px;
	 	color: @fff;
	 	.time_box{
	 		font-size: 12px;
	 		color: #8492a6;
	 		float: right;
	 	}
	 	.item:last-child{
	 		border:none
	 	}
        a{
          color: @fff;  
        }
	 	a:hover {
	 		text-decoration: none;
	 	}
	 }
	 .el-card__header{
	 	height: 60px;
	 	font-size: 18px;
	 	padding: 20px;
	 }
	 .el-card__body{
	 	padding-top: 4px;
	 	padding-right: 8px;
	 }
	 .el-carousel__item img {
	    height: 100%;
	    width: 100%;
	  }

	  .el-carousel__item:nth-child(2n) {
	     background-color: #99a9bf;
	  }

	  .el-carousel__item:nth-child(2n+1) {
	     background-color: #d3dce6;
	  }

		.btn-addProject {
            display: block;
            height: 50px;
            line-height: 50px;
            margin: 0;
            padding-left: 10px;
            border-bottom: 1px solid @line-color;
        }
        .btn-addProject:hover {
            text-decoration: none;
        }
	}
</style>
