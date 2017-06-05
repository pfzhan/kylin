<template>
<div id="dashbord" class="paddingbox">
  <el-row :gutter="30">
    <el-col :span="8">
      <div class="grid-content bg-purple">
    	<el-card class="box-card">
		  <div slot="header" class="clearfix">
		    <span style="line-height: 36px;">Project</span>
		  </div>
		  <section data-scrollbar id="project_scroll_box">
		  <a class="btn-addProject" href="javascript:;" @click="addProject">+ Project</a>
		  <div v-for="o in projectList" :key="o.uuid" class="text item" @click="selectProject(o.name)" style="cursor:pointer">
		    {{o.name }}
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
    	<el-col :span="8">
    		<div>
    			models
    		</div>
    		<div>
    			cubes
    		</div>
    	</el-col>
    	<el-col :span="8">
    		<div>Jobs</div>
    		<div>Users</div>
    	</el-col>
    </el-row>
  </el-row>
  <el-row :gutter="30">
    <el-col :span="8">
      <div class="grid-content bg-purple">
    	<el-card class="box-card">
		  <div slot="header" class="clearfix">
		    <span style="line-height: 36px;">Documentation</span>
		  </div>
		  <div v-for="o in manualList" :key="o.title" class="text item">
		    <a>{{o.title }}</a>
		  </div>
		</el-card>
      </div>
    </el-col>
    <el-col :span="8"><div class="grid-content bg-purple video_box">
    	<el-card class="box-card">
		  <div slot="header" class="clearfix">
		    <span style="line-height: 36px;">Tutorial</span>
		  </div>
		  <div >
		    <video controls="controls" src="http://kyligence.io/public/assets/video/video.mp4" data-current="--" data-duration="--">
				Your browser does not support HTML5 video.
			</video>
		  </div>
		</el-card>
    </div></el-col>
    <el-col :span="8"><div class="grid-content bg-purple">
    	<el-card class="box-card">
		  <div slot="header" class="clearfix">
		    <span style="line-height: 36px;">Statistics</span>
		  </div>
		  <div >
		     <table cellspacing="0">
		     	<tr>
		     		<td style="border-right:solid 1px #ccd3db;border-bottom:solid 1px #ccd3db">
		     		  <h2 @click="goto('Project')" style="cursor:pointer">18</h2>
		       	      <p>Projects</p>
		     		</td>
		     		<td style="border-bottom:solid 1px #ccd3db">
		     		  <h2 @click="goto('Studio', 'model')" style="cursor:pointer">89</h2>
		       	      <p>Models</p>
		     		</td>
		     	</tr>
		     	<tr>
		     		<td style="border-right:solid 1px #ccd3db;">
		     		  <h2 @click="goto('Studio', 'cube')" style="cursor:pointer">15</h2>
		          	  <p>Cubes</p>
		     		</td>
		     		<td >
		     		<h2 @click="goto('Monitor')" style="cursor:pointer">97</h2>
		       	      <p>Jobs</p>
		     		</td>
		     	</tr>
		     </table>
		  </div>
		</el-card>
    </div></el-col>
  </el-row>
  <el-row :gutter="30">
    <el-col :span="12"><div class="grid-content bg-purple news-wrap">
    	<el-card class="box-card">
		  <div slot="header" class="clearfix">
		    <span style="line-height: 36px;">News</span>
		  </div>
		  <div v-for="o in newsList" :key="o.title" class="text item">
		     <a class="single-line">{{o.title }}</a><span class="fright time_box">{{o.time}}</span>
		  </div>
		</el-card>
    </div></el-col>
    <el-col :span="12"><div class="grid-content bg-purple">
    	<el-card class="box-card">
		  <div slot="header" class="clearfix">
		    <span style="line-height: 36px;">Blog</span>
		  </div>
		  <div v-for="o in blogsList" :key="o.title" class="text item">
		    <a class="single-line">{{o.title }}</a><span class="fright time_box">{{o.time}}</span>
		  </div>
		</el-card>
    </div></el-col>
  </el-row>
</div>
</template>

<script>
import { mapActions } from 'vuex'
// import Scrollbar from 'smooth-scrollbar'
export default {
  methods: {
    ...mapActions({
      loadProjects: 'LOAD_ALL_PROJECT'
    }),
    addProject () {
      this.$emit('addProject')
    },
    goto (routername, to) {
      this.$router.push({name: routername, params: {subaction: to}})
    },
    selectProject (projectName) {
      this.$store.state.project.selected_project = projectName
      localStorage.setItem('selected_project', projectName)
      this.$router.push('studio/datasource')
    }
  },
  data () {
    return {
      sliderImgs: [{index: 0, src: require('../../assets/img/banner.png')}, {index: 1, src: require('../../assets/img/banner.png')}, {index: 2, src: require('../../assets/img/banner.png')}, {index: 3, src: require('../../assets/img/banner.png')}],
      newsList: [{id: 0, title: '大数据初创企业Kyligence亮相硅谷顶级大数据峰会Strata+Hadoop World', time: '3/14/2017'}, {id: 0, title: 'Kyligence智能分析平台助力国泰君安构建互联网级大数据分析平台', time: '3/14/2017'}, {id: 0, title: '华人顶级开源项目强强联合，Kyligence与Alluxio达成战略合作协议', time: '3/14/2017'}, {id: 0, title: 'Kyligence成功入选【微软加速器·上海】，成为“黄埔一期”成员', time: '3/14/2017'}],
      blogsList: [{id: 0, title: 'Apache Kylin 在唯品会大数据的应用', time: '3/14/2017'}, {id: 0, title: 'Apache Kylin在美团数十亿数据OLAP场景下的实践', time: '3/14/2017'}, {id: 0, title: '大数据多维分析引擎在魅族的实践', time: '3/14/2017'}, {id: 0, title: '链家网大数据：Apache Kylin作为OLAP引擎提供快速多维分析能力', time: '3/14/2017'}],
      manualList: [{id: 0, title: 'KAP 手册', time: '3/14/2017'}, {id: 0, title: 'Kylin 手册', time: '3/14/2017'}, {id: 0, title: 'Kybot 手册', time: '3/14/2017'}, {id: 0, title: 'KyAnalyzer 手册', time: '3/14/2017'}]

    }
  },
  computed: {
    projectList () {
      return this.$store.state.project.allProject
    }
  },
  mounted () {
    // Scrollbar.init(document.getElementById('project_scroll_box'))
  },
  created () {
    this.loadProjects()
  }
}
</script>
<style lang="less">
	@import url(../../less/config.less);
	#dashbord{
		.grid-content {
			.el-card {
				background: @bg-top;
				border:0 none;
			}
			.el-card__header {
				background: @bg-menu;
				color: @fff;
				font-size: 14px;
				border-bottom: 0;
			}
		}
		#project_scroll_box {
		  height: 220px;
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
		}
	  .item{
	 	height: 50px;
	 	line-height: 50px;
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
	 	a:hover {
	 		text-decoration: none;
	 	}
	 }
	 .el-card__header{
	 	background-color: #eff2f7;
	 	height: 72px;
	 	font-size: 18px;
	 	color: #475669;
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
            border-bottom: 1px solid @line-color;
        }
        .btn-addProject:hover {
            text-decoration: none;
        }
	}

</style>
