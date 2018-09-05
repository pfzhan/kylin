<template>
<div id="dashbord" class="paddingbox">
  <el-row :gutter="30">
    <el-col :span="6">
      <div>
    	<el-card class="project-list">
		  <div slot="header" @click="goProject">
		    {{$t('kylinLang.common.projects')}}
		  </div>
		  <a v-if="isAdmin" class="btn-addProject" href="javascript:;" @click="addProject">+&nbsp;{{$t('kylinLang.common.project')}}</a>
      <div class="noProject" v-if="!(projectList && projectList.length)">
        <img src="../../assets/img/default_project.png" class="null_pic ksd-mb-20" >
        <div>{{$t('kylinLang.common.noProjectTips')}}</div>
      </div>
          <section data-scrollbar id="project_scroll_box">
		    <div v-for="o in projectList" :key="o.uuid" class="item" @click="selectProject(o.name)" style="cursor:pointer">
				    {{o.name}}
		    </div>
		  </section>
		</el-card>
      </div>
    </el-col>

	<el-col :span="9">
	  <div class="counter-list ky-cursor">
		<el-card @click.native="goto('Studio','model', '/studio/datasource')">
			<div slot="header" class="clearfix">
    			{{$t('kylinLang.common.models')}}
			</div>
			<section>
				{{totalModels}}
			</section>
		</el-card>
	  </div>

	  <div class="counter-list ky-cursor">
		<el-card @click.native="goto('Monitor', 'monitor', '/monitor/jobs')">
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
	<div class="counter-list ky-cursor">
		  <el-card @click.native="goto('Studio', 'cube', '/studio/datasource')">
			<div slot="header" class="clearfix">
	    	  {{$t('kylinLang.common.cubes')}}
			</div>
			<section>
			  {{totalCubes}}
			</section>
		  </el-card>
		</div>
		<div class="counter-list ky-cursor">
		  <el-card @click.native="goto('System', 'user', '/security/user')">
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
  <el-row :gutter="30" class="ksd-mt-20 ksd-mb-40">
    <el-col :span="6">
      <div>
    	<el-card >
		  <div slot="header">
		    <span>{{$t('kylinLang.common.manual')}}</span>
		  </div>
		  <div v-for="o in manualList" :key="o.title" class="item">
        <i class="dolt"></i>
		    <a :href="o.link" target="_blank">{{$t(o.title)}}</a>
		  </div>
		</el-card>
      </div>
    </el-col>
    <el-col :span="9"><div>
    	<el-card class="video-box">
		  <div slot="header">
		    <span>{{$t('kylinLang.common.tutorial')}}</span>
		  </div>
		  <div style="height:260px">
		    <video controls="controls" src="http://cdn.kyligence.io/wp-content/uploads/2018/03/showcase-v1.3.mp4" height="100%" width="100%">
				Your browser does not support HTML5 video.
			</video>
		  </div>
		</el-card>
    </div></el-col>
    <el-col :span="9">
      <div>
        <el-card>
          <div slot="header" >
            <a target="_blank" href="https://kybot.io/#/kybot/kb?src=kap250" ><span>{{$t('kylinLang.common.qa')}}</span></a>
          </div>
          <div v-for="o in blogsList" :key="o.title" class="item">
            <a :href="o.link+kapVersionPara" target="_blank">{{o.title }}</a>
          </div>
        </el-card>
      </div>
    </el-col>
  </el-row>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { hasRole, hasPermission } from 'util/business'
import { permissions } from '../../config'
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
      if (to === 'user' && !this.isAdmin) {
        return
      }
      if (to === 'monitor' && !this.isAdmin && !this.hasSomeProjectPermission()) {
        return
      }
      this.$router.push({name: routername, params: {subaction: to}})
      this.$emit('changeCurrentPath', path)
    },
    goProject () {
      this.$router.push('/project')
    },
    selectProject (projectName) {
      this.$store.state.project.selected_project = projectName
      localStorage.setItem('selected_project', projectName)
      this.$emit('changeCurrentPath', '/studio/model')
      this.$router.push('studio/model')
    },
    gotooutlink (hr) {
      location.href = hr
    },
    hasSomeProjectPermission () {
      return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask, permissions.OPERATION.mask)
    }
  },
  data () {
    return {
      sliderImgs: [{index: 0, src: require('../../assets/img/banner.png')}, {index: 1, src: require('../../assets/img/banner.png')}, {index: 2, src: require('../../assets/img/banner.png')}, {index: 3, src: require('../../assets/img/banner.png')}],
      blogsList: [{id: 0, title: 'Query returns incorrect date via JDBC', time: '3/14/2017', link: 'https://kybot.io/kybot/search_detail/115003630227'}, {id: 0, title: 'How to clean up hive temporary tables', time: '3/14/2017', link: 'https://kybot.io/kybot/search_detail/115004004868'}, {id: 0, title: 'What latency should I expect while streaming from Kafka?', time: '3/14/2017', link: 'https://kybot.io/kybot/search_detail/115003632207'}, {id: 0, title: 'Size of table snapshot exceeds the limitation', time: '3/14/2017', link: 'https://kybot.io/kybot/search_detail/115003988308'}],
      manualList: [
        {id: 0, title: 'kapManual', time: '3/14/2017', link: 'http://docs.kyligence.io'},
        {id: 0, title: 'kylinManual', time: '3/14/2017', link: 'http://kylin.apache.org/docs20/'}
      ]

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
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    kapVersionPara () {
      var _kapV = this.$store.state.system.serverAboutKap && this.$store.state.system.serverAboutKap['kap.version'] || null
      return _kapV ? '?src=' + _kapV : ''
    }
  },
  mounted () {
    this.$nextTick(() => {
      Scrollbar.init(document.getElementById('project_scroll_box'), {continuousScrolling: true})
    })
  },
  created () {
    var params = {pageSize: 1, pageOffset: 0, projectName: this.$store.state.project.selected_project}
    this.loadProjects({
      ignoreAccess: true
    })
    this.getCubesList(params)
    this.loadModels(params)
    this.loadJobsList({pageSize: 1, pageOffset: 0, timeFilter: 4, projectName: this.$store.state.project.selected_project})
    if (this.isAdmin) {
      this.loadUsersList(params)
    } else {
      this.$store.state.user.usersSize = 0
    }
  },
  locales: {
    'en': {'kapManual': 'KAP manual', 'kylinManual': 'Apache Kylin document'},
    'zh-cn': {'kapManual': 'KAP 操作手册', 'kylinManual': 'Apache Kylin 文档'}
  }
}
</script>
<style lang="less">
  @import "../../assets/styles/variables.less";
  #dashbord{
	  margin: 18px 20px 0 20px;
    .dolt {
      height: 6px;
      width: 6px;
      border-radius: 100%;
      background-color: @color-text-placeholder;
      display: inline-block;
      position: relative;
      top: -2px;
      margin-right: 2px;
    }
    .el-card {
      box-shadow: none;
      border-color: @line-border-color;
      &:hover {
        box-shadow: 0 2px 4px 0 @line-border-color;
      }
    }
    .noProject {
      text-align: center;
      margin-top: 120px;
      color: @text-normal-color;
      .null_pic{
        width: 40px;
        height: 40px;
      }
    }
	.project-list {
	  height: 600px;
	}
	.el-card__header {
	  background: @grey-3;
	  color: @text-title-color;
    font-size: 16px;
    a {
      color: @text-title-color;
      :hover {
        color: @link-hover-color
      }
    }
	}	
	.el-col {
	  .counter-list:first-child {	
		margin-bottom: 20px;
	  }
	}
	.counter-list {
	  .el-card {
		  height: 290px;
	  }
	  .el-card__body {
	  	padding: 0px;
  		height: 230px;
  		line-height: 230px;
  		font-size: 120px;
  		color: @base-color;
  		text-align: center;
	  }
	}   
	.btn-addProject {
	  display: block;
      height: 50px;
      line-height: 50px;
      font-size: 16px;
      box-shadow: inset 0 -1px 0 0 @line-border-color;
    }
    .btn-addProject:hover {
      text-decoration: none;
    } 
    .item{
  	  height: 50px;
  	  line-height: 50px;
  	  box-shadow: inset 0 -1px 0 0 @line-border-color;
  	  color: @text-title-color;
      a {
        color: @text-title-color;
        &:hover {
          color: @link-hover-color;
          text-decoration: none;
        }
      }
  	}
   	.item:last-child{
   	  box-shadow: none;
   	}
  	#project_scroll_box {
  	  height: 446px;
        .item:hover{
        	color: @base-color;
        }
  	}
   	.el-row:last-child{
   	  .el-card__body {
   	  	height: 220px;
   	  }
   	  .video-box{
  	    .el-card__body{
  		  padding: 0px;
  		  height: 260px;
  	    }
  	  }
  	  .item:hover{
        	text-decoration: none;
        }
   	}
  	.statis_box{
  		display: inline-block;
  		width: 140px;
  		height: 80px;
  		margin: 0;
  	}

  }
</style>
