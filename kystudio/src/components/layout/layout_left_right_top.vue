<template>
<div>
<el-row class="panel layout_left_right_top" :class="briefMenu">
  <el-col :span="24" class="panel-center">
    <aside class="left_menu">
      <img v-show="briefMenu!=='brief_menu'" src="../../assets/img/logo_all.png" class="logo" id="logo" @click="goHome" style="cursor:pointer;">
      <img v-show="briefMenu==='brief_menu'" src="../../assets/img/logo.png" class="logo" @click="goHome" style="cursor:pointer;"><span class="logo_text"></span>
      <el-menu style="border-top: 1px solid #475669;" :default-active="defaultActive" class="el-menu-vertical-demo" @open="handleopen" @close="handleclose" @select="handleselect" theme="dark" unique-opened router>
        <template v-for="(item,index) in menus" >
          <el-menu-item :index="item.path" :key="index" ><img :src="item.icon"> <span>{{item.name}}</span></el-menu-item>
        </template>
      </el-menu>
    </aside>
    <div class="topbar">
      <icon name="bars" v-on:click.native="toggleMenu"></icon>
      <project_select class="project_select" v-show="gloalProjectSelectShow" v-on:changePro="changeProject" ref="projectSelect"></project_select>
      <el-button v-show="gloalProjectSelectShow" @click="goToProjectList"><icon name="window-restore" scale="0.8"></icon></el-button>
      <el-button @click="addProject" v-show="gloalProjectSelectShow"><icon name="plus" scale="0.8"></icon></el-button>

      <ul>
        <li><help></help></li>
        <li style="min-width:0"><change_lang></change_lang></li>
        <li>
          <el-dropdown @command="handleCommand">
            <span class="el-dropdown-link">bob <icon name="angle-down"></icon>
            </span>
            <el-dropdown-menu slot="dropdown" >
              <el-dropdown-item command="setting">设置</el-dropdown-item>
              <el-dropdown-item command="loginout">注销</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </li>
      </ul>
    </div>
    <section class="panel-c-c" id="scrollBox">
      <div class="grid-content bg-purple-light">
        <el-col :span="24" style="margin-bottom:15px;">

          <el-breadcrumb separator="/" >
            <el-breadcrumb-item :to="{ path: '/dashbord' }"><icon class="home_icon" name="home" ></icon></el-breadcrumb-item>
             <el-breadcrumb-item v-if="currentPathName!=''">{{currentPath}}</el-breadcrumb-item>
            <!-- <el-breadcrumb-item v-if="currentPathNameParent!=''" >{{currentPathNameParent}}</el-breadcrumb-item>	 -->

          </el-breadcrumb>
        </el-col>
        <el-col :span="24" style="box-sizing: border-box;" id="mainBox">
          <!--<transition name="fade">-->
          <router-view v-on:addProject="addProject" ></router-view>
          <!--</transition>-->
        </el-col>
        <el-dialog title="Project" v-model="FormVisible" @close="resetProjectForm">
          <project_edit :project="project" ref="projectForm" v-on:validSuccess="validSuccess">
          </project_edit>
          <span slot="footer" class="dialog-footer">
            <el-button @click="FormVisible = false">取 消</el-button>
            <el-button type="primary" @click.native="Save">确 定</el-button>
          </span>
        </el-dialog>
      </div>
    </section>
  </el-col>
</el-row>
<el-dialog @close="closeResetPassword" :title="$t('resetPassword')" v-model="resetPasswordFormVisible">
    <reset_password :userDetail="currentUser" ref="resetPassword" v-on:validSuccess="resetPasswordValidSuccess"></reset_password>
    <div slot="footer" class="dialog-footer">
      <el-button @click="resetPasswordFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkResetPasswordForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>
  </div>
</template>

<script>
  import { handleSuccess, handleError } from '../../util/business'
  import { mapActions, mapMutations } from 'vuex'
  import projectSelect from '../project/project_select'
  import projectEdit from '../project/project_edit'
  import changeLang from '../common/change_lang'
  import help from '../common/help'
  import resetPassword from '../system/reset_password'
  import $ from 'jquery'
  export default {
    data () {
      return {
        project: {},
        FormVisible: false,
        currentPathName: 'DesignModel',
        currentPathNameParent: 'Model',
        defaultActive: '/dashbord',
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
          {name: 'Dashbord', path: '/dashbord', icon: require('../../assets/img/dashboard.png')},
          {name: 'Studio', path: '/studio/datasource', icon: require('../../assets/img/model.png')},
          {name: 'Insight', path: '/insight', icon: require('../../assets/img/insight.png')},
          {name: 'Monitor', path: '/monitor', icon: require('../../assets/img/monitor.png')},
          {name: 'System', path: '/system', icon: require('../../assets/img/system.png')}
        ],
        resetPasswordFormVisible: false
      }
    },
    components: {
      'project_select': projectSelect,
      'project_edit': projectEdit,
      'change_lang': changeLang,
      'reset_password': resetPassword,
      help
    },
    created () {
      let hash = location.hash.replace(/#/, '')
      for (let i = 0; i < this.menus.length; i++) {
        if (this.menus[i].path === hash) {
          this.currentPathName = this.menus[i].name
          this.defaultActive = hash
        }
      }
      this.getConf()
      this.getEncoding()
      this.getCurUserInfo().then((res) => {
        handleSuccess(res, (data) => {
          this.setCurUser({ user: data })
        })
      })
    },
    methods: {
      ...mapActions({
        loginOut: 'LOGIN_OUT',
        saveProject: 'SAVE_PROJECT',
        getConf: 'GET_CONF',
        getCurUserInfo: 'USER_AUTHENTICATION',
        getEncoding: 'GET_ENCODINGS',
        loadProjects: 'LOAD_PROJECT_LIST',
        resetPassword: 'RESET_PASSWORD'
      }),
      ...mapMutations({
        setCurUser: 'SAVE_CURRENT_LOGIN_USER'
      }),
      changeProject () {
        this.$router.go(0)
        location.reload()
      },
      addProject () {
        this.FormVisible = true
        this.project = {name: '', description: '', override_kylin_properties: {}}
      },
      Save () {
        this.$refs.projectForm.$emit('projectFormValid')
      },
      validSuccess (data) {
        let _this = this
        this.saveProject(JSON.stringify(data)).then((result) => {
          this.$message({
            type: 'success',
            message: '保存成功!'
          })
          _this.loadProjects()
        }, (res) => {
          handleError(res, (data, code, status, msg) => {
            if (status === 400) {
              this.$message({
                type: 'success',
                message: msg
              })
            }
          })
        })
        this.FormVisible = false
      },
      onSubmit () {
        console.log('submit!')
      },
      handleopen () {
      },
      handleclose () {
      },
      handleselect: function (a, b) {
      },
      toggleMenu: function () {
        this.$store.state.config.layoutConfig.briefMenu = this.$store.state.config.layoutConfig.briefMenu ? '' : 'brief_menu'
        localStorage.setItem('menu_type', this.$store.state.config.layoutConfig.briefMenu)
      },
      logoutConfirm: function () {
        return this.$confirm('确认退出吗?', '提示', {})
      },
      handleCommand (command) {
        if (command === 'loginout') {
          this.logoutConfirm().then(() => {
            this.loginOut().then(() => {
              this.$refs.projectSelect.clearProject()
              this.$router.replace('/access/login')
            })
          })
        } else if (command === 'setting') {
          console.log('this.currentUser :', this.currentUser)
          this.resetPasswordFormVisible = true
        }
      },
      goToProjectList () {
        this.$router.replace('/project')
      },
      goHome () {
        $('.el-menu').find('li').eq(0).click()
      },
      closeResetPassword: function () {
        this.$refs['resetPassword'].$refs['resetPasswordForm'].resetFields()
      },
      checkResetPasswordForm: function () {
        this.$refs['resetPassword'].$emit('resetPasswordFormValid')
      },
      resetPasswordValidSuccess: function (data) {
        // let userPassword = {
        //   username: data.username,
        //   password: data.oldPassword,
        //   newPassword: data.password
        // }
        // this.resetPassword(userPassword).then((result) => {
        //   this.$message({
        //     type: 'success',
        //     message: result.statusText
        //   })
        // }).catch((result) => {
        //   this.$message({
        //     type: 'error',
        //     message: result.statusText
        //   })
        // })
        // this.resetPasswordFormVisible = false
      },
      resetProjectForm () {
        this.$refs['projectForm'].$refs['projectForm'].resetFields()
      }
    },
    computed: {
      briefMenu () {
        return this.$store.state.config.layoutConfig.briefMenu
      },
      gloalProjectSelectShow () {
        return this.$store.state.config.layoutConfig.gloalProjectSelectShow
      },
      currentPath () {
        return this.$store.state.config.routerConfig.currentPathName
      },
      currentUser () {
        console.log('layout currentUser:', this.$store.state.user.currentUser)
        this.currentUserInfo = this.$store.state.user.currentUser
        let info = Object.create(this.currentUserInfo)
        info.password = ''
        return info
      }
    },
    mounted () {
    },
    locales: {
      'en': {resetPassword: 'Reset Password'},
      'zh-cn': {resetPassword: '重置密码'}
    }
  }
</script>

<style lang="less">
   .brief_menu {
      .logo_text {
        display: none;
      }
      .logo{
        margin: 16px 10px 10px 10px;
      }
      .left_menu {
        width: 100px;
        text-align:center;
        img.logo {
          cursor: pointer;
        }
      }
      .topbar > svg {
        margin-left: 124px;
      }


      .panel-c-c{
        left: 100px;
      }
      .el-menu {
        li {
          text-align: center;
          span {
            display: none;
          }

        }
      }
   }
  .el-breadcrumb{
    margin-left: 26px;
    margin-top: 20px;
  }
  .logo_text{
    color:#fff;
    font-size: 24px;
    vertical-align: middle;
    font-weight: 500;
    display: inline-block;
    margin-top: 2px;
  }
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
		top: 67px;
		bottom: 0px;
		left: 200px;
		overflow-y: scroll;
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
    vertical-align: middle;
		z-index:999;
		margin: 13px 10px 13px 20px;
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
  .home_icon{
      margin-top: -4px;
    }
	.topbar{
		height: 66px;
		width: 100%;
		background-color: #fff;
		border-bottom: solid 1px rgba(192,204,218,1);
		position: absolute;
    top:0;
    >svg{
    	margin-left:224px;
    	margin-top:24px;
    	cursor:pointer;
      float: left;
    }
    .project_select{
    	margin-left: 20px;
      margin-top: 16px;
      float: left;
    }
    .el-dropdown{
    	cursor:pointer;
      svg{
    	vertical-align:middle;
    	margin-left:2px;
      }
    }
    .el-button {
      padding: 8px 12px;
      margin-top: 18px;
      margin-left: 4px;
    }
    ul{
    	float:right;
    	li{
    		min-width:150px;
    		display:inline-block;
    		height:66px;
    		line-height:66px;
    		text-align:center;
    	}

    }


	}
	.el-menu{
    padding-top: 40px;
		li{
      text-align: left;
      span{
        color: #c0ccda;
        font-size: 14px;
      }
			img{
        vertical-align: middle;
        width: 20px;
        height: 20px;
        margin-right: 10px;
      }
		}
		.is-active{
		  border-left: solid 4px #58b7ff;
      color: #fff;
      background: #475669;
		}
	}
	.left_menu{
		position:relative;
		z-index:999;
		background-color: #20a0ff;
    width: 200px;
	}
    .el-icon-arrow-down:before{
		content: ''
	}

</style>
<style lang="less">
   #mainBox{
      .fullbox{

      }
      .paddingbox{
        padding: 20px;
      }
    }
</style>

