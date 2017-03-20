<template>
	<div>
		<el-row :gutter="20">
		  <el-col :span="8"  v-for="(o, index) in modelsList" >
		    <el-card :body-style="{ padding: '0px' }">
		      <p class="title">Last updated {{o.last_modified | utcTime}}<el-button type="text" class="button">
					<el-dropdown @command="handleCommand">
					  <span class="el-dropdown-link">
					    <icon name="ellipsis-h"></icon>
					  </span>
					  <el-dropdown-menu slot="dropdown">
					    <el-dropdown-item command="a">Edit</el-dropdown-item>
					    <el-dropdown-item command="b">Clone</el-dropdown-item>
					    <el-dropdown-item command="c">Drop</el-dropdown-item>
					  </el-dropdown-menu>
					</el-dropdown>

		      </el-button></p>
		      <div style="padding: 14px;">
		        <h2>{{o.name}}</h2>
		        <div class="bottom clearfix">
		          <time class="time">{{o.owner}}</time>
		          <div class="view_btn" v-on:click="editModel(o.name)"><icon name="long-arrow-right" style="font-size:20px"></icon></div>
		        </div>
		      </div>
		    </el-card>
		  </el-col>
		</el-row>
		<pager :pageSize="pageSize" :totalSize="modelsTotal" :currentPage='currentPage' v-on:handleCurrentChange='pageCurrentChange' v-on:handleSizeChange='sizeChange'></pager>
	</div>
</template>
<script>
import { mapActions } from 'vuex'

import pager from '../common/pager'

export default {
  data () {
    return {
      currentDate: new Date(),
      pageSize: 12,
      currentPage: 1
    }
  },
  components: {
    pager
  },
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST'
    }),
    pageCurrentChange () {
      this.loadModels({limit: this.pageSize, current: this.currentPage, projectName: localStorage.getItem('selected_project')})
    },
    sizeChange () {
    },
    editModel (modelName) {
      this.$emit('addtabs', 'Edit' + modelName, 'projectList')
    }
  },
  computed: {
    modelsList () {
      return this.$store.state.model.modelsList
    },
    modelsTotal () {
      return this.$store.state.model.modelsTotal
    },
    handleCommand () {
      alert(1)
    }
  },
  created () {
    this.loadModels({limit: this.pageSize, current: this.currentPage, projectName: localStorage.getItem('selected_project')})
  }
}
</script>
<style scoped="">
 h2{
 	color:#58b7ff;
 }
 .title{
 	 margin-left: 20px;
 	 margin-top: 10px;
 	 color: #383838;
 	 font-size: 14px;
 }
 .el-col {
    margin-bottom: 20px;
    &:last-child {
      margin-bottom: 0;
    }
  }
 .time {
    font-size: 13px;
    color: #999;
  }
  
  .bottom {
    margin-top: 10px;
    margin-right: 10px;
    line-height: 12px;
  }
  .view_btn{
  	float: right;
  	/*margin-right: 10px;*/
  	color:#58b7ff;
  }
  .button {
    padding: 0;
    float: right;
    margin-top: -4px;
    margin-right: 10px;
    color:#ccc;
  }
  .button:hover{
    color:#58b7ff;
  }
  .button span{
      font-size: 14px;
    }
   .view_btn{
   	font-size: 20px;
   }
  .image {
    width: 100%;
    display: block;
  }

  .clearfix:before,
  .clearfix:after {
      display: table;
      content: "";
  }
  
  .clearfix:after {
      clear: both
  }
</style>
