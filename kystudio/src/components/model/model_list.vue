<template>
	<div>
		<el-row :gutter="20">
		  <el-col :span="8"  v-for="(o, index) in 12" >
		    <el-card :body-style="{ padding: '0px' }">
		      <p>Last updated by</p>
		      <div style="padding: 14px;">
		        <span>model</span>
		        <div class="bottom clearfix">
		          <time class="time">{{ currentDate }}</time>
		          <el-button type="text" class="button">...</el-button>
		        </div>
		      </div>
		    </el-card>
		  </el-col>
		</el-row>
		<pager :pageSize="pageSize" :totalSize="totalSize" :currentPage='currentPage' v-on:handleCurrentChange='pageCurrentChange' v-on:handleSizeChange='sizeChange'></pager>
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
      totalSize: 100,
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
      alert(1)
    },
    sizeChange () {
      alert(2)
    }
  },
  created () {
    this.loadModels()
  }
}
</script>
<style scoped="">
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
    margin-top: 13px;
    line-height: 12px;
  }

  .button {
    padding: 0;
    float: right;
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
