<template>
  <div class="query_panel_box">
    <p class="tips">querying <span>{{pending}}</span>S in Cube: <span>aire_line</span></p>
    <div v-show="!errinfo">
      <el-progress type="circle" :percentage="percent"></el-progress>
      <!-- <div class="panel_btn"><icon name="close"></icon> cancel</div> -->
    </div>
	  <div v-show="errinfo" class="errorBox">
      <i class="el-icon-circle-cross"></i>
      <p class = "errorText">{{errinfo}}</p>
    </div>
  </div>

</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
export default {
  name: 'queryPanel',
  props: ['extraoption'],
  methods: {
    ...mapActions({
      query: 'QUERY_BUILD_TABLES'
    })
  },
  data () {
    return {
      errinfo: '',
      percent: 0,
      ST: null,
      pending: 0
    }
  },
  created () {
  },
  mounted () {
    var _this = this
    clearInterval(this.ST)
    this.ST = setInterval(function () {
      this.pending += 300
      var randomPlus = Math.round(10 * Math.random())
      if (_this.percent + randomPlus < 99) {
        _this.percent += randomPlus
      } else {
        clearInterval(_this.ST)
      }
    }, 300)
    this.query({
      acceptPartial: this.extraoption.acceptPartial,
      limit: this.extraoption.limit,
      offset: this.extraoption.offset,
      project: this.extraoption.project,
      sql: this.extraoption.sql
    }).then((res) => {
      clearInterval(_this.ST)
      handleSuccess(res, (data) => {
        this.$emit('changeView', _this.extraoption.index, data)
      })
    }, (res) => {
      handleError(res, (data, code, status, msg) => {
        this.errinfo = msg
        this.$emit('changeView', _this.extraoption.index, data, 'close', 'querypanel')
      })
    })
  },
  locales: {
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员'}
  },
  destoryed () {
    clearInterval(this.ST)
  }
}
</script>
<style lang="less">
  .query_panel_box{
    text-align: center;
    .tips{
      font-size: 12px;
      margin-bottom: 20px;
      span{
        color: #20a0ff;
      }
    }
    .el-progress{
      margin-bottom: 10px;
    }
    .panel_btn{
      width: 140px;
      margin-top: 20px;
      border-top: solid 1px #ccc;
      margin: 0 auto;
      padding-top: 10px;
    }
    .errorBox{
      color:red;
      font-size: 12px;
      i{
        font-size: 14px;
        margin-bottom: 20px;
        &.el-icon-circle-close {
          color:red
        }
      }
    }
  }
</style>
