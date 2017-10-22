<template>
  <div id="manageCard">
    <el-card>
      <div slot="header">
        <el-row :gutter="5">
          <el-col :span="1" class="ksd-lineheight-35 check_box">
            <slot name="header">
            </slot>
          </el-col>
          <el-col :span="22" :offset="hasCheck?0:1" class="ksd-lineheight-35" @click.native="changeShow">
            <h2 class="ksd-lineheight-36">{{title}}</h2>
            <slot name="tip">
            </slot>
          </el-col>
          <el-col :span="1" class="ksd-lineheight-35">
            <el-button type="text" @click="changeShow">{{showContent?$t('hide'):$t('edit')}}</el-button>
          </el-col>
        </el-row>
      </div>
      <div v-show="showContent">
        <slot name="content">
        </slot>
        <el-row class="row_padding cardFooter">
          <el-col :span="2" class="ksd-left ksd-lineheight-20" :offset="1" >
            <el-button type="primary" size="small" @click="save">Save</el-button>
          </el-col>
          <el-col :span="2" class="ksd-left ksd-lineheight-20">
            <el-button size="small" @click="changeShow">Cancel</el-button>
          </el-col>
        </el-row>
      </div>
    </el-card>
  </div>
</template>
<script>
export default {
  name: 'manage_card',
  props: ['title', 'hasCheck'],
  data () {
    return {
      showContent: false
    }
  },
  methods: {
    changeShow () {
      this.showContent = !this.showContent
    },
    save () {
      this.$emit('save')
    }
  },
  locales: {
    'en': {edit: 'Edit', hide: 'Hide'},
    'zh-cn': {edit: '编辑', hide: '隐藏'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
 #manageCard {
  margin:20px 20px 20px 20px;
  .el-card {
    background-color: #2d3140;
    border: 1px solid #4f5473;
    .el-card__header {
      padding: 5px 30px 5px 9px;
      border-bottom: none;
      .check_box {
        line-height: 33px;
      }
      h2 {
        font-size: 14px;
        display: inline;
      }
      .el-button--text {
        height: 100%;
        float: right;
        span {
          color: #20a0ff;
        }
      }
    }
    .el-card__body {
      padding: 0px;
    }
  }
  .el-button {
    span {
      font-size: 14px;
      font-weight:bold;
    }
  }
  .row_padding {
    padding: 5px 30px 5px 8px;
  }
  .cardFooter {
    margin: 10px 0px 10px 0px;
  }
 }
</style>
