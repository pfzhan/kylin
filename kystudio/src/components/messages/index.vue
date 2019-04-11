<template>
  <div id="messages">
    <div class="critical-messages">
      <div class="mess-title ksd-mb-20 clearfix">
        <span>Messages (23)</span><el-button plain size="small" class="ksd-left ksd-ml-20" @click="backToDashBoard">{{$t('kylinLang.common.back')}}</el-button>
        <el-button plain size="small" class="ksd-fright ksd-ml-20">{{$t('kylinLang.common.removeAll')}}</el-button>
        <el-button plain size="small" class="ksd-fright">{{$t('kylinLang.common.readedAll')}}</el-button>
      </div>
      <div class="messages-box">
        <div v-for="o in messages" :key="o.timestamp" class="message-item">
          <i :class="{
          'el-icon-ksd-good-health': o.status === 'success',
          'el-icon-ksd-error_01': o.status === 'error',
          'el-icon-warning': o.status === 'warning'}"></i>
          <div class="item-content">
            <div :class="{'readed-color': o.isReaded}">{{o.message}}</div>
            <div class="timestamp ksd-mt-6">{{o.timestamp | gmtTime}}</div>
            <el-button plain size="mini" v-if="!o.isReaded" class="isreded" @click="o.isReaded=true">{{$t('kylinLang.common.unread')}}</el-button>
            <el-button type="info" text size="mini" v-else class="isreded readed-color">{{$t('kylinLang.common.readed')}}</el-button>
          </div>
        </div>
      </div>
      <kap-pager
        class="ksd-center ksd-mtb-10" ref="pager"
        :totalSize="messages.length"
        @handleCurrentChange="handleCurrentChange">
      </kap-pager>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
@Component({
  methods: {
    ...mapActions({})
  }
})
export default class Messages extends Vue {
  currentPage1 = 1
  currentPage2 = 1
  messages = [
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'success', timestamp: 1524829437628, isReaded: false},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'warning', timestamp: 1524829437628, isReaded: false},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'error', timestamp: 1524829437628, isReaded: false},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'success', timestamp: 1524829437628, isReaded: true},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'warning', timestamp: 1524829437628, isReaded: false},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'error', timestamp: 1524829437628, isReaded: false},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'error', timestamp: 1524829437628, isReaded: false},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'warning', timestamp: 1524829437628, isReaded: false},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'error', timestamp: 1524829437628, isReaded: false},
    {message: 'I\'m sorry,  Cube 「test_01_BX」 build errors, please check.', status: 'error', timestamp: 1524829437628, isReaded: false}
  ]

  backToDashBoard () {
    this.$router.push({name: 'Overview'})
  }
  handleCurrentChange (currentPage) {
    this.currentPage1 = currentPage
  }
  pageCurrentChange2 (currentPage) {
    this.currentPage2 = currentPage
  }
  readMessage (row) {
    row.new = false
  }
}
</script>

<style lang="less">
  @import "../../assets/styles/variables.less";
  #messages {
    margin: 0 30px 30px 30px;
    .mess-title {
      height: 20px;
      line-height: 20px;
      color: @text-title-color;
      font-weight: bold;
    }
    .messages-box {
      border: 1px solid @line-border-color;
    }
    .message-item {
      padding: 0 10px;
      position: relative;
      width: 100%;
      box-sizing: border-box;
      overflow: hidden;
      display: flex;
      align-items: normal;
      color: @text-title-color;
      border-bottom: 1px solid @grey-3;
      &:last-child {
        border-bottom: none;
      }
      .readed-color {
        color: @text-disabled-color;
      }
      .item-content {
        padding: 10px 20px 10px 10px;
        display: table-cell;
        word-break: break-word;
        font-size: 14px;
        line-height: 20px;
        .auto-success-type {
          color: @base-color;
        }
        .readed {
          color: @text-secondary-color;
        }
        .timestamp {
          font-size: 12px;
          color: @text-secondary-color;
        }
        .isreded {
          position: absolute;
          bottom: 10px;
          right: 20px;
        }
      }
      i {
        top: 15px;
        position: relative;
        &.el-icon-ksd-good-health {
          color: @color-success;
        }
        &.el-icon-ksd-auto_wizard {
          color: @base-color;
        }
        &.el-icon-ksd-error_01 {
          color: @color-danger;
        }
        &.el-icon-warning {
          color: @color-warning;
        }
      }
    }
    .el-table__expanded-cell {
      background-color: @table-stripe-color;
      padding: 10px 20px;
      font-size: 14px;
      line-height: 24px;
      a {
        color: @link-color;
      }
      &:hover {
        background-color: @table-stripe-color !important;
      }
    }
  }
</style>
