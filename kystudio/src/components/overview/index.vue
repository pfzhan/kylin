<template>
  <div id="overview">
    <el-row :gutter="20">
      <el-col :span="16">
        <el-card class="statistics">
          <div slot="header"><i class="el-icon-ksd-total_query_statistics"></i> {{$t('statistics')}}</div>
          <div class="statistics-body">
            <!-- <img src="../../assets/img/overview.png" height="700" width="750"> -->
            <div class="statistics-center">
              <el-row class="datasource-layer">
                <el-col :span="8">
                  <img src="../../assets/img/bi-dashboard.png" height="14" width="14">
                  BI Dashboard
                </el-col>
                <el-col :span="8">
                  <img src="../../assets/img/chart.png" height="14" width="14">
                  Reporting Tool
                </el-col>
                <el-col :span="8">
                  <img src="../../assets/img/sql.png" height="14" width="14">
                  Rest API
                </el-col>
              </el-row>
              <div class="statistics-img1">
                <img src="../../assets/img/renderUp.gif" height="63" width="48">
                <div class="data-flow"><span>{{statisticObj.amount}}</span><span>Query / Day</span></div>
              </div>
              <div class="kap-layer">Kyligence Enterprise</div>
              <div class="gif-layer">
                <img src="../../assets/img/renderDown.gif" height="240" width="570">
              </div>
              <el-row class="analysis-layer">
                <el-col :span="12" class="analysis-before">
                  <el-row>
                     <el-col :span="12">
                        <div class="percent"><span>{{statisticObj['RDBMS'].ratio * 100}}</span><span>%</span></div>
                        <div><img src="../../assets/img/RDBMs.png" height="118" width="136"></div>
                        <div class="type">RDBMS</div>
                        <div class="cost-time">
                          <img src="../../assets/img/cost_time.png" height="15" width="20">
                          <span>{{statisticObj['RDBMS'].mean / 1000 | fixed(2) || '0.00'}}</span><span>s</span>
                        </div>
                     </el-col>
                     <el-col :span="12">
                       <div class="percent"><span>{{statisticObj['HIVE'].ratio * 100}}</span><span>%</span></div>
                        <div><img src="../../assets/img/Hive.png" height="118" width="136"></div>
                        <div class="type">Hive</div>
                        <div class="cost-time">
                          <img src="../../assets/img/cost_time.png" height="15" width="20">
                          <span>{{statisticObj['HIVE'].mean / 1000 | fixed(2) || '0.00'}}</span><span>s</span>
                        </div>
                     </el-col>
                  </el-row>
                </el-col>
                <el-col :span="1"></el-col>
                <el-col :span="11" class="analysis-after">
                  <el-row>
                     <el-col :span="12">
                       <div class="percent"><span>{{statisticObj['Agg Index'].ratio * 100}}</span><span>%</span></div>
                        <div><img src="../../assets/img/cube.png" height="118" width="136"></div>
                        <div class="type">Aggregate Index</div>
                        <div class="cost-time">
                          <img src="../../assets/img/cost_time.png" height="15" width="20">
                          <span>{{statisticObj['Agg Index'].mean / 1000 | fixed(2) || '0.00'}}</span><span>s</span>
                        </div>
                     </el-col>
                     <el-col :span="12">
                       <div class="percent"><span>{{statisticObj['Table Index'].ratio * 100}}</span><span>%</span></div>
                        <div><img src="../../assets/img/table_index.png" height="118" width="136"></div>
                        <div class="type">Tabel Index</div>
                        <div class="cost-time">
                          <img src="../../assets/img/cost_time.png" height="15" width="20">
                          <span>{{statisticObj['Table Index'].mean / 1000 | fixed(2) || '0.00'}}</span><span>s</span>
                        </div>
                     </el-col>
                  </el-row>
                </el-col>
              </el-row>
            </div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card class="messages">
          <div slot="header">
            <i class="el-icon-ksd-message"></i> <router-link :to="'messages'" class="view-more">{{$t('messages')}}(10)</router-link>
          </div>
          <div class="messages-body">
            <section data-scrollbar id="message_scroll_box">
              <div v-for="o in messageList" :key="o.timestamp" class="message-item">
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
            </section>
          </div>
        </el-card>
        <router-link :to="'messages'" class="view-more"><div class="view-more-btn">{{$t('viewAll')}}</div></router-link>
        <el-card class="documentation ksd-mt-20">
          <div slot="header"><i class="el-icon-ksd-document"></i> {{$t('documentation')}}</div>
          <div class="documentation-body">
            <div v-for="o in manualList" :key="o.title" class="item">
              <a :href="o.link" target="_blank">{{$t(o.title)}}</a>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import Scrollbar from 'smooth-scrollbar'
import { handleSuccess, handleError } from '../../util/business'
@Component({
  methods: {
    ...mapActions({
      // loadProjects: 'LOAD_ALL_PROJECT',
      // getCubesList: 'GET_CUBES_LIST',
      // loadModels: 'LOAD_MODEL_LIST',
      // loadJobsList: 'LOAD_JOBS_LIST',
      // loadUsersList: 'LOAD_USERS_LIST',
      loadStatistics: 'LOAD_STATISTICS'
    })
  },
  locales: {
    'en': {'kapManual': 'Kyligence Enterprise Manual', 'kylinManual': 'Apache Kylin Document', 'statistics': 'Total Query Statistics', 'messages': 'Messages', 'documentation': 'Documentation', 'viewAll': 'View All'},
    'zh-cn': {'kapManual': 'Kyligence Enterprise 操作手册', 'kylinManual': 'Apache Kylin 文档', 'statistics': '查询流量图', 'messages': '消息盒子', 'documentation': '使用文档', 'viewAll': '查看更多'}
  }
})
export default class Overview extends Vue {
  sliderImgs = [
    {index: 0, src: require('../../assets/img/banner.png')},
    {index: 1, src: require('../../assets/img/banner.png')},
    {index: 2, src: require('../../assets/img/banner.png')},
    {index: 3, src: require('../../assets/img/banner.png')}
  ]
  blogsList = [
    {id: 0, title: 'Query returns incorrect date via JDBC', time: '3/14/2017', link: 'https://kybot.io/kybot/search_detail/115003630227'},
    {id: 0, title: 'How to clean up hive temporary tables', time: '3/14/2017', link: 'https://kybot.io/kybot/search_detail/115004004868'},
    {id: 0, title: 'What latency should I expect while streaming from Kafka?', time: '3/14/2017', link: 'https://kybot.io/kybot/search_detail/115003632207'},
    {id: 0, title: 'Size of table snapshot exceeds the limitation', time: '3/14/2017', link: 'https://kybot.io/kybot/search_detail/115003988308'}
  ]
  manualList = [
    {id: 0, title: 'kapManual', time: '3/14/2017', link: 'http://docs.kyligence.io'},
    {id: 0, title: 'kylinManual', time: '3/14/2017', link: 'http://kylin.apache.org/docs20/'}
  ]
  messageList = [
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
  statisticObj = {
    amount: 0,
    'RDBMS': {ratio: 0, mean: 0},
    'HIVE': {ratio: 0, mean: 0},
    'Agg Index': {ratio: 0, mean: 0},
    'Table Index': {ratio: 0, mean: 0}
  }

  handleCommand (command) {}
  mounted () {
    this.$nextTick(() => {
      Scrollbar.init(document.getElementById('message_scroll_box'), {continuousScrolling: true})
    })
  }
  created () {
    // var params = {pageSize: 1, pageOffset: 0, projectName: this.$store.state.project.selected_project}
    // this.loadProjects({
    //   ignoreAccess: true
    // })
    // for newten
    // this.getCubesList(params)
    // this.loadModels(params)
    // this.loadJobsList({pageSize: 1, pageOffset: 0, timeFilter: 4, projectName: this.$store.state.project.selected_project})
    // if (this.isAdmin) {
    //   this.loadUsersList(params)
    // } else {
    //   this.$store.state.user.usersSize = 0
    // }

    this.loadStatistics({start_time: new Date().setHours(0, 0, 0, 0), end_time: new Date().setHours(23, 59, 59, 999)}).then((res) => {
      handleSuccess(res, (data) => {
        this.statisticObj = data
      })
    }, (errResp) => {
      handleError(errResp)
    })
  }
}
</script>

<style lang="less">
  @import "../../assets/styles/variables.less";
  #overview{
	  margin: 40px 30px;
    .el-card__header {
      font-weight: 500;
    }
    .statistics {
      height: 843px;
      .statistics-body {
        text-align: center;
        padding: 30px 0;
        .statistics-center {
          width: 700px;
          height: 700px;
          margin: 0 auto;
          // border: 1px solid @line-border-color;
          .datasource-layer {
            margin: 0 auto;
            width: 450px;
            height: 55px;
            line-height: 55px;
            border: 1px solid @line-border-color;
            background-color: @aceditor-bg-color;
            font-size: 16px;
            overflow: hidden;
            .el-col {
              text-align: center;
            }
          }
          .statistics-img1 {
            text-align: center;
            position: relative;
            .data-flow {
              position: absolute;
              left: 55%;
              top: 20px;
              font-weight: bold;
              color: @base-color;
              span:first-child {
                font-size: 24px;
              }
              span:last-child {
                font-size: 16px;
                margin-left: 10px;
              }
            }
          }
          .kap-layer {
            margin: 0 auto;
            width: 450px;
            height: 55px;
            line-height: 55px;
            font-size: 16px;
            font-weight: bold;
            color: @base-color;
            border: 1px solid @base-color;
            background-color: @base-color-9;
          }
          .analysis-layer {
            margin: 0 auto;
            width: 700px;
            height: 270px;
            overflow: hidden;
            .percent {
              height: 40px;
              span:first-child {
                font-size: 24px;
                font-weight: bold;
              }
              span:last-child {
                font-size: 16px;
              }
            }
            .type {
              font-size: 16px;
              height: 20px;
              margin-bottom: 15px;
              font-weight: bold;
            }
            .cost-time {
              font-weight: bold;
              border: 2px solid @line-border-color;
              border-radius: 15px;
              height: 30px;
              line-height: 30px;
              width: 85px;
              text-align: center;
              margin: 0 auto;
              padding-right: 8px;
              img {
                position: relative;
                top: 4px;
              }
              span:first-child {
                font-size: 16px;
              }
              span:last-child {
                font-size: 12px;
              }
            }
            .analysis-before {
              height: 270px;
              padding: 15px 22px;
            }
            .analysis-after {
              padding: 15px 10px;
              height: 270px;
              margin-left: 29px;
              border: 1px solid @base-color;
              background-color: @base-color-9;
              color: @base-color;
              .cost-time {
                border: 2px solid @base-color;
                background-color: @fff;
              }
            }
          }
        }
      }
    }
    .view-more {
      &:hover {
        text-decoration: none;
      }
    }
    .view-more-btn {
      height: 30px;
      line-height: 30px;
      text-align: center;
      font-size: 12px;
      background-color: @table-stripe-color;
      border: 1px solid @line-border-color;
      cursor: pointer;
      color: @text-title-color;

      &:hover {
        border-color: @base-color;
        color: @base-color;
        background-color: @fff;
      }
    }
    .messages {
      height: 460px;
      position: relative;
      border-bottom: none;
      .el-card__header .view-more {
        color: @text-title-color;
        &:hover {
          color: @link-color;
        }
      }
      #message_scroll_box {
        height: 460px;
        .message-item {
          padding: 12px 20px;
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
            padding: 0 45px 0 8px;
            display: table-cell;
            word-break: break-word;
            a {
              font-size: 14px;
              line-height: 20px;
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
            top: 4px;
            position: relative;
            &.el-icon-ksd-good-health {
              color: @color-success;
            }
            &.el-icon-ksd-error_01 {
              color: @color-danger;
            }
            &.el-icon-warning {
              color: @color-warning;
            }
          }
        }
      }
    }
    .documentation-body {
      .item {
        padding: 15px 20px;
        border-bottom: 1px solid @line-border-color;
        &:hover {
          background-color: @base-color-9;
        }
        a {
          color: @text-title-color;
          &:hover {
            color: @link-hover-color;
          }
        }
      }
    }
    .documentation {
      height: 330px;
    }
  }
</style>
