<template>
  <div class="capacity-top-bar">
    <el-popover ref="activeNodes" width="290" popper-class="nodes-popover" v-model="showNodes">
      <div class="contain">
        <div class="lastest-update-time">
          <el-tooltip :content="$t('lastUpdateTime')" effect="dark" placement="top"><i class="icon el-icon-ksd-type_time"></i></el-tooltip>{{latestUpdateTime | timeFormatHasTimeZone}}</div>
        <div class="data-valumns">
          <p class="label">
            <span>{{$t('usedData')}}：
              <i v-if="systemCapacityInfo.isLoading" class="el-icon-loading"></i>
              <span :class="['font-medium', getValueColor]" v-else-if="!systemCapacityInfo.fail">{{!systemCapacityInfo.evaluation ? getCapacityPrecent + '%' : filterElements.dataSize(systemCapacityInfo.current_capacity)}}</span>
            </span>
            <template v-if="!systemCapacityInfo.isLoading">
              <span class="font-disabled" v-if="systemCapacityInfo.fail">{{$t('failApi')}}</span>
              <span v-if="systemCapacityInfo.error_over_thirty_days">
                <el-tooltip :content="$t('failedTagTip')" effect="dark" placement="top">
                  <el-tag class="over-thirty-days" size="mini" type="danger">{{$t('failApi')}}<i class="is-danger el-icon-ksd-what ksd-ml-5"></i></el-tag>
                </el-tooltip>
              </span>
              <el-tag size="mini" type="danger" v-if="systemCapacityInfo.capacity_status === 'OVERCAPACITY'">{{$t('excess')}}</el-tag>
            </template>
          </p>
          <p :class="['label', 'node-item', {'is-disabled': systemNodeInfo.isLoading || systemNodeInfo.fail}]" @mouseenter="showNodeDetails = true" @mouseleave="showNodeDetails = false">
            <span>{{$t('usedNodes')}}：<i v-if="systemNodeInfo.isLoading" class="el-icon-loading"></i><span :class="['font-medium', {'is-danger': systemNodeInfo.current_node > systemNodeInfo.node && !systemNodeInfo.evaluation}]" v-else-if="!systemNodeInfo.fail">{{!systemNodeInfo.evaluation ? `${systemNodeInfo.current_node}/${systemNodeInfo.node}` : systemNodeInfo.current_node}}</span></span>
            <template v-if="!systemNodeInfo.isLoading && isNodeLoadingSuccess">
              <span class="font-disabled" v-if="systemNodeInfo.fail">{{$t('failApi')}}</span>
              <!-- <el-tooltip :content="$t('failedTagTip')" effect="dark" placement="top">
                <el-tag size="mini" type="danger">{{$t('failApi')}}</el-tag>
              </el-tooltip> -->
              <el-tag size="mini" type="danger" v-if="isOnlyQueryNode">{{$t('noActiveAllNode')}}</el-tag>
              <el-tag size="mini" type="danger" v-if="systemNodeInfo.node_status === 'OVERCAPACITY'">{{$t('excess')}}</el-tag>
            </template>
            <span class="icon el-icon-ksd-more_02 node-list-icon"></span></p>
        </div>
      </div>
      <div class="nodes" v-if="showNodeDetails">
        <!-- <p class="error-text" v-if="!nodeList.filter(it => it.mode === 'all').length">{{$t('noNodesTip1')}}</p> -->
        <ul v-if="isNodeLoadingSuccess && !isNodeLoading" class="node-details"><li class="node-list" v-for="(node, index) in nodeList.map(item => `${item.host}(${item.mode})`)" :key="index">{{node}}</li></ul>
      </div>
    </el-popover>
    <p class="active-nodes" v-popover:activeNodes @click="showNodes = !showNodes"><span class="server-status">{{$t('serverStatus')}}</span>
      <i v-if="showLoadingStatus" class="el-icon-loading"></i>
      <template v-else>
        <span :class="['flag', getNodesNumColor]" v-if="getDataFails"></span>
        <template v-else>
          <span class="font-disabled">{{$t('failApi')}}</span>
          <el-tooltip :content="$t('refresh')" effect="dark" placement="top">
            <i class="icon el-icon-ksd-restart" v-if="isAdminRole" @click.stop="refreshCapacityOrNodes"></i>
          </el-tooltip>
        </template>
      </template>
    </p>
  </div>
</template>
<script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { mapActions, mapState, mapGetters } from 'vuex'
  import locales from './locales'
  import filterElements from '../../../filter/index'
  import { handleError } from '../../../util/business'

  @Component({
    methods: {
      ...mapActions({
        getNodeList: 'GET_NODES_LIST',
        getNodesInfo: 'GET_NODES_INFO',
        getSystemCapacity: 'GET_SYSTEM_CAPACITY_INFO',
        refreshAll: 'REFRESH_ALL_SYSTEM',
        resetCapacityData: 'RESET_CAPACITY_DATA'
      })
    },
    computed: {
      ...mapState({
        systemNodeInfo: state => state.capacity.systemNodeInfo,
        systemCapacityInfo: state => state.capacity.systemCapacityInfo,
        latestUpdateTime: state => state.capacity.latestUpdateTime
      }),
      ...mapGetters([
        'isOnlyQueryNode',
        'isAdminRole'
      ])
    },
    locales
  })
  export default class CapacityTopBar extends Vue {
    showNodes = false
    isNodeLoadingSuccess = false
    nodeList = []
    isNodeLoading = true
    showNodeDetails = false
    filterElements = filterElements
    nodesTimer = null

    get getDataFails () {
      return this.systemCapacityInfo.capacity_status === 'OVERCAPACITY' || this.systemNodeInfo.node_status === 'OVERCAPACITY' || this.systemCapacityInfo.error_over_thirty_days || this.isOnlyQueryNode || this.getCapacityPrecent >= 80 || (!this.systemCapacityInfo.fail && !this.systemNodeInfo.fail)
    }

    get getNodesNumColor () {
      if (this.systemCapacityInfo.capacity_status === 'OVERCAPACITY' || this.systemCapacityInfo.error_over_thirty_days || this.systemNodeInfo.node_status === 'OVERCAPACITY' || this.isOnlyQueryNode) {
        return 'is-danger'
      } else if (this.getCapacityPrecent >= 80) {
        return 'is-warning'
      } else {
        return 'is-success'
      }
    }

    get getValueColor () {
      const num = this.getCapacityPrecent
      if (num > 100) {
        return 'is-danger'
      } else if (num >= 80 && num <= 100) {
        return 'is-warning'
      } else {
        return ''
      }
    }

    get getCapacityPrecent () {
      return this.systemCapacityInfo.capacity === 0 || this.systemCapacityInfo.evaluation ? 0 : (this.systemCapacityInfo.current_capacity / this.systemCapacityInfo.capacity * 100).toFixed(2)
    }

    get showLoadingStatus () {
      return this.systemCapacityInfo.capacity_status !== 'OVERCAPACITY' && !this.systemCapacityInfo.error_over_thirty_days && this.systemNodeInfo.node_status !== 'OVERCAPACITY' && this.getCapacityPrecent < 80 && (this.systemCapacityInfo.isLoading || this.systemNodeInfo.isLoading || this.isNodeLoading)
    }

    created () {
      this.getHANodes()
      this.getNodesInfo()
      this.getSystemCapacity()
    }

    getHANodes () {
      if (this._isDestroyed) {
        return
      }
      this.isNodeLoading = true
      const data = {ext: true}
      if (this.nodesTimer) {
        data.isAuto = true
      }
      this.getNodeList(data).then((res) => {
        if (this._isDestroyed) {
          return
        }
        this.isNodeLoadingSuccess = true
        this.$set(this, 'nodeList', res.servers)
        this.isNodeLoading = false
        clearTimeout(this.nodesTimer)
        this.nodesTimer = setTimeout(() => {
          this.getHANodes()
        }, 1000 * 60)
      }).catch((e) => {
        if (e.status === 401) {
          handleError(e)
        } else {
          clearTimeout(this.nodesTimer)
          this.timer = setTimeout(() => {
            this.getHANodes()
          }, 1000 * 60)
        }
      })
    }

    // 刷新获取失败的接口
    async refreshCapacityOrNodes () {
      this.isNodeLoading = true
      if (this.systemCapacityInfo.fail) {
        await this.refreshAll()
        this.isNodeLoading = false
      }
      if (this.systemNodeInfo.fail) {
        await this.getNodesInfo()
        this.isNodeLoading = false
      }
    }

    beforeDestroy () {
      this.resetCapacityData()
    }
  }
</script>
<style lang="less" scoped>
  @import '../../../assets/styles/variables.less';
  
  .capacity-top-bar {
    position: relative;
    min-width: 75px;
    // padding-right: 20px;
    .active-nodes {
      position: relative;
      .server-status {
        font-weight: bold;
      }
      .flag {
        width: 10px;
        height: 10px;
        // position: absolute;
        display: inline-block;
        border-radius: 100%;
        &.is-danger {
          background-color: @error-color-1;
        }
        &.is-warning {
          background-color: @warning-color-1;
        }
        &.is-success {
          background-color: @normal-color-1;
        }
      }
      .el-icon-ksd-restart {
        color: @base-color;
      }
    }
    .font-disabled {
      color: @text-disabled-color;
    }
  }
  .is-danger {
    color: @error-color-1;
  }
  .is-warning {
    color: @warning-color-1;
  }
  .is-success {
    color: @normal-color-1;
  }
  .error-text {
    color: @error-color-1;
    font-size: 12px;
    margin-bottom: 13px;
  }
</style>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  .nodes-popover {
    padding: 0 !important;
    margin-left: -50px;
    .popper__arrow {
      margin-left: 50px;
    }
    .font-disabled {
      color: @text-disabled-color;
    }
    .contain {
      .lastest-update-time {
        height: 30px;
        padding: 5px 10px;
        line-height: 20px;
        box-sizing: border-box;
        color: @text-normal-color;
        border-bottom: 1px solid @line-border-color3;
        .icon {
          margin-right: 5px;
          color: @text-disabled-color;
        }
      }
      .data-valumns {
        padding: 10px 0;
        box-sizing: border-box;
        .label {
          line-height: 28px;
          padding: 0 10px;
          box-sizing: border-box;
          .over-thirty-days {
            cursor: pointer;
          }
        }
        .node-item {
          position: relative;
          cursor: pointer;
          .node-list-icon {
            position: absolute;
            right: 10px;
            top: 10px;
            font-size: 9px;
          }
          &:hover {
            background: @base-color-9;
          }
          &.is-disabled {
            pointer-events: none;
          }
        }
      }
    }
    .nodes {
      max-height: 170px;
      overflow: auto;
      text-align: left;
      position: absolute;
      background: #ffffff;
      left: calc(~'100% + 5px');
      margin-top: -35px;
      width: 280px;
      padding: 10px;
      box-sizing: border-box;
      box-shadow: 0 0px 6px 0px #E5E5E5;
      .node-details {
        text-align: left;
      }
      .node-list {
        color: @text-normal-color;
        margin-top: 8px;
        &:first-child {
          margin-top: 0;
        }
      }
    }
  }
</style>
