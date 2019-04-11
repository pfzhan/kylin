<template>
  <div class="source-new">
    <ul>
      <li class="datasouce ksd-center" :class="getSourceClass([sourceTypes.HIVE])">
        <div class="datasource-icon" @click="clickHandler(sourceTypes.HIVE)" v-guide.selectHive>
          <i class="el-icon-ksd-hive"></i>
        </div>
        <div class="datasource-name">Hive</div>
      </li>
      <li class="datasouce ksd-center disabled">
        <!-- 暂时屏蔽该功能 -->
        <!-- <li class="datasouce ksd-center disabled" :class="getSourceClass([sourceTypes.CSV])"> -->
        <!-- <div class="datasource-icon" @click="clickHandler(sourceTypes.CSV)"> -->
        <div class="datasource-icon">
          <i class="el-icon-ksd-csv"></i>
        </div>
        <div class="datasource-name">CSV</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled ksd-center">
        <div class="datasource-icon">
          <i class="el-icon-ksd-mysql"></i>
        </div>
        <div class="datasource-name">MySQL</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled ksd-center">
        <div class="datasource-icon">
          <i class="el-icon-ksd-kafka"></i>
        </div>
        <div class="datasource-name">Kafka</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
    </ul>
    <ul>
      <li class="datasouce disabled ksd-center">
        <div class="datasource-icon">
          <i class="el-icon-ksd-greenplum"></i>
        </div>
        <div class="datasource-name">Greenplum</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled ksd-center">
        <div class="datasource-icon">
          <i class="el-icon-ksd-sqlserver"></i>
        </div>
        <div class="datasource-name">SQL Server</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
    </ul>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { sourceTypes } from '../../../../config'

@Component({
  props: [ 'sourceType' ],
  computed: {
    ...mapGetters([
      'globalDefaultDatasource'
    ])
  },
  locales
})
export default class SourceSelect extends Vue {
  sourceTypes = sourceTypes

  getSourceClass (sourceTypes = []) {
    return {
      active: sourceTypes.includes(this.sourceType)
    }
  }

  clickHandler (value = '') {
    this.$emit('input', value)
  }

  mounted () {
    // 设置默认数据源
    // this.clickHandler(this.globalDefaultDatasource)
    // for newten 设置CSV为默认数据源
    // this.clickHandler(sourceTypes.HIVE)
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.source-new {
  padding: 60px 154px;
  ul {
    margin-bottom: 35px;
    &:last-child {
      margin-bottom: 0px;
    }
  }
  .datasouce {
    display: inline-block;
    height: 150px;
    width: 90px;
    vertical-align: top;
    color: @text-secondary-color;
    margin-right: 30px;
    border: 1px solid transparent;
    * {
      vertical-align: middle;
    }
    &:last-child {
      margin-right: 0;
    }
  }
  .datasource-icon:hover {
    border-color: @base-color;
  }
  .datasouce.active {
    color: @text-normal-color;
    .datasource-icon {
      color: @fff;
      background: @base-color;
      box-shadow: 2px 2px 4px 0 @text-placeholder-color;
    }
    .datasource-name {
      color: @base-color;
    }
  }
  .datasouce.disabled {
    .datasource-icon {
      color: @text-disabled-color;
      cursor: not-allowed;
      background: @background-disabled-color;
    }
    .datasource-name {
      color: @text-disabled-color;
      font-weight: normal;
    }
    &:hover {
      .datasource-icon {
        border-color: transparent;
      }
    }
    * {
      cursor: inherit;
    }
  }
  .datasource-icon {
    font-size: 65px;
    height: 90px;
    border-radius: 6px;
    background: @grey-4;
    margin-bottom: 10px;
    color: @base-color;
    cursor: pointer;
    border: 1px solid transparent;
  }
  .datasource-name {
    color: @text-normal-color;
    margin-bottom: 5px;
    font-size: 14px;
    font-weight: bold;
  }
  .status {
    background: @background-disabled-color;
    border-radius: 12px;
    overflow: hidden;
    color: @text-disabled-color;
    font-size: 12px;
    width: 90px;
    display: inline-block;
    height: 22px;
    line-height: 22px;
  }
}
</style>
