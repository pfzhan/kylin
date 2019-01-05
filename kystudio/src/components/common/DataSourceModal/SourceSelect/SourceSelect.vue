<template>
  <div class="source-new">
    <ul class="ksd-center">
      <li class="datasouce" :class="getSourceClass([sourceTypes.HIVE])">
        <div class="datasource-icon" @click="clickHandler(sourceTypes.HIVE)" v-guide.selectHive>
          <i class="el-icon-ksd-hive"></i>
        </div>
        <div class="datasource-name">Hive</div>
      </li>
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-mysql"></i>
        </div>
        <div class="datasource-name">MySQL</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-kafka"></i>
        </div>
        <div class="datasource-name">Kafka</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
    </ul>
    <ul class="ksd-center">
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-greenplum"></i>
        </div>
        <div class="datasource-name">Greenplum</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-sqlserver"></i>
        </div>
        <div class="datasource-name">SQL Server</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-csv"></i>
        </div>
        <div class="datasource-name">CSV</div>
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
  padding: 60px 0;
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
      color: @text-secondary-color;
      cursor: not-allowed;
      background: @grey-3;
    }
    .datasource-name {
      color: @text-secondary-color;
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
    margin-bottom: 10px;
    font-size: 16px;
  }
  .status {
    background: @text-placeholder-color;
    border-radius: 12px;
    overflow: hidden;
    color: @fff;
    font-size: 14px;
    width: 90px;
    display: inline-block;
    line-height: 24px;
  }
}
</style>
