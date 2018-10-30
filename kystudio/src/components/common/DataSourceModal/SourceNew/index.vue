<template>
  <div class="source-new">
    <ul class="ksd-center">
      <li class="datasouce" @click="clickHandler(sourceTypes.HIVE)" :class="getSourceClass([sourceTypes.HIVE])">
        <div class="datasource-icon">
          <i class="el-icon-ksd-hive_normal"></i>
        </div>
        <div>Hive</div>
      </li>
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-mysql"></i>
        </div>
        <div>MySQL</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-kafka_normal"></i>
        </div>
        <div>Kafka</div>
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
        <div>Greenplum</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-SQL-server"></i>
        </div>
        <div>SQL Server</div>
        <div class="status">
          <span>{{$t('upcoming')}}</span>
        </div>
      </li>
      <li class="datasouce disabled">
        <div class="datasource-icon">
          <i class="el-icon-ksd-csv"></i>
        </div>
        <div>CSV</div>
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
export default class SourceNew extends Vue {
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
    this.clickHandler(11)
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.source-new {
  padding: 105px 0 115px 0;
  ul {
    margin-bottom: 35px;
    &:last-child {
      margin-bottom: 0px;
    }
  }
  .datasouce {
    display: inline-block;
    height: 130px;
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
    &:hover {
      .datasource-icon {
        border-color: @base-color;
      }
    }
  }
  .datasouce.active {
    color: @text-normal-color;
    .datasource-icon {
      color: @fff;
      background: @base-color;
    }
  }
  .datasouce.disabled {
    .datasource-icon {
      color: @text-secondary-color;
      cursor: not-allowed;
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
  }
  .status {
    background: @text-placeholder-color;
    border-radius: 7.8px;
    overflow: hidden;
    color: @fff;
    font-size: 12px;
    height: 15.6px;
    width: 72px;
    display: inline-block;
    transform: scale(0.83333);
  }
}
</style>
