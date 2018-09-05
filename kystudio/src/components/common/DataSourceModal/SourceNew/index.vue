<template>
  <div class="source-new">
    <p class="ksd-center ksd-mt-40 select-title">
      {{$t('dataSourceTypeCheckTip')}}
    </p>

    <ul class="ksd-center datasource-type">
      <li @click="clickHandler(sourceTypes.HIVE)">
        <div class="type-hive" :class="getSourceClass([sourceTypes.HIVE])"></div>
        <p>Hive</p>
      </li>
      <li @click="clickHandler(sourceTypes.RDBMS)">
        <div class="type-rdbms" :class="getSourceClass([sourceTypes.RDBMS, sourceTypes.RDBMS2])"></div>
        <p>RDBMS</p>
      </li>
      <li @click="clickHandler(sourceTypes.KAFKA)">
        <div class="type-kafka" :class="getSourceClass([sourceTypes.KAFKA])"></div>
        <p>Kafka</p>
      </li>
    </ul>

    <p class="ksd-center ksd-mt-20 checksource-warn-msg">
      {{$t('singleSourceTip')}}
    </p>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { sourceTypes } from '../../../../config'

@Component({
  props: [ 'selectedType' ],
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
      active: sourceTypes.includes(this.selectedType)
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
  .datasource-type{
    margin-top: 40px;
  }
  .datasource-type li {
    display: inline-block;
    margin-right: 15px;
    margin-left: 15px;
    cursor: pointer;
  }
  .checksource-warn-msg {
    margin-bottom: 240px;
  }
  .type-hive,
  .type-rdbms,
  .type-kafka {
    width: 80px;
    height: 80px;
    background-color: @grey-4;
    background-repeat:no-repeat;
    background-position: center;
    margin-bottom: 10px;
    background-size: 50%;
  }
  .type-hive {
    background-image: url('../../../../assets/img/datasource/hive_blue.png');
    background-size: 60%;
    &.active{
      background-color: @base-color;
      background-image: url('../../../../assets/img/datasource/hive_white.png');
    }
  }
  .type-rdbms {
    background-image: url('../../../../assets/img/datasource/rdbms_blue.png');
    &.active{
      background-color: @base-color;
      background-image: url('../../../../assets/img/datasource/rdbms_white.png');
    }
  }
  .type-kafka {
    background-image: url('../../../../assets/img/datasource/kafka_blue.png');
    &.active{
      background-color: @base-color;
      background-image: url('../../../../assets/img/datasource/kafka_white.png');
    }
  }
}
</style>
