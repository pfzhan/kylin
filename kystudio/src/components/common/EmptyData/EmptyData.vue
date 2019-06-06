<template>
  <div class="empty-data" :class="emptyClass">
    <div class="center">
      <img :src="emptyImageUrl" :class="{'large-icon': size === 'large'}"/>
    </div>
    <div class="center">
      <div v-html="emptyContent"></div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import defaultImage from '../../../assets/img/no_data.png'

@Component({
  props: {
    image: {
      type: String
    },
    content: {
      type: String
    },
    size: {
      type: String
    }
  },
  locales
})
export default class EmptyData extends Vue {
  get emptyImageUrl () {
    return this.image || defaultImage
  }
  get emptyContent () {
    return this.content || this.$t('content')
  }
  get emptyClass () {
    return this.size ? 'empty-data-' + this.size : 'empty-data-normal'
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.empty-data {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  color: @text-disabled-color;
  .center {
    text-align: center;
  }
  .center:first-child {
    margin-bottom: 20px;
  }
  &.empty-data-normal {
    img {
      height: 120px;
    }
    font-size: 16px;
  }
  &.empty-data-small {
    img {
      height: 60px;
    }
    font-size: 14px;
    .center:first-child {
      margin-bottom: 10px;
    }
  }
}
</style>
