<template>
  <div class="empty-data">
    <div class="center">
      <img :src="emptyImageUrl" />
    </div>
    <div class="center">
      <span>{{emptyContent}}</span>
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
}
</script>

<style lang="less">
.empty-data {
  margin: 220px 0;
  // position: absolute;
  // top: 50%;
  // left: 50%;
  // transform: translate(-50%, -50%);
  // .center {
  //   text-align: center;
  // }
  // .center:first-child {
  //   margin-bottom: 20px;
  // }
}
</style>
