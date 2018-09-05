<template>
  <div>
    <UserEditModal />
    <DataSourceModal />
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

@Component({
  components: {
    UserEditModal: () => import('../UserEditModal/index.vue'),
    DataSourceModal: () => import('../DataSourceModal/index.vue')
  }
})
export default class Modal extends Vue {
}
</script>
