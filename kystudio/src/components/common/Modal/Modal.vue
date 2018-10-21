<template>
  <div>
    <UserEditModal />
    <DataSourceModal />
    <SourceTableModal :project-name="currentSelectedProject" />
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    UserEditModal: () => import('../UserEditModal/index.vue'),
    DataSourceModal: () => import('../DataSourceModal/index.vue'),
    SourceTableModal: () => import('../SourceTableModal/SourceTableModal.vue')
  }
})
export default class Modal extends Vue {
}
</script>
