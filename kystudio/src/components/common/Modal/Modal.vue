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
import UserEditModal from '../UserEditModal/index.vue'
import DataSourceModal from '../DataSourceModal/index.vue'
import SourceTableModal from '../SourceTableModal/SourceTableModal.vue'

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    UserEditModal,
    DataSourceModal,
    SourceTableModal
  }
})
export default class Modal extends Vue {
}
</script>
