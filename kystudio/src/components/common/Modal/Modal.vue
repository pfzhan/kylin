<template>
  <div class="modals">
    <UserEditModal />
    <BatchLoadModal />
    <DataSourceModal />
    <ProjectEditModal />
    <SourceTableModal :project-name="currentSelectedProject" />
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'
import UserEditModal from '../UserEditModal/index.vue'
import BatchLoadModal from '../BatchLoadModal/BatchLoadModal.vue'
import ProjectEditModal from '../ProjectEditModal/ProjectEditModal.vue'
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
    BatchLoadModal,
    DataSourceModal,
    SourceTableModal,
    ProjectEditModal
  }
})
export default class Modal extends Vue {
}
</script>
