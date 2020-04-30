<template>
  <div class="modals">
    <UserEditModal />
    <BatchLoadModal />
    <DataSourceModal />
    <ProjectEditModal />
    <SourceTableModal :project-name="currentSelectedProject" />
    <ModelsExportModal />
    <ModelsImportModal />
    <ModelERDiagramModal />
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
import ModelsExportModal from '../ModelsExportModal/ModelsExportModal.vue'
import ModelsImportModal from '../ModelsImportModal/ModelsImportModal.vue'
import ModelERDiagramModal from '../ModelERDiagramModal/ModelERDiagramModal'

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
    ProjectEditModal,
    ModelsExportModal,
    ModelsImportModal,
    ModelERDiagramModal
  }
})
export default class Modal extends Vue {
}
</script>
