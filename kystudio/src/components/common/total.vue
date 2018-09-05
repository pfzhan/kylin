<template>  
  <el-card :body-style="{ height: '100%' }" class="box-card" >
    <div slot="header" class="clearfix" >
      <span style="line-height: 20px;">Total {{name}}</span>
    </div>
    <div class="text item">
      <span>{{projectList.length}}</span>
    </div>
  </el-card>
</template>
<script>
import { mapActions } from 'vuex'
export default {
  name: 'total',
  props: ['name'],
  methods: {
    ...mapActions({
      loadProjects: 'LOAD_PROJECT_LIST'
    })
  },
  computed: {
    projectList () {
      return this.$store.state.project.projectList
    }
  },
  created () {
    this.loadProjects()
  }
}
</script>
<style>
  .text {
    font-size: 160px;
  }

  .item {
    padding: 18px 0;
  }
  .text span {
    text-align: center;
    display: block;
  }
  .box-card {
    width: 25%;
  }
</style>
