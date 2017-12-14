<template>
<div>
  <el-steps :active="activeStep"  finish-status="finish" process-status="wait" center align-center>
    <el-step title="Model Info" @click.native="step(1)"></el-step>
    <el-step title="Data Mode" @click.native="step(2)"></el-step>
    <el-step title="Dimensions"  @click.native="step(3)"></el-step>
    <el-step title="Measures" @click.native="step(4)"></el-step>
    <el-step title="Settings" @click.native="step(5)"></el-step>
  </el-steps>

  <el-card class="box-card" v-if="activeStep===1" >
  <el-row>
    <el-col :span="24"><div class="grid-content bg-purple-dark">Model Name <i class="el-icon-warning"></i>: {{model.name}}</div></el-col>
  </el-row>
  <el-row>
    <el-col :span="24"><div class="grid-content bg-purple-dark">Description :</div></el-col>
  </el-row>  
</el-card>

  <el-card class="box-card" v-if="activeStep===2">
  <el-row>
    <el-col :span="24"><div class="grid-content bg-purple-dark">Fact Table : {{model.name}}</div></el-col>
  </el-row>
  <el-table v-if="model.lookups.length>0"
    :data="model.lookups"
    stripe
    style="width: 100%">
    <el-table-column
      label="ID"
      width="180">
    </el-table-column>
    <el-table-column
      prop="alias"
      label="Table Alias"
      width="180">
    </el-table-column>
    <el-table-column
      prop="table"
      label="Table Name">
    </el-table-column>
    <el-table-column
      label="Kind">
        <template slot-scope="scope">
          <el-tag
          v-if="scope.row.kind=='FACT'"
          type="primary"
          close-transition>Limited</el-tag>
          <el-tag
          type="primary" 
          v-else
          close-transition>Normal</el-tag>
        </template>
    </el-table-column>
    <el-table-column
      label="Join">
      <template slot-scope="scope">
        <el-tag
          type="primary" 
          close-transition>{{scope.row.join.type}}</el-tag>
      </template>          
    </el-table-column>
    <el-table-column
      prop="join.primary_key"
      label="Join Condition">
    </el-table-column>
  </el-table>
  </el-card>

  <el-card v-if="activeStep===3">



  </el-card>
  
  <el-card v-if="activeStep===4">


  </el-card>
  
  <el-card v-if="activeStep===5">

  
  </el-card>  
</div>
</template>
<script>
export default {
  name: 'modelview',
  props: ['model'],
  data () {
    return {
      activeStep: 1
    }
  },
  methods: {
    step: function (num) {
      this.activeStep = num
    }
  }
}
</script>
<style scoped="">
.box-card {
  width: 90%
}
</style>
