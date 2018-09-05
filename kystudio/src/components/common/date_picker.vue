<template>
<div class="hideTime_pick">
  <el-date-picker ref="datePicker"
  :type="dateType"
  @change="selectDate"
  v-model="timeSelect"
  :value-format="valueFormate"
  placeholder="选择日期时间">
</el-date-picker>
</div>
</template>
<script>
export default {
  name: 'date_pick_trigger',
  props: ['selectList', 'dateType'],
  data () {
    return {
      timeSelect: ''
    }
  },
  methods: {
    selectDate (d) {
      if (d) {
        this.selectList.push(d)
      }
      this.timeSelect = ''
    }
  },
  computed: {
    valueFormate () {
      if (this.dateType === 'date') {
        return 'yyyy-MM-dd'
      } else if (this.dateType === 'datetime') {
        return 'yyyy-MM-dd HH:mm:SS'
      }
    }
  }
}
</script>
<style lang="less">
  // .el-month-table td .cell, .el-year-table td .cell{
  //  color:#fff;
  //  text-decoration: none;
  // }
  // .el-month-table td:not(.disabled) .cell:hover,.el-year-table td:not(.disabled) .cell:hover {
  //  background-color: #20a0ff;
  //  color: #fff;
  //  text-decoration: none;
  // }
  // .el-date-picker__header-label{
  //  color:#fff;
  // }
 .hideTime_pick {
   .el-date-editor {
     position: absolute;
     top: 4px;
     width: 1px;
     right: 30px;
     input {
      display: none;
     }
    .el-icon-time{
      top: 12px;
      right:0px;
    }
    .el-input__icon{
     top: 12px;
     right:0px;
     width:25px;
     height: 25px;
    }
  }
 }
</style>
