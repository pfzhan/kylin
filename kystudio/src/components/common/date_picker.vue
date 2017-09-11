<template>
<div class="hideTime_pick">
  <el-date-picker ref="datePicker"
  :type="dateType"
  @change="selectDate"
  v-model="timeSelect"
  placeholder="选择日期时间">
</el-date-picker>
</div>
</template>
<script>
export default {
  name: 'about_kap',
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
      // console.log(this.$refs.datePicker.showPicker())
    }
  }
}
</script>
<style lang="less">
 .available{
    color: #fff;
   }
   .prev-month{
    color:#48576a;
   }
   .next-month{
    color:#48576a;
   }
  .el-date-picker__header-label{
    color:#fff;
  }
 .hideTime_pick {

   .el-date-editor {
    position: absolute;
    top: 4px;
    width: 1px;
    right: 0;
    input {
      display: none;
    }
    // .el-icon-time{
    //   top: 12px;
    //   right:0px;
    // }
    .el-input__icon{
      top: 12px;
      right:0px;
    }
  }
 }
</style>
