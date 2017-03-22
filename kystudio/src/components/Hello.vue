<template>
  <div class="hello" id="welcome">
   {{$t('selectLanguage')}}
   <changelang></changelang>
   <changeproject></changeproject>
  </el-select>
    <el-row :gutter="20">
      <el-col :span="8" v-for="(o, index) in productions" :key="index " >
        <el-card :body-style="{ padding: '0px' }">
          <img src="../assets/img/test.png" class="image">
          <div style="padding: 14px;">
            <span><router-link :to="{name:o.link}">{{o.name}}</router-link></span>
            <p>{{o.discribe}}</p>
            <div class="bottom clearfix">
              <!-- <time class="time">{{o.currentDate}}</time>
              <el-button type="text" class="button">操作按钮</el-button> -->
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>
    <router-link to="demo" id="gotoDemo">{{$t('goToDemo')}}</router-link>
  </div>
</template>

<script>
import changelang from './common/change_lang'
import changeproject from './project/project_select'
export default {
  name: 'hello',
  data () {
    return {
      currentDate: new Date(),
      productions: [
        {name: 'KAP', discribe: this.$t('kap.discribe'), currentDate: '2017-1-1', link: ''},
        {name: 'KyBot', discribe: this.$t('kybot.discribe'), currentDate: '2017-1-1', link: ''},
        {name: 'KyStudio', discribe: this.$t('kystudio.discribe'), currentDate: '2017-1-1', link: 'model'}
      ]
    }
  },
  components: {
    changelang,
    changeproject
  },
  locales: {
    'en': {kap: {discribe: 'Kyligence Analytics Platform'}, kybot: {discribe: 'KyBot - Online Support Service for Apache Kylin'}, kystudio: {discribe: 'Next generation for KAP'}, goToDemo: 'Components Demo Site', selectLanguage: 'Select Language：'},
    'zh-cn': {kap: {discribe: '跬智分析平台'}, kybot: {discribe: 'KYLIN在线分析服务'}, kystudio: {discribe: '下一代KYLIN分析平台'}, goToDemo: '查看组件demo', selectLanguage: '请选择语言：'}
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style lang='less' scoped >
h1, h2 {
  font-weight: normal;
}

ul {
  list-style-type: none;
  padding: 0;
}

li {
  display: inline-block;
  margin: 0 10px;
}

a {
  color: #42b983;
}
#gotoDemo{
  position: absolute;
  right: 10px;
  bottom: 20px;
}
#welcome{
   text-align: center;
   margin-top: 120px;
  .time {
    font-size: 13px;
    color: #999;
    
  }
  .el-col{
    margin-top: 20px;
  }
  .bottom {
    margin-top: 13px;
    line-height: 12px;
  }

  .button {
    padding: 0;
    float: right;
  }

  .image {
    width: 100%;
    display: block;
    width: 200px;
    height: 200px;
    margin: 6px auto;

  }

  .clearfix:before,
  .clearfix:after {
      display: table;
      content: "";
  }
  
  .clearfix:after {
      clear: both
  }
}

</style>
