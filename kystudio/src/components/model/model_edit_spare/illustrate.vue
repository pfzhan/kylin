 <template>
     <table class="sample-info">
          <tr>
            <td><span class="iconD column-kind-typical">D</span><span class="info">{{$t('kylinLang.common.dimension')}}</span></td>
            <td><span class="iconM column-kind-typical">M</span><span class="info">{{$t('kylinLang.common.measure')}}</span></td>
            <td><span class="iconDis column-kind-typical">－</span><span class="info">{{$t('kylinLang.common.disable')}}</span></td>
            <td><span class="color_block color_fact"> </span><span class="info">{{$t('kylinLang.common.fact')}}</span></td>
            <td><span class="color_block color_lookup"> </span><span class="info">{{$t('kylinLang.common.lookup')}}</span></td>
          </tr>
      </table>
  </template>
  <script>
    export default {

    }
  </script>
  <style lang="less">
  @import '../../../assets/styles/variables.less';
  .sample-info{
    position: absolute;
    right: 6px;
    z-index: 2000;
    top:10px;
    right: 10px;
    td{
      font-size: 12px;
      min-width: 80px;
      span {
        display: inline-block;
        color:@text-normal-color;
        &.column-kind-typical {
          width: 14px;
          height: 14px;
          border-radius: 7px;
          background-color:@dm-bg-color;
          text-align: center;
          font-size: 12px;
          line-height: 14px;
        }
      }
      .info{
        padding-left: 4px;
      }
    }
   }
  </style>
