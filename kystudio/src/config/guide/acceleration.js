// 加速 页面的脚本配置
export function accelerationDrama () {
  return [
    {
      eventID: 8,
      done: false,
      router: 'FavoriteQuery', // 跳转到加速页面
      tip: 'speedTipAuto'
    },
    {
      eventID: 1,
      done: false,
      target: 'speedProcess', // 演示动画
      offsetX: -650,
      tip: 'speedTipAuto1'
    },
    {
      eventID: 1,
      done: false,
      target: 'speedProcess', // 跳转到monitor页面
      offsetX: -500,
      tip: 'speedTipAuto2'
    },
    {
      eventID: 1,
      done: false,
      target: 'speedProcess', // 跳转到monitor页面
      offsetX: -210
    },
    {
      eventID: 5,
      done: false,
      search: '.guide-checkData' // 检查有无数据
    },
    {
      eventID: 1,
      done: false,
      target: 'speedSqlNowBtn' // 飞向立即加速页面
    },
    {
      eventID: 2,
      done: false,
      target: 'speedSqlNowBtn' // 点击立即加速页面
    }
  ]
}
