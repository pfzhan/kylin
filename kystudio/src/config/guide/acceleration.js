import { verySlowGuideSpeed } from './config'
// 加速 页面的脚本配置
export function accelerationDrama () {
  return [
    {
      eventID: 8,
      done: false,
      router: 'Acceleration', // 跳转到加速页面
      tip: 'speedTipAuto'
    },
    {
      eventID: 1,
      done: false,
      target: 'speedProcess', // 演示动画
      offsetX: -650,
      tip: 'speedTipAuto1',
      timer: verySlowGuideSpeed
    },
    {
      eventID: 1,
      done: false,
      target: 'speedProcess', // 跳转到monitor页面
      offsetX: -500,
      tip: 'speedTipAuto2',
      timer: verySlowGuideSpeed
    },
    {
      eventID: 1,
      done: false,
      target: 'speedProcess', // 跳转到monitor页面
      offsetX: -210,
      timer: verySlowGuideSpeed
    },
    {
      eventID: 5,
      done: false,
      search: '.guide-checkData', // 检查有无数据
      timer: verySlowGuideSpeed
    },
    {
      eventID: 1,
      done: false,
      target: 'speedSqlNowBtn', // 飞向立即加速页面
      timer: verySlowGuideSpeed
    },
    {
      eventID: 2,
      done: false,
      target: 'speedSqlNowBtn', // 点击立即加速页面
      withEvent: true,
      timer: verySlowGuideSpeed
    }
  ]
}
