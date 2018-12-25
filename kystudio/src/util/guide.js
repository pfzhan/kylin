import TWEEN from '@tweenjs/tween.js'
import store from '../store'
import { drama } from '../config/guide/index.js'
import Scrollbar from 'smooth-scrollbar'
class Guide {
  constructor (options, _) {
    this.vm = _
    this.systemStore = store.state.system.guideConfig
    this.mode = options.mode || 'manual' // 模式
    this.steps = null
    this.promiseObj = null
    this.STs = []
    this.stepSpeed = 1000
    this.waitLimit = 2000 // 等待查询的次数限制
    this.waitCount = 0 // 等待次数
    this.currentStep = null
    this.events = [
      {id: 1, info: '鼠标移动到指定位置', action: 'mouse'},
      {id: 11, info: '鼠标移动到指定位置', action: 'drag'},
      {id: 2, info: '点击', action: 'click'},
      {id: 3, info: '打字机输入', action: 'input'},
      {id: 31, info: '直接输入', action: 'inputonce'},
      {id: 32, info: '直接输入(非响应)', action: 'inputwithnoresponse'},
      {id: 4, info: '拖动', action: 'click'},
      {id: 5, info: '检查dom不存在', action: 'checkAbsent'},
      {id: 51, info: '检查dom存在', action: 'check'},
      {id: 6, info: '元素进入可视区域', action: 'inView'},
      {id: 7, info: '展开树', action: 'click'},
      {id: 71, info: '点击树', action: 'extendTree'},
      {id: 8, info: '跳转路由', action: 'go'}
    ]
    this.drama = {
      manual: drama.manual(),
      project: drama.project(),
      loadTable: drama.loadTable(),
      addModel: drama.addModel(),
      monitor: drama.monitor()
    }
  }
  getEventById (id) {
    for (let i = 0; i < this.events.length; i++) {
      if (this.events[i].id === id) {
        return this.events[i]
      }
    }
  }
  _click (el, stepInfo, resolve) {
    this.hideAllMouse()
    this.systemStore.globalMouseClick = true
    setTimeout(() => {
      this.systemStore.globalMouseClick = false
      this.systemStore.globalMouseVisible = true
      el._isVue ? el.$emit('click', stepInfo.val) : el.click(stepInfo.val)
      resolve()
    }, 100)
  }
  _focus (el) {
    el.focus()
  }
  _input (el, val, resolve) {
    this._focus(el)
    this._printInput(el, val, resolve)
  }
  _inputonce (el, val, resolve) {
    this._focus(el)
    el && el.$emit('input', val)
  }
  _inputwithnoresponse (el, val, resolve) {
    this._focus(el)
    if (el) {
      el.$emit('change', val)
    }
  }
  _query (target, checkAbsent, search) {
    let result = null
    let targetResult = null
    let searchFuc = (resolve, reject) => {
      if (!target && !search) {
        return resolve()
      }
      if (target) {
        this.waitCount++
        targetResult = this.systemStore.targetList[target]
        if (targetResult) {
          if (search) {
            let targetDom = targetResult.$el || targetResult
            result = targetDom.querySelector(search)
          } else {
            result = targetResult
          }
        } else {
          result = null
        }
      } else {
        result = document.querySelector(search)
      }
      if (checkAbsent && result || !checkAbsent && !result) {
        if (this.waitCount > this.waitLimit) {
          console.log('查找超时')
          return reject()
        }
        setTimeout(() => {
          searchFuc(resolve, reject)
        }, 300)
      } else {
        console.log(result, targetResult)
        return resolve({dom: result, target: targetResult})
      }
    }
    return new Promise((resolve, reject) => {
      try {
        searchFuc(resolve, reject)
      } catch (e) {
        console.log(e)
        reject(e)
      }
    })
  }
  // 无拦截跳转路由
  _jump (name) {
    this.vm.$router.replace({name: name, params: { ignoreIntercept: true }})
  }
  hideAllMouse () {
    this.systemStore.globalMouseVisible = false
    this.systemStore.globalMouseClick = false
    this.systemStore.globalMouseDrag = false
  }
  // 手型移动
  _mouseTo (el, stepInfo, resolve) {
    if (el) {
      el = el.$el ? el.$el : el
    }
    this.hideAllMouse()
    if (stepInfo.isDrag) {
      this.systemStore.globalMouseDrag = true
    } else {
      this.systemStore.globalMouseVisible = true
    }
    let {left, top, width = 0, height = 0} = stepInfo.pos || el && el.getBoundingClientRect() || {}
    new TWEEN.Tween(this.systemStore.mousePos).to({
      x: left + width - 37,
      y: top + height - 30
    }, 200).onComplete(() => {
      resolve()
    }).start()
    function animate () {
      if (TWEEN.update()) {
        requestAnimationFrame(animate)
      }
    }
    animate()
  }
  // 打字机输入效果
  _printInput (dom, val, resolve) {
    let i = 0
    dom.$emit('input', '')
    let fuc = () => {
      let st = setTimeout(() => {
        if (i > val.length) {
          resolve()
          return
        }
        let printVal = val.slice(0, i++)
        if (dom) {
          dom.$emit('input', printVal)
        }
        fuc()
      }, 50)
      this.STs.push(st)
    }
    fuc()
  }
  _inView (dom, targetDom, stepInfo, resolve) {
    setTimeout(() => {
      if (targetDom) {
        targetDom = targetDom.$el ? targetDom.$el : targetDom
      }
      let scrollInstance = Scrollbar.get(targetDom)
      if (scrollInstance && dom) {
        scrollInstance.scrollIntoView(dom)
      }
      resolve()
    }, 10)
  }
  renderFuc (_event, stepInfo) {
    return new Promise((resolve) => {
      this.waitCount = 0
      this._query(stepInfo.target, _event.action === 'checkAbsent', stepInfo.search).then((domList) => {
        let dom = domList && domList.dom || null
        let targetDom = domList && domList.target || null
        if (_event.action === 'mouse') {
          this._mouseTo(dom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else if (_event.action === 'drag') {
          stepInfo.isDrag = true
          this._mouseTo(dom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else if (dom && _event.action === 'click') {
          this._click(dom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else if (dom && _event.action === 'input') {
          this._input(dom, stepInfo.val, () => {
            resolve(stepInfo)
          })
        } else if (dom && _event.action === 'inputonce') {
          this._inputonce(dom, stepInfo.val)
          resolve(stepInfo)
        } else if (dom && _event.action === 'inputwithnoresponse') {
          this._inputwithnoresponse(dom, stepInfo.val)
          resolve(stepInfo)
        } else if (_event.action === 'go') {
          this._jump(stepInfo.router)
          resolve(stepInfo)
        } else if (_event.action === 'inView') {
          this._inView(dom, targetDom, stepInfo, () => {
            resolve(stepInfo)
          })
        } else {
          resolve(stepInfo)
        }
      }).catch((e) => {
        console.log(e)
        this.stop()
        console.log('查找演示节点超时')
      })
    })
  }
  // 生成计划
  stepFuncs (funcs) {
    let result = []
    for (let i = 0; i < funcs.length; i++) {
      let stepEvent = this.getEventById(funcs[i].eventID)
      result.push(() => { return this.renderFuc(stepEvent, funcs[i]) })
    }
    return {
      steps: result,
      i: -1,
      next () {
        this.i++
        let step = this.steps[this.i]
        return {
          value: step && step() || null,
          done: this.i === this.steps.length - 1
        }
      },
      prev () {
        this.i--
        return {
          value: this.steps[this.i],
          done: false
        }
      }
    }
  }
  // 单步播放模式
  step (step, resolve, reject) {
    if (this.currentStep && this.currentStep.done === true || !this.currentStep) {
      let p = this.steps.next().value
      if (p && p.then) {
        p.then((stepInfo) => {
          this.currentStep = stepInfo
          stepInfo.done = true
          if (step === undefined || isNaN(step) || step - 1 > 0) {
            let st = setTimeout(() => {
              this.step(step - 1, resolve)
            }, this.stepSpeed)
            this.STs.push(st)
          }
        })
      } else {
        resolve()
        console.log('完成了！')
      }
    } else {
      reject()
      console.log('上一步还没完成')
    }
  }
  getStepInfo (i) {
    return this.steps[i]
  }
  // 按步执行效果
  start () {
    this.systemStore.globalMaskVisible = true
    this.steps = this.stepFuncs(this.drama[this.mode])
    return this
  }
  // 按步执行，step为空的话就连续执行
  go (step) {
    return new Promise((resolve, reject) => {
      this.step(step, resolve, reject)
    })
  }
  stop () {
    this.systemStore.globalMaskVisible = false
  }
}
export default Guide
