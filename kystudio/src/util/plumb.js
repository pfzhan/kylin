import { jsPlumb } from 'jsplumb'
import { stopPropagation } from './event'
// jsPlumb 工具库
export function jsPlumbTool () {
  var plumbInstance = null
  let lineColor = '#0988de'
  return {
    endpointConfig: {
      endpoint: 'Dot',
      paintStyle: {
        stroke: lineColor,
        fill: 'transparent',
        radius: 1,
        strokeWidth: 1
      },
      isSource: true,
      isTarget: true,
      connector: [ 'Bezier', { curviness: 22 } ], // 设置连线为贝塞尔曲线
      connectorStyle: {
        strokeWidth: 1,
        stroke: lineColor,
        joinstyle: 'round'
      },
      dragOptions: {}
    },
    init: function (dom, zoom) {
      plumbInstance = this._getPlumbInstance(jsPlumb, dom)
      this.setZoom(zoom)
      return plumbInstance
    },
    lazyRender (cb) {
      jsPlumb.setSuspendDrawing(true)
      cb && cb()
      jsPlumb.setSuspendDrawing(false, true)
    },
    bindConnectionEvent (cb) {
      plumbInstance.bind('connection', (info, originalEvent) => {
        cb(info.connection, originalEvent)
      })
    },
    deleteAllEndPoints () {
      plumbInstance.deleteEveryEndpoint()
    },
    deleteEndPoint (uuid) {
      plumbInstance.deleteEndpoint(uuid)
    },
    addEndpoint (guid, anchor, endPointConfig) {
      plumbInstance.addEndpoint(guid, anchor, endPointConfig)
    },
    _getPlumbInstance (jsPlumb, el) {
      return jsPlumb.getInstance({
        DragOptions: { cursor: 'pointer', zIndex: 2000 },
        HoverPaintStyle: { stroke: lineColor },
        ConnectionOverlays: [
          [ 'Arrow', {
            location: 1,
            visible: true,
            width: 11,
            length: 11,
            id: 'ARROW'
          } ]],
        Container: el
      })
    },
    refreshPlumbInstance () {
      plumbInstance.repaintEverything()
    },
    draggable (idList, stopCb) {
      plumbInstance.draggable(idList, {
        handle: '.table-title',
        drop: function (e) {
        },
        stop: function (e) {
          stopCb(e)
        }
      })
    },
    setZoom (zoom) {
      var transformOrigin = [0.5, 0.5]
      var el = plumbInstance.getContainer()
      var p = [ 'webkit', 'moz', 'ms', 'o' ]
      var s = 'scale(' + zoom + ')'
      var oString = (transformOrigin[0] * 100) + '% ' + (transformOrigin[1] * 100) + '%'
      for (var i = 0; i < p.length; i++) {
        el.style[p[i] + 'Transform'] = s
        el.style[p[i] + 'TransformOrigin'] = oString
      }
      el.style['transform'] = s
      el.style['transformOrigin'] = oString
      plumbInstance.setZoom(zoom)
    },
    connect (pid, fid, clickCb, otherProper) {
      var defaultPata = {
        uuids: [fid, pid],
        deleteEndpointsOnDetach: true,
        editable: true,
        overlays: [
          ['Custom', {
            create: function () {
              let overlays = document.createElement('div')
              overlays.innerHTML = '<span class="label"></span><span class="close el-icon-ksd-close"></span>'
              return overlays
            },
            id: pid + (fid + 'label'),
            events: {
              mousedown: function (_, e) {
                stopPropagation(e)
                return false
              },
              click: function (_, e) {
                clickCb(pid, fid, e)
              }
            }
          }]
        ]
      }
      defaultPata = Object.assign(defaultPata, otherProper)
      return plumbInstance.connect(defaultPata)
    },
    deleteConnect (conn) {
      if (conn.endpoints) {
        conn.endpoints.forEach((point) => {
          this.deleteEndPoint(point) // 删除连接点，等同删除连线
        })
      }
    }
  }
}
