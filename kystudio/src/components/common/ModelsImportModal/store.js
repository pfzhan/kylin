import * as actionTypes from '../../../store/types'
import api from '../../../service/api'
import { importableConflictTypes, brokenConflictTypes, conflictTypes } from './handler'
import { handleError, handleSuccessAsync } from '../../../util'

export function getInitialState () {
  return {
    isShow: false,
    callback: null,
    project: null,
    models: [],
    conflicts: [],
    form: {
      signature: '',
      file: null,
      ids: []
    }
  }
}

function findConflictModelInZip (conflictItems, modelNamesMap) {
  let conflictModel = null
  for (const conflictItem of conflictItems) {
    const [modelName] = conflictItem.element.split('-')
    if (modelName in modelNamesMap) {
      conflictModel = modelName
    }
  }
  return conflictModel
}

function formatParsedResponse (response) {
  return {
    models: response.models.map(modelResponse => ({
      id: modelResponse.uuid,
      name: modelResponse.name,
      nodeType: 'model',
      children: modelResponse.tables.map(tableResponse => ({
        id: `${modelResponse.uuid}-${tableResponse.name}`,
        name: tableResponse.name,
        nodeType: 'table',
        type: tableResponse.kind
      }))
    })),
    conflicts: response.conflicts,
    signature: response.signature
  }
}

function formatUploadData (form) {
  const uploadData = new FormData()
  uploadData.append('file', form.file)
  return uploadData
}

function formatImportData (form) {
  const importData = new FormData()
  importData.append('file', form.file)
  importData.append('signature', form.signature)
  form.ids.forEach((id, idx) => {
    importData.append(`ids[${idx}]`, id)
  })
  return importData
}

export default {
  state: getInitialState(),
  mutations: {
    [actionTypes.SHOW_MODAL] (state) {
      state.isShow = true
    },
    [actionTypes.HIDE_MODAL] (state) {
      state.isShow = false
    },
    [actionTypes.SET_MODAL] (state, payload) {
      for (const [key, value] of Object.entries(payload)) {
        state[key] = value
      }
    },
    [actionTypes.INIT_MODAL] (state) {
      for (const [key, value] of Object.entries(getInitialState())) {
        state[key] = value
      }
    },
    [actionTypes.SET_MODAL_FORM] (state, payload) {
      for (const [key, value] of Object.entries(payload)) {
        state.form[key] = value
      }
    }
  },
  actions: {
    [actionTypes.CALL_MODAL] ({ commit }, payload) {
      return new Promise(resolve => {
        commit(actionTypes.INIT_MODAL)
        commit(actionTypes.SET_MODAL, { ...payload, callback: resolve })
        commit(actionTypes.SHOW_MODAL)
      })
    },
    [actionTypes.UPLOAD_MODEL_METADATA_FILE] ({ commit }, payload) {
      return new Promise(async (resolve, reject) => {
        const { project } = payload

        try {
          const form = formatUploadData(payload.form)
          const response = await api.model.uploadModelsMetadata({ project, form })
          const result = await handleSuccessAsync(response)

          const { models, conflicts, signature } = formatParsedResponse(result)
          commit(actionTypes.SET_MODAL, { models, conflicts })
          commit(actionTypes.SET_MODAL_FORM, { signature })
          resolve()
        } catch (e) {
          handleError(e)
          reject(e)
        }
      })
    },
    [actionTypes.IMPORT_MODEL_METADATA_FILE] ({ commit }, payload) {
      return new Promise(async (resolve, reject) => {
        const { project } = payload
        const form = formatImportData(payload.form)

        try {
          await api.model.importModelsMetadata({ project, form })
          resolve()
        } catch (e) {
          handleError(e)
          reject(e)
        }
      })
    }
  },
  getters: {
    modelNamesMap (state) {
      return Object
        .values(state.models)
        .reduce((map, model) => ({ ...map, [model.name]: model }), {})
    },
    conflictModels (state, getters) {
      const conflictModelsMap = {}
      const { modelNamesMap } = getters

      for (const conflict of state.conflicts) {
        // 只有前端允许的冲突类型，才可以成为冲突项
        if (conflict.type in conflictTypes) {
          // 如果conflict.items中有一项element隶属于zip解析后的某个model，则归类到map中
          const conflictModel = findConflictModelInZip(conflict.items, modelNamesMap)
          if (conflictModelsMap[conflictModel]) {
            conflictModelsMap[conflictModel].push(conflict)
          } else {
            conflictModelsMap[conflictModel] = [conflict]
          }
        }
      }
      // 把map整理为数组，随同id和conflicts一起导出
      return Object.entries(conflictModelsMap).map(model => {
        const [name, conflicts] = model
        const id = modelNamesMap[name].id
        return { id, name, conflicts }
      })
    },
    unConflictModels (state, getters) {
      const { conflictModels } = getters

      return state.models
        .filter(model => conflictModels.every(conflictModel => conflictModel.name !== model.name))
    },
    brokenConflictModels (state, getters) {
      const { conflictModels } = getters
      return conflictModels.filter(model => (
        model.conflicts.some(conflict => brokenConflictTypes.includes(conflict.type))
      ))
    },
    importableConflictModels (state, getters) {
      const { conflictModels } = getters
      return conflictModels.filter(model => (
        model.conflicts.every(conflict => importableConflictTypes.includes(conflict.type))
      ))
    }
  },
  namespaced: true
}
