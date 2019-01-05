import { set } from '../../../util/object'

export const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  INIT_FORM: 'INIT_FORM'
}

const initialState = JSON.stringify({
  isShow: false,
  callback: null,
  editType: '',
  firstEditType: '',
  form: {
    project: null,
    selectedTables: [],
    selectedDatabases: [],
    settings: {
      name: '',
      creator: '',
      description: '',
      host: '',
      port: '',
      isAuthentication: false,
      username: '',
      password: ''
    }
  },
  project: null
})

export default {
  state: JSON.parse(initialState),
  mutations: {
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = { ...state.form, ...payload }
    },
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    [types.SET_MODAL]: (state, payload) => {
      for (const key in payload) {
        state[key] = payload[key]
      }
    },
    [types.INIT_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
      state.form.project = _getEmptySourceProject(state.project, state.editType)
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, project }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { editType, project, firstEditType: editType, callback: resolve })
        commit(types.INIT_FORM)
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

function _getEmptySourceProject (project, editType) {
  const properties = { ...project.override_kylin_properties }
  project = set(project, 'override_kylin_properties', properties)
  project.override_kylin_properties['kylin.source.default'] = !isNaN(editType) ? editType : null
  return project
}
