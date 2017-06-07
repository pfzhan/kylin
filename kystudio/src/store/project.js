import api from './../service/api'
import * as types from './types'
export default {
  state: {
    projectList: [],
    allProject: [],
    projectTotalSize: 0,
    selected_project: localStorage.getItem('selected_project'),
    projectAccess: {}
  },
  mutations: {
    [types.SAVE_PROJECT_LIST]: function (state, { list, size }) {
      state.projectList = list
      state.projectTotalSize = size
    },
    [types.CACHE_PROJECT_ACCESS]: function (state, { access, projectId }) {
      state.projectAccess[projectId] = access
    },
    [types.CACHE_ALL_PROJECTS]: function (state, { list, size }) {
      state.allProject = list
      var hasMatch = false
      if (list) {
        list.forEach((p) => {
          if (p.name === localStorage.getItem('selected_project')) {
            hasMatch = true
          }
        })
        if (!hasMatch) {
          localStorage.setItem('selected_project', '')
          state.selected_project = ''
        }
      }
    }
  },
  actions: {
    [types.LOAD_PROJECT_LIST]: function ({ commit }, params) {
      return api.project.getProjectList(params).then((response) => {
        commit(types.SAVE_PROJECT_LIST, {list: response.data.data, size: response.data.data.size})
      })
    },
    [types.LOAD_ALL_PROJECT]: function ({ dispatch, commit }, params) {
      return api.project.getProjectList({pageOffset: 0, pageSize: 100000}).then((response) => {
        // 加载project所有的权限
        var pl = response.data.data && response.data.data.length || 0
        for (var i = 0; i < pl; i++) {
          dispatch(types.GET_PROJECT_ACCESS, response.data.data[i].uuid)
        }
        commit(types.CACHE_ALL_PROJECTS, {list: response.data.data})
      })
    },
    [types.DELETE_PROJECT]: function ({ commit }, projectName) {
      return api.project.deleteProject(projectName)
    },
    [types.UPDATE_PROJECT]: function ({ commit }, project) {
      return api.project.updateProject(project)
    },
    [types.SAVE_PROJECT]: function ({ dispatch, commit }, project) {
      return api.project.saveProject(project).then((res) => {
        dispatch(types.LOAD_ALL_PROJECT)
        return res
      })
    },
    [types.SAVE_PROJECT_ACCESS]: function ({ commit }, {accessData, id}) {
      return api.project.addProjectAccess(accessData, id)
    },
    [types.EDIT_PROJECT_ACCESS]: function ({ commit }, {accessData, id}) {
      return api.project.editProjectAccess(accessData, id)
    },
    [types.GET_PROJECT_ACCESS]: function ({ commit }, projectId) {
      return api.project.getProjectAccess(projectId).then((res) => {
        commit(types.CACHE_PROJECT_ACCESS, {access: res.data.data, projectId: projectId})
        return res
      })
    },
    [types.DEL_PROJECT_ACCESS]: function ({ commit }, {id, aid}) {
      return api.project.delProjectAccess(id, aid)
    },
    [types.ADD_PROJECT_FILTER]: function ({ commit }, filterData) {
      return api.project.saveProjectFilter(filterData)
    },
    [types.EDIT_PROJECT_FILTER]: function ({ commit }, filterData) {
      return api.project.updateProjectFilter(filterData)
    },
    [types.DEL_PROJECT_FILTER]: function ({ commit }, {project, filterName}) {
      return api.project.delProjectFilter(project, filterName)
    },
    [types.GET_PROJECT_FILTER]: function ({ commit }, project) {
      return api.project.getProjectFilter(project)
    },
    [types.BACKUP_PROJECT]: function ({ commit }, project) {
      return api.project.backupProject(project)
    }
  },
  getters: {}
}
