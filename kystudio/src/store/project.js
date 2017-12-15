import api from './../service/api'
import * as types from './types'
export default {
  state: {
    projectList: [],
    allProject: [],
    projectTotalSize: 0,
    selected_project: localStorage.getItem('selected_project')
  },
  mutations: {
    [types.SAVE_PROJECT_LIST]: function (state, { list, size }) {
      state.projectList = list
      state.projectTotalSize = size
    },
    [types.CACHE_ALL_PROJECTS]: function (state, { list, size }) {
      state.allProject.splice(0, state.allProject.length)
      state.allProject = Object.assign(state.allProject, list)
      var hasMatch = false
      if (list) {
        list.forEach((p) => {
          if (p.name === localStorage.getItem('selected_project')) {
            hasMatch = true
            state.selected_project = p.name // 之前没这句，在其他tab 切换了project，顶部会不变
          }
        })
        if (!hasMatch) {
          localStorage.setItem('selected_project', '')
          state.selected_project = ''
        }
        if (state.allProject.length > 0 && !localStorage.getItem('selected_project')) {
          state.selected_project = state.allProject[0].name
          localStorage.setItem('selected_project', state.selected_project)
        }
      } else {
        localStorage.setItem('selected_project', '')
        state.selected_project = ''
      }
    },
    [types.REMOVE_ALL_PROJECTS]: function (state) {
      state.allProject = []
    },
    [types.RESET_PROJECT_STATE]: function (state) {
      state.projectList.splice(0, state.projectList.length)
      state.allProject.splice(0, state.allProject.length)
      state.projectTotalSize = 0
      state.selected_project = localStorage.getItem('selected_project')
      state.projectAccess = {}
      state.projectEndAccess = {}
    }
  },
  actions: {
    [types.LOAD_PROJECT_LIST]: function ({ commit }, params) {
      return api.project.getProjectList(params).then((response) => {
        commit(types.SAVE_PROJECT_LIST, {list: response.data.data.projects, size: response.data.data.size})
      })
    },
    [types.LOAD_ALL_PROJECT]: function ({ dispatch, commit, state }, params) {
      return new Promise((resolve, reject) => {
        api.project.getProjectList({pageOffset: 0, pageSize: 100000}).then((response) => {
          commit(types.CACHE_ALL_PROJECTS, {list: response.data.data.projects})
          let pl = response.data.data.projects && response.data.data.projects.length || 0
          if (!((params && params.ignoreAccess) || pl === 0)) {
            let curProjectUserAccessPromise = dispatch(types.USER_ACCESS, {project: state.selected_project})
            Promise.all([curProjectUserAccessPromise]).then(() => {
              resolve(response.data.data.projects)
            }, () => {
              resolve(response.data.data.projects)
            })
          } else {
            resolve(response.data.data.projects)
          }
        }, () => {
          commit(types.REMOVE_ALL_PROJECTS)
          reject()
        })
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
    [types.GET_PROJECT_ACCESS]: function ({ commit }, para) {
      return api.project.getProjectAccess(para.projectId, para.data)
    },
    [types.DEL_PROJECT_ACCESS]: function ({ commit }, {id, aid, userName}) {
      return api.project.delProjectAccess(id, aid, userName)
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
    },
    [types.ACCESS_AVAILABLE_USER_OR_GROUP]: function ({ commit }, para) {
      return api.project.accessAvailableUserOrGroup(para.type, para.uuid, para.data)
    }
  },
  getters: {}
}
