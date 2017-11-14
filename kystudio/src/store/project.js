import api from './../service/api'
import * as types from './types'
export default {
  state: {
    projectList: [],
    allProject: [],
    projectTotalSize: 0,
    selected_project: localStorage.getItem('selected_project'),
    projectAccess: {},
    projectEndAccess: {}
  },
  mutations: {
    [types.SAVE_PROJECT_LIST]: function (state, { list, size }) {
      state.projectList = list
      state.projectTotalSize = size
    },
    [types.CACHE_PROJECT_ACCESS]: function (state, { access, projectId }) {
      state.projectAccess[projectId] = access
    },
    [types.CACHE_PROJECT_END_ACCESS]: function (state, { access, projectId }) {
      state.projectEndAccess[projectId] = access
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
    /* [types.LOAD_ALL_PROJECT]: function ({ dispatch, commit }, params) {
      return api.project.getProjectList({pageOffset: 0, pageSize: 100000}).then((response) => {
        // 加载project所有的权限
        var pl = response.data.data.projects && response.data.data.projects.length || 0
        if (!(params && params.ignoreAccess)) {
          for (var i = 0; i < pl; i++) {
            dispatch(types.GET_PROJECT_ACCESS, response.data.data.projects[i].uuid)
            dispatch(types.GET_PROJECT_END_ACCESS, response.data.data.projects[i].uuid)
          }
        }
        commit(types.CACHE_ALL_PROJECTS, {list: response.data.data.projects})
      }, () => {
        commit(types.REMOVE_ALL_PROJECTS)
      })
    }, */
    [types.LOAD_ALL_PROJECT]: function ({ dispatch, commit, state }, params) {
      return new Promise((resolve, reject) => {
        api.project.getProjectList({pageOffset: 0, pageSize: 100000}).then((response) => {
          commit(types.CACHE_ALL_PROJECTS, {list: response.data.data.projects})
          let pl = response.data.data.projects && response.data.data.projects.length || 0
          if (!((params && params.ignoreAccess) || pl === 0)) {
            var uuid = response.data.data.projects[0].uuid
            for (var i = 0; i < response.data.data.projects.length; i++) {
              var item = response.data.data.projects[i]
              if (item.name === state.selected_project) {
                uuid = item.uuid
                break
              }
            }
            let curProjectAccessPromise = dispatch(types.GET_PROJECT_END_ACCESS, uuid)
            let curProjectUserAccessPromise = dispatch(types.USER_ACCESS, {project: state.selected_project})
            Promise.all([curProjectAccessPromise, curProjectUserAccessPromise]).then(() => {
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
    [types.GET_PROJECT_ACCESS]: function ({ commit }, projectId) {
      return api.project.getProjectAccess(projectId).then((res) => {
        commit(types.CACHE_PROJECT_ACCESS, {access: res.data.data, projectId: projectId})
        return res
      })
    },
    /* [types.GET_PROJECT_END_ACCESS]: function ({ commit }, projectId) {
      return api.project.getProjectEndAccess(projectId).then((res) => {
        commit(types.CACHE_PROJECT_END_ACCESS, {access: res.data.data, projectId: projectId})
        return res
      })
    }, */
    [types.GET_PROJECT_END_ACCESS]: function ({ commit }, projectId) {
      return new Promise((resolve, reject) => {
        api.project.getProjectEndAccess(projectId).then((res) => {
          commit(types.CACHE_PROJECT_END_ACCESS, {access: res.data.data, projectId: projectId})
          resolve(res)
        }, () => {
          reject()
        })
      })
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
    }
  },
  getters: {}
}
