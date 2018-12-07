/* eslint-disable */
export default {
  "allOptionMaps": {
    /**
     * 参数值列表配置
     */
    "menu": [
      { "id": "dashboard", "value": "dashboard", "title": "Dashboard" },
      { "id": "query", "value": "query", "title": "Query" },
      { "id": "insight", "value": "insight", "title": "Insight" },
      { "id": "queryHistory", "value": "query_history", "title": "Query History" },
      { "id": "favoriteQuery", "value": "favorite_query", "title": "Favorite Query" },
      { "id": "studio", "value": "studio", "title": "Studio" },
      { "id": "setting", "value": "project_setting", "title": "setting" },
      { "id": "source", "value": "source", "title": "Source" },
      { "id": "model", "value": "model", "title": "Model" },
      { "id": "index", "value": "index", "title": "Index" },
      { "id": "modelEdit", "value": "modelEdit", "title": "Model Edit" },
      { "id": "monitor", "value": "monitor", "title": "Monitor" },
      { "id": "job", "value": "job", "title": "Job" },
      { "id": "security", "value": "security", "title": "Security" },
      { "id": "user", "value": "user", "title": "User" },
      { "id": "group", "value": "group", "title": "Group" }
    ],
    "datasource": [
      { "id": "hive", "value": 9, "title": "HIVE" },
      { "id": "rdbms", "value": 16, "title": "RDBMS" },
      { "id": "kafka", "value": 1, "title": "HIVE" },
      { "id": "rdbms2", "value": 8, "title": "RDBMS2" }
    ],
    "projectType": [
      { "id": "manualMaintain", "value": "MANUAL_MAINTAIN", "title": "Manual Maintain" },
      { "id": "autoMaintain", "value": "AUTO_MAINTAIN", "title": "Auto Maintain" }
    ],
    // 项目内权限
    "projectRole": [
      { "id": "admin", "value": "ADMINISTRATION", "title": "Admin" },
      { "id": "admin", "value": 16, "title": "Admin" }
    ],
    // 系统组权限
    "groupRole": [
      { "id": "systemAdmin", "value": "ROLE_ADMIN", "title": "Admin" },
      { "id": "systemUser", "value": "ALL_USERS", "title": "User" }
    ],
    "modelType": [
      { "id": "tableOriented", "value": "TABLE_ORIENTED", "title": "Table Oriented" },
      { "id": "modelBased", "value": "MODEL_BASED", "title": "Model Based" }
    ],
    /**
     * ACL权限配置
     */
    "userActions": [
      { "id": "addUser" },
      { "id": "editUser" },
      { "id": "assignGroup" },
      { "id": "changePassword" },
      { "id": "deleteUser" },
      { "id": "disableUser" }
    ],
    "projectActions": [
      { "id": "addGrant" },
      { "id": "editGrant" },
      { "id": "deleteGrant" }
    ],
    "groupActions": [
      { "id": "addGroup" },
      { "id": "editGroup" },
      { "id": "deleteGroup" }
    ],
    "modelActions": [
      { "id": "viewInRelated" },
      { "id": "discard" },
      { "id": "build" },
      { "id": "factSwitcher" },
      { "id": "deleteTable" },
      { "id": "addJoin" }
    ],
    "aggregateActions": [
      { "id": "viewAggGroup" },
      { "id": "editAggGroup" },
      { "id": "addAggGroup" }
    ]
  },
  "disableOptionMaps": {
    "modelActions": {
      "keyPattern": "projectType-modelType",
      "entries": [
        { "key": "manualMaintain-tableOriented", "value": "build" },
        { "key": "manualMaintain-modelBased", "value": "viewInRelated,discard" },
        { "key": "autoMaintain-tableOriented", "value": "build" },
        { "key": "autoMaintain-modelBased", "value": "viewInRelated,discard" }
      ]
    }
  },
  "enableOptionMaps": {
    // 菜单权限
    "menu": {
      "keyPattern": "groupRole-projectRole",
      "entries": [
        { "key": "systemAdmin-*", "value": "overview,query,insight,queryHistory,favoriteQuery,studio,project,source,model,modelEdit,monitor,job,security,user,group" },
        { "key": "systemUser-*", "value": "overview,query,insight,queryHistory,favoriteQuery,studio,project,source,model,modelEdit,monitor,job,security,user" }
      ]
    },
    // 用户操作权限
    "userActions": {
      "keyPattern": "groupRole",
      "entries": [
        { "key": "systemAdmin", "value": "addUser,assignGroup,editUser,changePassword,deleteUser,disableUser" },
        { "key": "systemUser", "value": "none" }
      ]
    },
    // 用户组操作权限
    "groupActions": {
      "keyPattern": "groupRole",
      "entries": [
        { "key": "systemAdmin", "value": "addGroup,editGroup,deleteGroup,viewGroup" },
        { "key": "systemUser", "value": "none" }
      ]
    },
    // 项目操作权限
    "projectActions": {
      "keyPattern": "groupRole-projectRole",
      "entries": [
        { "key": "systemAdmin-*", "value": "addGrant,editGrant,deleteGrant" },
        { "key": "systemUser-admin", "value": "addGrant,editGrant,deleteGrant" },
        { "key": "systemUser-[management,operation,read]", "value": "none" }
      ]
    },
    "aggregateActions": {
      "keyPattern": "projectType",
      "entries": [
        { "key": "manualMaintain", "value": "viewAggGroup,editAggGroup,addAggGroup" },
        { "key": "autoMaintain", "value": "none" }
      ]
    }
  }
}