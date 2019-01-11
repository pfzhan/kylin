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
      { "id": "queryHistory", "value": "queryhistory", "title": "Query History" },
      { "id": "favoriteQuery", "value": "favoritequery", "title": "Favorite Query" },
      { "id": "studio", "value": "studio", "title": "Studio" },
      { "id": "setting", "value": "setting", "title": "setting" },
      { "id": "source", "value": "source", "title": "Source" },
      { "id": "model", "value": "model", "title": "Model" },
      { "id": "index", "value": "index", "title": "Index" },
      { "id": "modelEdit", "value": "modelEdit", "title": "Model Edit" },
      { "id": "monitor", "value": "monitor", "title": "Monitor" },
      { "id": "job", "value": "job", "title": "Job" },
      { "id": "admin", "value": "admin", "title": "Admin" },
      { "id": "project", "value": "project", "title": "Project" },
      { "id": "user", "value": "user", "title": "User" },
      { "id": "group", "value": "group", "title": "Group" },
      { "id": "groupDetail", "value": "groupdetail", "title": "GroupDetail" }
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
    "modelType": [
      { "id": "tableOriented", "value": "TABLE_ORIENTED", "title": "Table Oriented" },
      { "id": "modelBased", "value": "MODEL_BASED", "title": "Model Based" }
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
    // 菜单权限
    "menu": {
      "keyPattern": "groupRole-projectRole-menu",
      "entries": [
        { "key": "systemAdmin-*-[project,user,group,groupDetail]", "value": "dashboard,query,insight,queryHistory,favoriteQuery,studio,setting,source,model,index,modelEdit,monitor,job" },
        { "key": "systemAdmin-*-*", "value": "project,user,group,groupDetail" },
        { "key": "systemUser-*-*", "value": "admin,project,group,groupDetail,user" }
      ]
    },
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