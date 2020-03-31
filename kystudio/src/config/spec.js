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
      { "id": "acceleration", "value": "acceleration", "title": "Favorite Query" },
      { "id": "studio", "value": "studio", "title": "Studio" },
      { "id": "setting", "value": "setting", "title": "setting" },
      { "id": "source", "value": "source", "title": "Source" },
      { "id": "model", "value": "modellist", "title": "modelList" },
      { "id": "index", "value": "index", "title": "Index" },
      { "id": "modelEdit", "value": "modeledit", "title": "Model Edit" },
      { "id": "monitor", "value": "monitor", "title": "Monitor" },
      { "id": "job", "value": "job", "title": "Job" },
      { "id": "admin", "value": "admin", "title": "Admin" },
      { "id": "project", "value": "project", "title": "Project" },
      { "id": "user", "value": "user", "title": "User" },
      { "id": "group", "value": "group", "title": "Group" },
      { "id": "groupDetail", "value": "groupdetail", "title": "GroupDetail" },
      { "id": "projectAuthority", "value": "projectauthority", "title": "Project Authority" },
      { "id": "diagnostic", "value": "diagnostic", "title": "" }
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
      { "id": "management", "value": "MANAGEMENT", "title": "Admin" },
      { "id": "operation", "value": "OPERATION", "title": "Admin" },
      { "id": "read", "value": "READ", "title": "Admin" },
      { "id": "admin", "value": 16, "title": "Admin" },
      { "id": "default", "value": "DEFAULT", "title": "Admin" }
    ],
    // 系统组权限
    "groupRole": [
      { "id": "systemAdmin", "value": "ROLE_ADMIN", "title": "Admin" },
      { "id": "systemUser", "value": "ALL_USERS", "title": "User" }
    ],
    /**
     * ACL权限配置
     */
    "datasourceActions": [
      { "id": "sourceManagement" },
      { "id": "loadSource" },
      { "id": "delSourceTable" },
      { "id": "sampleSourceTable" },
      { "id": "reloadSourceTable" },
      { "id": "loadData" },
      { "id": "accelerationActions" },
      { "id": "acceRuleSettingActions" },
      { "id": "acceDelSql" },
      { "id": "acceBlackList" },
      { "id": "modelActions" },
      { "id": "segmentActions" },
      { "id": "editAggGroup" },
      { "id": "delAggIdx" },
      { "id": "bulidIndex" },
      { "id": "tableIndexActions" },
      { "id": "viewDataSource" },
      { "id": "changeBuildType" },
      { "id": "changePartition" }
    ],
    "modelActions": [
      { "id": "dataLoad" },
      { "id": "importMDX" },
      { "id": "exportTDS" },
      { "id": "exportMDX" },
      { "id": "rename" },
      { "id": "clone" },
      { "id": "delete" },
      { "id": "purge" },
      { "id": "offline" },
      { "id": "online" }
    ],
    "metadataActions": [
      { "id": "executeModelMetadata" }
    ],
    "monitorActions": [
      { "id": "jobActions" },
      { "id": "diagnostic" }
    ],
    "userActions": [
      { "id": "addUser" },
      { "id": "editUser" },
      { "id": "assignGroup" },
      { "id": "changePassword" },
      { "id": "deleteUser" },
      { "id": "disableUser" }
    ],
    "projectActions": [
      { "id": "addProject" },
      { "id": "deleteProject" },
      { "id": "editProject" },
      { "id": "backUpProject" },
      { "id": "accessActions" },
      { "id": "executeModelsMetadata" }
    ],
    "groupActions": [
      { "id": "addGroup" },
      { "id": "editGroup" },
      { "id": "deleteGroup" }
    ],
    "dashboardActions": [
      { "id": "clearStorage" },
      { "id": "viewJobList" },
      { "id": "viewSetting" },
      { "id": "viewAcceleration" }
    ],
    "systemActions": [
      { "id": "userGuide" },
      { "id": "viewAllProjectJobs" },
      { "id": "updateLicense" }
    ],
    "settingActions": [
      { "id": "yarnQueue" }
    ]
  },
  "disableOptionMaps": {
  },
  "enableOptionMaps": {
    // 菜单权限
    "menu": {
      "keyPattern": "groupRole-projectRole-menu",
      "entries": [
        { "key": "systemAdmin-*-[project,user,group,groupDetail,projectAuthority,diagnostic]", "value": "admin,project,user,group,groupDetail,diagnostic,projectAuthority" },
        { "key": "systemAdmin-*-[dashboard,query,insight,queryHistory,acceleration,studio,setting,source,model,index,modelEdit,monitor,job]", "value": "dashboard,query,insight,queryHistory,acceleration,studio,setting,source,model,index,modelEdit,monitor,job,admin" },
        { "key": "systemUser-admin-[project,user,group,groupDetail,projectAuthority]", "value": "project,admin,projectAuthority" },
        { "key": "systemUser-admin-[dashboard,query,insight,queryHistory,acceleration,studio,setting,source,model,index,modelEdit,monitor,job]", "value": "dashboard,query,insight,queryHistory,acceleration,studio,setting,source,model,index,modelEdit,monitor,job,admin" },
        { "key": "systemUser-management-*", "value": "dashboard,query,insight,queryHistory,acceleration,studio,source,model,index,modelEdit,monitor,job" },
        { "key": "systemUser-operation-*", "value": "dashboard,query,insight,queryHistory,studio,model,index,monitor,job" },
        { "key": "systemUser-read-*", "value": "dashboard,query,insight,queryHistory,studio,model,index" },
        { "key": "systemUser-default-*", "value": "dashboard" }
      ]
    },
    // 新手引导
    "systemActions": {
      "keyPattern": "groupRole-projectRole",
      "entries": [
        { "key": "systemAdmin-*", "value": "userGuide,viewAllProjectJobs,updateLicense" },
        { "key": "systemUser-admin", "value": "viewAllProjectJobs" },
        { "key": "systemUser-*", "value": "none" }
      ]
    },
    // 仪表盘
    "dashboardActions": {
      "keyPattern": "groupRole-projectRole",
      "entries": [
        { "key": "systemAdmin-*", "value": "clearStorage,viewJobList,viewSetting,viewAcceleration" },
        { "key": "systemUser-admin", "value": "clearStorage,viewJobList,viewSetting,viewAcceleration" },
        { "key": "systemUser-management", "value": "viewJobList,viewAcceleration" },
        { "key": "systemUser-operation", "value": "viewJobList" },
        { "key": "systemUser-read", "value": "none" }
      ]
    },
    // 建模中心：数据源，加速引擎，模型
    "datasourceActions": {
      "keyPattern": "groupRole-projectType-projectRole",
      "entries": [
        { "key": "systemAdmin-manualMaintain-*", "value": "sourceManagement,loadSource,delSourceTable,sampleSourceTable,reloadSourceTable,loadData,accelerationActions,acceRuleSettingActions,acceBlackList,acceDelSql,modelActions,segmentActions,editAggGroup,delAggIdx,bulidIndex,tableIndexActions,changePartition,changeBuildType" },
        { "key": "systemAdmin-autoMaintain-*", "value": "sourceManagement,loadSource,delSourceTable,sampleSourceTable,reloadSourceTable,loadData,accelerationActions,acceRuleSettingActions,acceBlackList,acceDelSql" },

        { "key": "systemUser-manualMaintain-admin", "value": "sourceManagement,loadSource,delSourceTable,sampleSourceTable,reloadSourceTable,loadData,accelerationActions,acceRuleSettingActions,acceBlackList,acceDelSql,modelActions,segmentActions,editAggGroup,delAggIdx,bulidIndex,tableIndexActions,changePartition,changeBuildType" },
        { "key": "systemUser-autoMaintain-admin", "value": "sourceManagement,loadSource,delSourceTable,sampleSourceTable,reloadSourceTable,loadData,accelerationActions,acceRuleSettingActions,acceBlackList,acceDelSql" },

        { "key": "systemUser-manualMaintain-management", "value": "loadData,accelerationActions,modelActions,segmentActions,editAggGroup,delAggIdx,bulidIndex,tableIndexActions,changePartition,changeBuildType" },
        { "key": "systemUser-autoMaintain-management", "value": "loadData,accelerationActions" },

        { "key": "systemUser-manualMaintain-operation", "value": "loadData,segmentActions,bulidIndex" },
        { "key": "systemUser-autoMaintain-operation", "value": "loadData" },

        { "key": "systemUser-*-read", "value": "none" }
      ]
    },
    // 模型操作权限
    "modelActions": {
      "keyPattern": "groupRole-projectRole",
      "entries": [
        { "key": "systemAdmin-*", "value": "dataLoad,importMDX,exportTDS,exportMDX,rename,clone,delete,purge,offline,online,deleteIndex" },
        { "key": "systemUser-admin", "value": "dataLoad,importMDX,exportTDS,exportMDX,rename,clone,delete,purge,offline,online,deleteIndex" },
        { "key": "systemUser-management", "value": "dataLoad,importMDX,exportTDS,exportMDX,rename,clone,delete,purge,offline,online,deleteIndex" },
        { "key": "systemUser-operation", "value": "purge" },
        { "key": "systemUser-read", "value": "none" }
      ]
    },
    // 任务模块操作权限
    "monitorActions": {
      "keyPattern": "groupRole-projectRole",
      "entries": [
        { "key": "systemAdmin-*", "value": "jobActions,diagnostic" },
        { "key": "systemUser-[admin,management,operation]", "value": "jobActions" },
        { "key": "systemUser-read", "value": "none" }
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
        { "key": "systemAdmin-*", "value": "addProject,deleteProject,editProject,backUpProject,accessActions,executeModelsMetadata" },
        { "key": "systemUser-admin", "value": "editProject,backUpProject,accessActions,executeModelsMetadata" },
        { "key": "systemUser-[management,operation,read]", "value": "none" }
      ]
    },
    "settingActions": {
      "keyPattern": "groupRole",
      "entries": [
        { "key": "systemAdmin", "value": "yarnQueue" },
        { "key": "systemUser", "value": "none" }
      ]
    },
    "metadataActions": {
      "keyPattern": "groupRole-projectRole-projectType",
      "entries": [
        { "key": "*-*-autoMaintain", "value": "none" },
        { "key": "systemAdmin-*-*", "value": "executeModelMetadata" },
        { "key": "systemUser-[admin,management]-*", "value": "executeModelMetadata" },
        { "key": "systemUser-[operation,read]-*", "value": "none" }
      ]
    }
  }
}