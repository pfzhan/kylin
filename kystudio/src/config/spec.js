/* eslint-disable */
export default {
  "allOptionMaps": {
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
    "role": [
      { "id": "admin", "value": 16, "title": "Admin" },
      { "id": "management", "value": 32, "title": "Management" },
      { "id": "operation", "value": 64, "title": "Operation" },
      { "id": "read", "value": 1, "title": "Read" },

      { "id": "admin", "value": "ROLE_ADMIN", "title": "Admin" },
      { "id": "admin", "value": "ROLE_ANALYST", "title": "Admin" },
      { "id": "admin", "value": "ROLE_MODELER", "title": "Admin" }
    ],
    "modelType": [
      { "id": "tableOriented", "value": "TABLE_ORIENTED", "title": "Table Oriented" },
      { "id": "modelBased", "value": "MODEL_BASED", "title": "Model Based" }
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
    "menu": {
      "keyPattern": "projectType-role",
      "entries": [
        { "key": "manualMaintain-[admin,management]", "value": "none" },
        { "key": "manualMaintain-operation", "value": "user,group" },
        { "key": "manualMaintain-read", "value": "job,user,group" },
        { "key": "autoMaintain-[admin,management]", "value": "none" },
        { "key": "autoMaintain-operation", "value": "user,group" },
        { "key": "autoMaintain-read", "value": "job,user,group" }
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
    "aggregateActions": {
      "keyPattern": "projectType",
      "entries": [
        { "key": "manualMaintain", "value": "viewAggGroup,editAggGroup,addAggGroup" },
        { "key": "autoMaintain", "value": "none" }
      ]
    }
  }
}