// project actions mutations
export const LOAD_PROJECT_LIST = 'LOAD_PROJECT_LIST'
export const SAVE_PROJECT_LIST = 'SAVE_PROJECT_LIST'
export const DELETE_PROJECT = 'DELETE_PROJECT'
export const UPDATE_PROJECT = 'UPDATE_PROJECT'
export const SAVE_PROJECT = 'SAVE_PROJECT'
export const SAVE_PROJECT_ACCESS = 'SAVE_PROJECT_ACCESS'
export const EDIT_PROJECT_ACCESS = 'EDIT_PROJECT_ACCESS'
export const GET_PROJECT_ACCESS = 'GET_PROJECT_ACCESS'
export const DEL_PROJECT_ACCESS = 'DEL_PROJECT_ACCESS'
export const ADD_PROJECT_FILTER = 'ADD_PROJECT_FILTER'
export const EDIT_PROJECT_FILTER = 'EDIT_PROJECT_FILTER'
export const DEL_PROJECT_FILTER = 'DEL_PROJECT_FILTER'
export const GET_PROJECT_FILTER = 'GET_PROJECT_FILTER'
export const CACHE_ALL_PROJECTS = 'CACHE_ALL_PROJECTS'
export const LOAD_ALL_PROJECT = 'LOAD_ALL_PROJECT'
export const BACKUP_PROJECT = 'BACKUP_PROJECT'
export const REMOVE_ALL_PROJECTS = 'REMOVE_ALL_PROJECTS'
export const RESET_PROJECT_STATE = 'RESET_PROJECT_STATE'
export const ACCESS_AVAILABLE_USER_OR_GROUP = 'ACCESS_AVAILABLE_USER_OR_GROUP'
export const GET_QUOTA_INFO = 'GET_QUOTA_INFO'
export const SET_QUOTA_INFO = 'SET_QUOTA_INFO'
export const CLEAR_TRASH = 'CLEAR_TRASH'
export const SET_PROJECT = 'SET_PROJECT'
export const FETCH_PROJECT_SETTINGS = 'FETCH_PROJECT_SETTINGS'
export const CACHE_PROJECT_TIPS_CONFIG = 'CACHE_PROJECT_TIPS_CONFIG'
export const UPDATE_PROJECT_GENERAL_INFO = 'UPDATE_PROJECT_GENERAL_INFO'
export const UPDATE_SEGMENT_CONFIG = 'UPDATE_SEGMENT_CONFIG'
export const UPDATE_PUSHDOWN_CONFIG = 'UPDATE_PUSHDOWN_CONFIG'
export const CACHE_PROJECT_PUSHDOWN_CONFIG = 'CACHE_PROJECT_PUSHDOWN_CONFIG'
export const UPDATE_STORAGE_QUOTA = 'UPDATE_STORAGE_QUOTA'
export const UPDATE_ACCELERATION_SETTINGS = 'UPDATE_ACCELERATION_SETTINGS'
export const UPDATE_JOB_ALERT_SETTINGS = 'UPDATE_JOB_ALERT_SETTINGS'
export const UPDATE_PROJECT_DATASOURCE = 'UPDATE_PROJECT_DATASOURCE'
export const RESET_PROJECT_CONFIG = 'RESET_PROJECT_CONFIG'
export const UPDATE_DEFAULT_DB_SETTINGS = 'UPDATE_DEFAULT_DB_SETTINGS'
export const CACHE_PROJECT_DEFAULT_DB = 'CACHE_PROJECT_DEFAULT_DB'
export const UPDATE_PROJECT_SEMI_AUTOMATIC_STATUS = 'UPDATE_PROJECT_SEMI_AUTOMATIC_STATUS'
export const UPDATE_YARN_QUEUE = 'UPDATE_YARN_QUEUE'
export const GET_ACL_PERMISSION = 'GET_ACL_PERMISSION'
export const CHECK_SSB = 'CHECK_SSB'
export const IMPORT_SSB_DATABASE = 'IMPORT_SSB_DATABASE'
export const UPDATE_EXPOSE_CC_CONFIG = 'UPDATE_EXPOSE_CC_CONFIG'
// datasource actions mutations
// csv 数据源
export const VERIFY_CSV_CONN = 'VERIFY_CSV_CONN'
export const GET_CSV_SAMPLE = 'GET_CSV_SAMPLE'
export const SAVE_CSV_INFO = 'SAVE_CSV_INFO'
export const LOAD_CSV_SCHEME = 'LOAD_CSV_SCHEME'
export const VERIFY_CSV_SQL = 'VERIFY_CSV_SQL'
// csv 数据源
export const RELOAD_DATASOURCE = 'RELOAD_DATASOURCE'
export const GET_RELOAD_INFLUENCE = 'GET_RELOAD_INFLUENCE'
export const LOAD_DASHBOARD_QUERY_INFO = 'LOAD_DASHBOARD_QUERY_INFO'
export const LOAD_QUERY_CHART_DATA = 'LOAD_QUERY_CHART_DATA'
export const LOAD_QUERY_DURA_CHART_DATA = 'LOAD_QUERY_DURA_CHART_DATA'
export const FETCH_DB_AND_TABLES = 'FETCH_DB_AND_TABLES'
export const FETCH_DATABASES = 'FETCH_DATABASES'
export const FETCH_TABLES = 'FETCH_TABLES'
export const DELETE_TABLE = 'DELETE_TABLE'
export const SUBMIT_SAMPLING = 'SUBMIT_SAMPLING'
export const HAS_SAMPLING_JOB = 'HAS_SAMPLING_JOB'
export const UPDATE_TOP_TABLE = 'UPDATE_TOP_TABLE'
export const LOAD_DATASOURCE = 'LOAD_DATASOURCE'
export const CACHE_DATASOURCE = 'CACHE_DATASOURCE'
export const REPLACE_TABLE_CACHE = 'REPLACE_TABLE_CACHE'
export const DELETE_TABLE_CACHE = 'DELETE_TABLE_CACHE'
export const LOAD_DATASOURCE_EXT = 'LOAD_DATASOURCE_EXT'
export const LOAD_HIVEBASIC_DATABASE_TABLES = 'LOAD_HIVEBASIC_DATABASE_TABLES'
export const LOAD_HIVEBASIC_DATABASE = 'LOAD_HIVEBASIC_DATABASE'
export const LOAD_HIVE_TABLES = 'LOAD_HIVE_TABLES'
export const LOAD_HIVE_IN_PROJECT = 'LOAD_HIVE_IN_PROJECT'
export const UN_LOAD_HIVE_IN_PROJECT = 'UN_LOAD_HIVE_IN_PROJECT'
export const LOAD_BUILD_COMPLETE_TABLES = 'LOAD_BUILD_COMPLETE_TABLES'
export const QUERY_BUILD_TABLES = 'QUERY_BUILD_TABLES'
export const SAVE_QUERY = 'SAVE_QUERY'
export const GET_SAVE_QUERIES = 'GET_SAVE_QUERIES'
export const GET_HISTORY_LIST = 'GET_HISTORY_LIST'
export const GET_FAVORITE_LIST = 'GET_FAVORITE_LIST'
export const GET_CANDIDATE_LIST = 'GET_CANDIDATE_LIST'
export const VALIDATE_WHITE_SQL = 'VALIDATE_WHITE_SQL'
export const ADD_TO_FAVORITE_LIST = 'ADD_TO_FAVORITE_LIST'
export const REMOVE_FAVORITE_SQL = 'REMOVE_FAVORITE_SQL'
export const LOAD_BLACK_LIST = 'LOAD_BLACK_LIST'
export const DELETE_BLACK_SQL = 'DELETE_BLACK_SQL'
export const GET_RULES_IMPACT = 'GET_RULES_IMPACT'
export const GET_PREFERRENCE = 'GET_PREFERRENCE'
export const GET_RULES = 'GET_RULES'
export const UPDATE_RULES = 'UPDATE_RULES'
export const GET_USER_AND_GROUPS = 'GET_USER_AND_GROUPS'
export const UPDATE_PREFERRENCE = 'UPDATE_PREFERRENCE'
export const DELETE_QUERY = 'DELETE_QUERY'
export const GET_ENCODINGS = 'GET_ENCODINGS'
export const GET_ENCODINGMATCHS = 'GET_ENCODINGMATCHS'
export const CACHE_ENCODINGS = 'CACHE_ENCODINGS'
export const CACHE_ENCODINGMATCHS = 'CACHE_ENCODINGMATCHS'
export const GET_ENCODINGS_BY_COLUMNS = 'GET_ENCODINGS_BY_COLUMNS'
export const COLLECT_SAMPLE_DATA = 'COLLECT_SAMPLE_DATA'
export const GET_TABLE_JOB = 'GET_TABLE_JOB'
export const LOAD_KAFKA_SAMPLEDATA = 'LOAD_KAFKA_SAMPLEDATA'
export const LOAD_STREAMING_CONFIG = 'LOAD_STREAMING_CONFIG'
export const SAVED_QUERIES = 'SAVED_QUERIES'
export const GET_ACL_BLACKLIST_TABLE = 'GET_ACL_BLACKLIST_TABLE'
export const SET_CURRENT_TABLE = 'SET_CURRENT_TABLE'
export const UPDATE_SOURCE_TYPE = 'UPDATE_SOURCE_TYPE'
export const SAVE_TABLE_PARTITION = 'SAVE_TABLE_PARTITION'
export const SAVE_DATA_RANGE = 'SAVE_DATA_RANGE'
export const FETCH_SEGMENTS = 'FETCH_SEGMENTS'
export const FETCH_AGGREGATES = 'FETCH_AGGREGATES'
export const FETCH_CUBOID = 'FETCH_CUBOID'
export const FETCH_CUBOIDS = 'FETCH_CUBOIDS'
export const FETCH_RELATED_MODELS = 'FETCH_RELATED_MODELS'
export const GET_AGG_CUBOIDS = 'GET_AGG_CUBOIDS'
export const FRESH_RANGE_DATA = 'FRESH_RANGE_DATA'
export const PREPARE_UNLOAD = 'PREPARE_UNLOAD'
export const FETCH_CHANGE_TYPE_INFO = 'FETCH_CHANGE_TYPE_INFO'
export const FETCH_RANGE_FRESH_INFO = 'FETCH_RANGE_FRESH_INFO'
export const FETCH_MERGE_CONFIG = 'FETCH_MERGE_CONFIG'
export const UPDATE_MERGE_CONFIG = 'UPDATE_MERGE_CONFIG'
export const FETCH_PUSHDOWN_CONFIG = 'FETCH_PUSHDOWN_CONFIG'
export const DISCARD_TABLE_MODEL = 'DISCARD_TABLE_MODEL'
export const FETCH_NEWEST_TABLE_RANGE = 'FETCH_NEWEST_TABLE_RANGE'
export const SET_QUERY_TABS = 'SET_QUERY_TABS'
export const RESET_QUERY_TABS = 'RESET_QUERY_TABS'
export const FETCH_BATCH_LOAD_TABLES = 'FETCH_BATCH_LOAD_TABLES'
export const SAVE_TABLES_BATCH_LOAD = 'SAVE_TABLES_BATCH_LOAD'
export const SAVE_SOURCE_CONFIG = 'SAVE_SOURCE_CONFIG'
export const IMPORT_SQL_FILES = 'IMPORT_SQL_FILES'
export const GET_WAITING_ACCE_SIZE = 'GET_WAITING_ACCE_SIZE'
export const RELOAD_HIVE_DB_TABLES = 'RELOAD_HIVE_DB_TABLES'
export const FETCH_PARTITION_FORMAT = 'FETCH_PARTITION_FORMAT'

export const GET_ACCESS_DETAILS_BY_USER = 'GET_ACCESS_DETAILS_BY_USER'
export const SUBMIT_ACCESS_DATA = 'SUBMIT_ACCESS_DATA'
// acl table
export const GET_ACL_SET_TABLE = 'GET_ACL_SET_TABLE'
export const SAVE_ACL_SET_TABLE = 'SAVE_ACL_SET_TABLE'
export const DEL_ACL_SET_TABLE = 'DEL_ACL_SET_TABLE'
// acl column
export const GET_ACL_SET_COLUMN = 'GET_ACL_SET_COLUMN'
export const SAVE_ACL_SET_COLUMN = 'SAVE_ACL_SET_COLUMN'
export const UPDATE_ACL_SET_COLUMN = 'UPDATE_ACL_SET_COLUMN'
export const DEL_ACL_SET_COLUMN = 'DEL_ACL_SET_COLUMN'
export const GET_ACL_WHITELIST_COLUMN = 'GET_ACL_WHITELIST_COLUMN'
// acl row
export const GET_ACL_SET_ROW = 'GET_ACL_SET_ROW'
export const SAVE_ACL_SET_ROW = 'SAVE_ACL_SET_ROW'
export const UPDATE_ACL_SET_ROW = 'UPDATE_ACL_SET_ROW'
export const DEL_ACL_SET_ROW = 'DEL_ACL_SET_ROW'
export const GET_ACL_WHITELIST_ROW = 'GET_ACL_WHITELIST_ROW'
export const PREVIEW_ACL_SET_ROW_SQL = 'PREVIEW_ACL_SET_ROW_SQL'

// model actions mutations
export const LOAD_ONLINE_QUERY_NODES = 'LOAD_ONLINE_QUERY_NODES'
export const LOAD_MODEL_LIST = 'LOAD_MODEL_LIST'
export const LOCK_SPEED_INFO = 'LOCK_SPEED_INFO'
export const GET_SPEED_INFO = 'GET_SPEED_INFO'
export const CACHE_SPEED_INFO = 'CACHE_SPEED_INFO'
export const APPLY_SPEED_INFO = 'APPLY_SPEED_INFO'
export const IGNORE_SPEED_INFO = 'IGNORE_SPEED_INFO'
export const PURGE_MODEL = 'PURGE_MODEL'
export const RENAME_MODEL = 'RENAME_MODEL'
export const DISABLE_MODEL = 'DISABLE_MODEL'
export const ENABLE_MODEL = 'ENABLE_MODEL'
export const SAVE_MODEL_LIST = 'SAVE_MODEL_LIST'
export const SUGGEST_DIMENSION_MEASURE = 'SUGGEST_DIMENSION_MEASURE'
export const LOAD_MODEL_INFO = 'LOAD_MODEL_INFO'
export const DELETE_MODEL = 'DELETE_MODEL'
export const CLONE_MODEL = 'CLONE_MODEL'
export const SAVE_MODEL = 'SAVE_MODEL'
export const SAVE_MODEL_DRAFT = 'SAVE_MODEL_DRAFT'
export const UPDATE_MODEL = 'UPDATE_MODEL'
export const CACHE_MODEL_EDIT = 'CACHE_MODEL_EDIT'
export const COLLECT_MODEL_STATS = 'COLLECT_MODEL_STATS'
export const DIAGNOSE = 'DIAGNOSE'
export const DIAGNOSELIST = 'DIAGNOSELIST'
export const CACHE_MODEL_DIANOSELIST = 'CACHE_MODEL_DIANOSELIST'
export const CHECK_MODELNAME = 'CHECK_MODELNAME'
export const GET_USED_COLS = 'GET_USED_COLS'
export const LOAD_ALL_MODEL = 'LOAD_ALL_MODEL'
export const GET_MODEL_PROGRESS = 'GET_MODEL_PROGRESS'
export const GET_MODEL_ACCESS = 'GET_MODEL_ACCESS'
export const GET_MODEL_END_ACCESS = 'GET_MODEL_END_ACCESS'
export const CACHE_MODEL_END_ACCESS = 'CACHE_MODEL_END_ACCESS'
export const CACHE_MODEL_ACCESS = 'CACHE_MODEL_ACCESS'
export const MODEL_CHECKABLE = 'MODEL_CHECKABLE'
export const GET_COLUMN_SAMPLEDATA = 'GET_COLUMN_SAMPLEDATA'
export const VALID_PARTITION_COLUMN = 'VALID_PARTITION_COLUMN'
export const CHECK_COMPUTED_EXPRESSION = 'CHECK_COMPUTED_EXPRESSION'
export const GET_COMPUTED_COLUMNS = 'GET_COMPUTED_COLUMNS'
export const VERIFY_MODEL_SQL = 'VERIFY_MODEL_SQL'
export const AUTO_MODEL = 'AUTO_MODEL'
export const VALID_AUTOMODEL_SQL = 'VALID_AUTOMODEL_SQL'
export const SUGGEST_IS_BY_ANSWERED = 'SUGGEST_IS_BY_ANSWERED'
export const GET_AUTOMODEL_SQL = 'GET_AUTOMODEL_SQL'
export const REFRESH_SEGMENTS = 'REFRESH_SEGMENTS'
export const DELETE_SEGMENTS = 'DELETE_SEGMENTS'
export const MERGE_SEGMENTS = 'MERGE_SEGMENTS'
export const MODEL_DATA_CHECK = 'MODEL_DATA_CHECK'
export const MODEL_BUILD = 'MODEL_BUILD'
export const MODEL_FULLLOAD_BUILD = 'MODEL_FULLLOAD_BUILD'
export const MODEL_PARTITION_SET = 'MODEL_PARTITION_SET'
export const FETCH_AGGREGATE_GROUPS = 'FETCH_AGGREGATE_GROUPS'
export const UPDATE_AGGREGATE_GROUPS = 'UPDATE_AGGREGATE_GROUPS'
export const FETCH_RELATED_MODEL_STATUS = 'FETCH_RELATED_MODEL_STATUS'
export const LOAD_MODEL_CONFIG_LIST = 'LOAD_MODEL_CONFIG_LIST'
export const UPDATE_MODEL_CONFIG = 'UPDATE_MODEL_CONFIG'
export const GET_MODEL_NEWEST_RANGE = 'GET_MODEL_NEWEST_RANGE'
export const GET_MODEL_JSON = 'GET_MODEL_JSON'
export const GET_MODEL_SQL = 'GET_MODEL_SQL'
export const CLEAR_DATASOURCE_CACHE = 'CLEAR_DATASOURCE_CACHE'
export const ADD_AGG_INDEX_ADVANCED = 'ADD_AGG_INDEX_ADVANCED'
export const GET_AGG_INDEX_ADVANCED = 'GET_AGG_INDEX_ADVANCED'
export const CHECK_FILTER_CONDITION = 'CHECK_FILTER_CONDITION'
export const GET_INDEX_DIFF = 'GET_INDEX_DIFF'
export const AUTO_FIX_SEGMENT_HOLES = 'AUTO_FIX_SEGMENT_HOLES'
export const CHECK_DATA_RANGE = 'CHECK_DATA_RANGE'
export const GET_MODELS_METADATA_STRUCTURE = 'GET_MODELS_METADATA_STRUCTURE'
export const DOWNLOAD_MODELS_METADATA = 'DOWNLOAD_MODELS_METADATA'
export const UPLOAD_MODEL_METADATA_FILE = 'UPLOAD_MODEL_METADATA_FILE'
export const IMPORT_MODEL_METADATA_FILE = 'IMPORT_MODEL_METADATA_FILE'
// table index
export const GET_TABLE_INDEX = 'GET_TABLE_INDEX'
export const EDIT_TABLE_INDEX = 'EDIT_TABLE_INDEX'
export const DELETE_TABLE_INDEX = 'DELETE_TABLE_INDEX'
export const ADD_TABLE_INDEX = 'ADD_TABLE_INDEX'
export const BUILD_INDEX = 'BUILD_INDEX'
export const GET_MODEL_RECOMMENDATIONS = 'GET_MODEL_RECOMMENDATIONS'
export const ADOPT_MODEL_RECOMMENDATIONS = 'ADOPT_MODEL_RECOMMENDATIONS'
export const CLEAR_MODEL_RECOMMENDATIONS = 'CLEAR_MODEL_RECOMMENDATIONS'
export const GET_AGG_INDEX_CONTENTLIST = 'GET_AGG_INDEX_CONTENTLIST'
export const GET_TABLE_INDEX_CONTENTLIST = 'GET_TABLE_INDEX_CONTENTLIST'
export const GET_INDEX_CONTENTLIST = 'GET_INDEX_CONTENTLIST'
export const LOAD_ALL_INDEX = 'LOAD_ALL_INDEX'
export const DELETE_INDEX = 'DELETE_INDEX'
export const FETCH_INDEX_GRAPH = 'FETCH_INDEX_GRAPH'

// cube actions mutations
export const CHECK_SEGMENTS = 'CHECK_SEGMENTS'
export const GET_CUBES_LIST = 'GET_CUBES_LIST'
export const GET_CUBES_SEGMENTS_LIST = 'GET_CUBES_SEGMENTS_LIST'
export const SAVE_CUBES_LIST = 'SAVE_CUBES_LIST'
export const LOAD_CUBE_DESC = 'LOAD_CUBE_DESC'
export const SAVE_CUBE_DESC = 'SAVE_CUBE_DESC'
export const GET_CUBE_SQL = 'GET_CUBE_SQL'
export const GET_COLUMNAR_INFO = 'GET_COLUMNAR_INFO'
export const GET_HBASE_INFO = 'GET_HBASE_INFO'
export const GET_CUBE_SEGMENTS = 'GET_CUBE_SEGMENTS'
export const GET_MP_VALUES = 'GET_MP_VALUES'
export const UPDATE_CUBE_SEGMENTS = 'UPDATE_CUBE_SEGMENTS'
export const DELETE_CUBE = 'DELETE_CUBE'
export const REBUILD_CUBE = 'REBUILD_CUBE'
export const REBUILD_STREAMING_CUBE = 'REBUILD_STREAMING_CUBE'
export const ENABLE_CUBE = 'ENABLE_CUBE'
export const DISABLE_CUBE = 'DISABLE_CUBE'
export const PURGE_CUBE = 'PURGE_CUBE'
export const CLONE_CUBE = 'CLONE_CUBE'
export const BACKUP_CUBE = 'BACKUP_CUBE'
export const UPDATE_CUBE = 'UPDATE_CUBE'
export const SAVE_CUBE = 'SAVE_CUBE'
export const CHECK_CUBE_NAME_AVAILABILITY = 'CHECK_CUBE_NAME_AVAILABILITY'
export const CAL_CUBOID = 'CAL_CUBOID'
export const GET_ENCODING = 'GET_ENCODING'
export const GET_ENCODING_VERSION = 'GET_ENCODING_VERSION'
export const GET_RAW_TABLE = 'GET_RAW_TABLE'
export const DELETE_RAW_TABLE = 'DELETE_RAW_TABLE'
export const SAVE_RAW_TABLE = 'SAVE_RAW_TABLE'
export const UPDATE_RAW_TABLE = 'UPDATE_RAW_TABLE'
export const SAVE_SAMPLE_SQL = 'SAVE_SAMPLE_SQL'
export const GET_SAMPLE_SQL = 'GET_SAMPLE_SQL'
export const GET_CUBE_SUGGESTIONS = 'GET_CUBE_SUGGESTIONS'
export const GET_SQL_DIMENSIONS = 'GET_SQL_DIMENSIONS'
export const GET_SCHEDULER = 'GET_SCHEDULER'
export const UPDATE_SCHEDULER = 'UPDATE_SCHEDULER'
export const DELETE_SCHEDULER = 'DELETE_SCHEDULER'
export const DRAFT_CUBE = 'DRAFT_CUBE'
export const SAVE_CUBE_ACCESS = 'SAVE_CUBE_ACCESS'
export const EDIT_CUBE_ACCESS = 'EDIT_CUBE_ACCESS'
export const GET_CUBE_ACCESS = 'GET_CUBE_ACCESS'
export const GET_CUBE_END_ACCESS = 'GET_CUBE_END_ACCESS'
export const CACHE_CUBE_END_ACCESS = 'CACHE_CUBE_END_ACCESS'
export const DEL_CUBE_ACCESS = 'DEL_CUBE_ACCESS'
export const CACHE_CUBE_ACCESS = 'CACHE_CUBE_ACCESS'
export const GET_CUBE_DIMENSIONS = 'GET_CUBE_DIMENSIONS'
export const CACHE_RAWTABLE__BASEDATA = 'CACHE_RAWTABLE__BASEDATA'
export const CHECK_SQL = 'CHECK_SQL'
export const VERIFY_CUBE_SQL = 'VERIFY_CUBE_SQL'
// config actions mutations
export const LOAD_DEFAULT_CONFIG = 'LOAD_DEFAULT_CONFIG'
export const SAVE_DEFAULT_CONFIG = 'SAVE_DEFAULT_CONFIG'
export const SAVE_HIDDEN_FEATURE = 'SAVE_HIDDEN_FEATURE'
export const TOGGLE_SCREEN = 'TOGGLE_SCREEN'
export const TOGGLE_MENU = 'TOGGLE_MENU'
export const CACHE_HISTORY = 'CACHE_HISTORY'

// kafka actions mutations
export const GET_CLUSTER_INFO = 'GET_CLUSTER_INFO'
export const GET_TOPIC_INFO = 'GET_TOPIC_INFO'
export const SAVE_SAMPLE_DATA = 'SAVE_SAMPLE_DATA'
export const SAVE_KAFKA = 'SAVE_KAFKA'
export const GET_CONFIG = 'GET_CONFIG'
export const GET_KAFKA_CONFIG = 'GET_KAFKA_CONFIG'
export const UPDATE_KAFKA = 'UPDATE_KAFKA'

// system actions mutations
export const LOAD_USERS_LIST = 'LOAD_USERS_LIST'
export const SAVE_USERS_LIST = 'SAVE_USERS_LIST'
export const UPDATE_STATUS = 'UPDATE_STATUS'
export const SAVE_USER = 'SAVE_USER'
export const RESET_PASSWORD = 'RESET_PASSWORD'
export const EDIT_ROLE = 'EDIT_ROLE'
export const REMOVE_USER = 'REMOVE_USER'
export const SAVE_AUTHENTICATION = 'SAVE_AUTHENTICATION'
export const LOAD_AUTHENTICATION = 'LOAD_AUTHENTICATION'
export const GET_ENV = 'GET_ENV'
export const SAVE_ENV = 'SAVE_ENV'
export const GET_CONF = 'GET_CONF'
export const GET_INSTANCE_CONF = 'GET_INSTANCE_CONF'
export const SAVE_ADMIN_CONF = 'SAVE_ADMIN_CONF'
export const GET_ADMIN_CONFIG = 'GET_ADMIN_CONFIG'
export const GET_CONF_BY_NAME = 'GET_CONF_BY_NAME'
export const SAVE_INSTANCE_CONF_BY_NAME = 'SAVE_INSTANCE_CONF_BY_NAME'
export const SAVE_CONF = 'SAVE_CONF'
export const RELOAD_METADATA = 'RELOAD_METADATA'
export const BACKUP_METADATA = 'BACKUP_METADATA'
export const UPDATE_CONFIG = 'UPDATE_CONFIG'
export const USER_AUTHENTICATION = 'USER_AUTHENTICATION'
export const GET_ABOUTKAP = 'GET_ABOUTKAP'
export const GET_ABOUT = 'GET_ABOUT'
export const GET_KYBOT_UPLOAD = 'GET_KYBOT_UPLOAD'
export const GET_KYBOT_DUMP = 'GET_KYBOT_DUMP'
export const GET_JOB_KYBOT = 'GET_JOB_KYBOT'
export const SAVE_LICENSE_CONTENT = 'SAVE_LICENSE_CONTENT'
export const SAVE_LICENSE_FILE = 'SAVE_LICENSE_FILE'
export const TRIAL_LICENSE_FILE = 'TRIAL_LICENSE_FILE'
export const USER_ACCESS = 'USER_ACCESS'
export const SAVE_CURRENT_USER_ACCESS = 'SAVE_CURRENT_USER_ACCESS'
export const ADD_USERS_TO_GROUP = 'ADD_USERS_TO_GROUP'
export const ADD_GROUPS_TO_USER = 'ADD_GROUPS_TO_USER'
export const GET_GROUP_LIST = 'GET_GROUP_LIST'
export const GET_GROUP_USERS_LIST = 'GET_GROUP_USERS_LIST'
export const SAVE_GROUP_USERS_LIST = 'SAVE_GROUP_USERS_LIST'
export const ADD_GROUP = 'ADD_GROUP'
export const DEL_GROUP = 'DEL_GROUP'
export const GET_USERS_BY_GROUPNAME = 'GET_USERS_BY_GROUPNAME'
export const SAVE_SERVICE_STATE = 'SAVE_SERVICE_STATE'
export const GET_SERVICE_STATE = 'GET_SERVICE_STATE'
export const GET_CANARY_REPORT = 'GET_CANARY_REPORT'
export const SAVE_CANARY_REPORT = 'SAVE_CANARY_REPORT'
export const INIT_SPEC = 'INIT_SPEC'
export const TOGGLE_LICENSE_DIALOG = 'TOGGLE_LICENSE_DIALOG'
// kybot account and upload
export const GET_KYACCOUNT = 'GET_KYACCOUNT'
export const GET_KYSTATUS = 'GET_KYSTATUS'
export const GET_KYBOT_ACCOUNT = 'GET_KYBOT_ACCOUNT'
export const LOGIN_KYBOT = 'LOGIN_KYBOT'
export const LOGOUT_KYBOT = 'LOGOUT_KYBOT'
export const GET_CUR_ACCOUNTNAME = 'GET_CUR_ACCOUNTNAME'
export const GET_KYBOT_SERVERS = 'GET_KYBOT_SERVERS'
// monitor actions mutations
export const SUGGEST_MODEL = 'SUGGEST_MODEL'
export const SAVE_SUGGEST_MODELS = 'SAVE_SUGGEST_MODELS'
export const VALIDATE_MODEL_NAME = 'VALIDATE_MODEL_NAME'
export const LOAD_JOBS_LIST = 'LOAD_JOBS_LIST'
export const GET_JOB_DETAIL = 'GET_JOB_DETAIL'
export const SAVE_JOBS_LIST = 'SAVE_JOBS_LIST'
export const LOAD_WAITTING_JOB_MODELS = 'LOAD_WAITTING_JOB_MODELS'
export const LOAD_WAITTING_JOBS_BY_MODEL = 'LOAD_WAITTING_JOBS_BY_MODEL'
export const LOAD_SLOW_QUERIES = 'LOAD_SLOW_QUERIES'
export const SAVE_SLOW_QUERIES = 'SAVE_SLOW_QUERIES'
export const LOAD_STEP_OUTPUTS = 'LOAD_STEP_OUTPUTS'
export const RESUME_JOB = 'RESUME_JOB'
export const RESTART_JOB = 'RESTART_JOB'
export const PAUSE_JOB = 'PAUSE_JOB'
export const DISCARD_JOB = 'DISCARD_JOB'
export const REMOVE_JOB = 'REMOVE_JOB'
export const ROMOVE_JOB_FOR_ALL = 'ROMOVE_JOB_FOR_ALL'
export const LOAD_PUSHDOWN_QUERIES = 'LOAD_PUSHDOWN_QUERIES'
export const SAVE_PUSHDOWN_QUERIES = 'SAVE_PUSHDOWN_QUERIES'
export const EXPORT_PUSHDOWN = 'EXPORT_PUSHDOWN'
export const RESET_MONITOR_STATE = 'RESET_MONITOR_STATE'
export const IS_CLOUD = 'IS_CLOUD'

export const LOGIN = 'LOGIN'
export const LOGIN_OUT = 'LOGIN_OUT'
export const SAVE_CURRENT_LOGIN_USER = 'SAVE_CURRENT_LOGIN_USER'
export const RESET_CURRENT_USER = 'RESET_CURRENT_USER'

export const FORMAT_SQL = 'FORMAT_SQL'
export const LOAD_QUERY_HISTORY = 'LOAD_QUERY_HISTORY'
export const SAVE_QUERY_HISTORY = 'SAVE_QUERY_HISTORY'
export const AUTO_CHECK_SQL = 'AUTO_CHECK_SQL'
export const AUTO_QUERY_PATTERNS = 'AUTO_QUERY_PATTERNS'
export const AUTO_MUTIL_MODELS = 'AUTO_MUTIL_MODELS'
export const AUTO_SAVE_MODELS = 'AUTO_SAVE_MODELS'
export const AUTO_MODELING_JOBS = 'AUTO_MODELING_JOBS'
export const AUTO_BUILD_MODELS = 'AUTO_BUILD_MODELS'
export const AUTO_JOBS_LIST = 'AUTO_JOBS_LIST'
export const LOAD_STATISTICS = 'LOAD_STATISTICS'
export const LOAD_DASHBOARD_JOB_INFO = 'LOAD_DASHBOARD_JOB_INFO'
export const LOAD_JOB_CHART_DATA = 'LOAD_JOB_CHART_DATA'
export const LOAD_JOB_BULID_CHART_DATA = 'LOAD_JOB_BULID_CHART_DATA'

export const SHOW_MODAL = 'SHOW_MODAL'
export const HIDE_MODAL = 'HIDE_MODAL'
export const SET_MODAL = 'SET_MODAL'
export const SET_MODAL_FORM = 'SET_MODAL_FORM'
export const CALL_MODAL = 'CALL_MODAL'
export const INIT_MODAL = 'INIT_MODAL'
