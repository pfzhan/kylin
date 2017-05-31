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
export const CACHE_PROJECT_ACCESS = 'CACHE_PROJECT_ACCESS'
export const CACHE_ALL_PROJECTS = 'CACHE_ALL_PROJECTS'
export const LOAD_ALL_PROJECT = 'LOAD_ALL_PROJECT'
export const BACKUP_PROJECT = 'BACKUP_PROJECT'
// datasource actions mutations
export const LOAD_DATASOURCE = 'LOAD_DATASOURCE'
export const CACHE_DATASOURCE = 'CACHE_DATASOURCE'
export const LOAD_DATASOURCE_EXT = 'LOAD_DATASOURCE_EXT'
export const LOAD_HIVEBASIC_DATABASE = 'LOAD_HIVEBASIC_DATABASE'
export const LOAD_HIVE_TABLES = 'LOAD_HIVE_TABLES'
export const LOAD_HIVE_IN_PROJECT = 'LOAD_HIVE_IN_PROJECT'
export const UN_LOAD_HIVE_IN_PROJECT = 'UN_LOAD_HIVE_IN_PROJECT'
export const LOAD_BUILD_COMPLETE_TABLES = 'LOAD_BUILD_COMPLETE_TABLES'
export const QUERY_BUILD_TABLES = 'QUERY_BUILD_TABLES'
export const SAVE_QUERY = 'SAVE_QUERY'
export const GET_SAVE_QUERIES = 'GET_SAVE_QUERIES'
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
// model actions mutations
export const LOAD_MODEL_LIST = 'LOAD_MODEL_LIST'
export const SAVE_MODEL_LIST = 'SAVE_MODEL_LIST'
export const SUGGEST_DIMENSION_MEASURE = 'SUGGEST_DIMENSION_MEASURE'
export const LOAD_MODEL_INFO = 'LOAD_MODEL_INFO'
export const DELETE_MODEL = 'DELETE_MODEL'
export const CLONE_MODEL = 'CLONE_MODEL'
export const SAVE_MODEL = 'SAVE_MODEL'
export const SAVE_MODEL_DRAFT = 'SAVE_MODEL_DRAFT'
export const CACHE_UPDATE_MODEL_EDIT = 'CACHE_UPDATE_MODEL_EDIT'
export const CACHE_MODEL_EDIT = 'CACHE_MODEL_EDIT'
export const COLLECT_MODEL_STATS = 'COLLECT_MODEL_STATS'
export const DIAGNOSE = 'DIAGNOSE'
export const DIAGNOSELIST = 'DIAGNOSELIST'
export const CACHE_MODEL_DIANOSELIST = 'CACHE_MODEL_DIANOSELIST'
export const CHECK_MODELNAME = 'CHECK_MODELNAME'
export const GET_USED_COLS = 'GET_USED_COLS'
export const LOAD_ALL_MODEL = 'LOAD_ALL_MODEL'
export const GET_MODEL_PROGRESS = 'GET_MODEL_PROGRESS'
// cube actions mutations
export const GET_CUBES_LIST = 'GET_CUBES_LIST'
export const SAVE_CUBES_LIST = 'SAVE_CUBES_LIST'
export const LOAD_CUBE_DESC = 'LOAD_CUBE_DESC'
export const SAVE_CUBE_DESC = 'SAVE_CUBE_DESC'
export const GET_CUBE_SQL = 'GET_CUBE_SQL'
export const GET_COLUMNAR_INFO = 'GET_COLUMNAR_INFO'
export const GET_HBASE_INFO = 'GET_HBASE_INFO'
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
export const GET_SCHEDULER = 'GET_SCHEDULER'
export const UPDATE_SCHEDULER = 'UPDATE_SCHEDULER'
export const DELETE_SCHEDULER = 'DELETE_SCHEDULER'
export const DRAFT_CUBE = 'DRAFT_CUBE'
export const SAVE_CUBE_ACCESS = 'SAVE_CUBE_ACCESS'
export const EDIT_CUBE_ACCESS = 'EDIT_CUBE_ACCESS'
export const GET_CUBE_ACCESS = 'GET_CUBE_ACCESS'
export const DEL_CUBE_ACCESS = 'DEL_CUBE_ACCESS'
export const CACHE_CUBE_ACCESS = 'CACHE_CUBE_ACCESS'
// config actions mutations
export const LOAD_DEFAULT_CONFIG = 'LOAD_DEFAULT_CONFIG'
export const SAVE_DEFAULT_CONFIG = 'SAVE_DEFAULT_CONFIG'
export const LOAD_HIDDEN_FEATURE = 'LOAD_HIDDEN_FEATURE'
export const SAVE_HIDDEN_FEATURE = 'SAVE_HIDDEN_FEATURE'

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
export const GET_CONF_BY_NAME = 'GET_CONF_BY_NAME'
export const SAVE_CONF = 'SAVE_CONF'
export const RELOAD_METADATA = 'RELOAD_METADATA'
export const BACKUP_METADATA = 'BACKUP_METADATA'
export const UPDATE_CONFIG = 'UPDATE_CONFIG'
export const USER_AUTHENTICATION = 'USER_AUTHENTICATION'
export const GET_TIMEZONE = 'GET_TIMEZONE'
export const GET_ABOUTKAP = 'GET_ABOUTKAP'
export const GET_ABOUT = 'GET_ABOUT'
export const GET_KYBOT_UPLOAD = 'GET_KYBOT_UPLOAD'
export const GET_KYBOT_DUMP = 'GET_KYBOT_DUMP'
export const GET_JOB_KYBOT = 'GET_JOB_KYBOT'

// kybot account and upload
export const GET_KYACCOUNT = 'GET_KYACCOUNT'
export const GET_KYSTATUS = 'GET_KYSTATUS'
export const GET_KYBOT_ACCOUNT = 'GET_KYBOT_ACCOUNT'
export const LOGIN_KYBOT = 'LOGIN_KYBOT'
export const GET_KYBOT_STATUS = 'GET_KYBOT_STATUS'
export const START_KYBOT = 'START_KYBOT'
export const STOP_KYBOT = 'STOP_KYBOT'
export const GET_AGREEMENT = 'GET_AGREEMENT'
export const SET_AGREEMENT = 'SET_AGREEMENT'

// monitor actions mutations
export const LOAD_JOBS_LIST = 'LOAD_JOBS_LIST'
export const SAVE_JOBS_LIST = 'SAVE_JOBS_LIST'
export const LOAD_SLOW_QUERIES = 'LOAD_SLOW_QUERIES'
export const SAVE_SLOW_QUERIES = 'SAVE_SLOW_QUERIES'
export const LOAD_STEP_OUTPUTS = 'LOAD_STEP_OUTPUTS'
export const RESUME_JOB = 'RESUME_JOB'
export const CANCEL_JOB = 'CANCEL_JOB'
export const PAUSE_JOB = 'PAUSE_JOB'
export const REMOVE_JOB = 'REMOVE_JOB'

export const LOGIN = 'LOGIN'
export const LOGIN_OUT = 'LOGIN_OUT'
export const SAVE_CURRENT_LOGIN_USER = 'SAVE_CURRENT_LOGIN_USER'

