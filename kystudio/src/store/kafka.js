import api from './../service/api'
import * as types from './types'
export default {
  state: {
  },
  mutations: {
  },
  actions: {
    [types.GET_CLUSTER_INFO]: function ({ commit }, kafka) {
      return api.kafka.getCusterTopic(kafka)
    },
    [types.GET_TOPIC_INFO]: function ({ commit }, topic) {
      return api.kafka.getTopicInfo(topic)
    },
    [types.SAVE_SAMPLE_DATA]: function ({ commit }, data) {
      return api.kafka.saveSampleData(data.tableName, data.sampleData, data.project)
    },
    [types.SAVE_KAFKA]: function ({ commit }, kafka) {
      return api.kafka.saveKafka(kafka)
    },
    [types.GET_CONFIG]: function ({ commit }, tableName) {
      return api.kafka.getConfig(tableName)
    },
    [types.GET_KAFKA_CONFIG]: function ({ commit }, tableName) {
      return api.kafka.getKafkaConfig(tableName)
    },
    [types.LOAD_KAFKA_SAMPLEDATA]: function ({ commit }, para) {
      return api.kafka.loadKafkaSampleData(para.tableName, para.project)
    },
    [types.LOAD_STREAMING_CONFIG]: function ({ commit }, tableName) {
      return api.kafka.getStreamingConfig(tableName)
    },
    [types.UPDATE_KAFKA]: function ({ commit }, kafka) {
      return api.kafka.updateKafka(kafka)
    }
  },
  getters: {}
}

