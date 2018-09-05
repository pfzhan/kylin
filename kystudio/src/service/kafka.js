import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getCusterTopic: (kafka) => {
    return Vue.resource(apiUrl + 'kafka').save(kafka)
  },
  getTopicInfo: (topic) => {
    return Vue.resource(apiUrl + 'kafka/' + topic.cluster + '/' + topic.name).save(topic.kafka)
  },
  saveSampleData: (tableName, sampleData, project) => {
    return Vue.resource(apiUrl + 'kafka/' + project + '/' + tableName + '/samples').save(sampleData)
  },
  saveKafka: (kafka) => {
    return Vue.resource(apiUrl + 'streaming').save(kafka)
  },
  updateKafka: (kafka) => {
    return Vue.resource(apiUrl + 'streaming').update(kafka)
  },
  getConfig: (tableName, project) => {
    return Vue.resource(apiUrl + 'streaming/getConfig').get({table: tableName, project: project})
  },
  getKafkaConfig: (tableName, project) => {
    return Vue.resource(apiUrl + 'streaming/getKfkConfig').get({kafkaConfigName: tableName, project: project})
  },
  loadKafkaSampleData: (tableName, project) => {
    return Vue.resource(apiUrl + 'kafka/' + project + '/' + tableName + '/update_samples').get()
  },
  getStreamingConfig: (tableName, project) => {
    return Vue.resource(apiUrl + 'streaming/getConfig').get({table: tableName, project: project})
  }
}
