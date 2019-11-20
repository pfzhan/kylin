export const validate = {
  'positiveNumber' (rule, value, callback) {
    if (value === '' || value === undefined || value <= 0) {
      callback(new Error(null))
    } else {
      callback()
    }
  },
  'validateYarnName' (rule, value, callback) {
    if (!value) {
      callback(rule.message[0])
    } else if (rule.type !== 'string') {
      callback(rule.message[1])
    } else {
      callback()
    }
  }
}

export function _getAccelerationSettings (data) {
  return {
    project: data.project,
    threshold: data.threshold,
    tips_enabled: data.tips_enabled
  }
}

export function _getJobAlertSettings (data, isArrayDefaultValue) {
  let jobEmails = [...data.job_notification_emails]

  if (isArrayDefaultValue) {
    !jobEmails.length && jobEmails.push('')
  }

  return {
    project: data.project,
    job_error_notification_enabled: data.job_error_notification_enabled,
    data_load_empty_notification_enabled: data.data_load_empty_notification_enabled,
    job_notification_emails: jobEmails
  }
}

export function _getDefaultDBSettings (data) {
  return {
    project: data.project,
    defaultDatabase: data.default_database
  }
}

export function _getYarnNameSetting (data) {
  return {
    project: data.project,
    yarn_queue: data.yarn_queue
  }
}
