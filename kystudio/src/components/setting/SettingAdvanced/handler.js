export const validate = {
  'positiveNumber' (rule, value, callback) {
    if (value === '' || value === undefined || value <= 0) {
      callback(new Error(null))
    } else {
      callback()
    }
  }
}

export function _getAccelerationSettings (data) {
  return {
    project: data.project,
    threshold: data.threshold,
    batch_enabled: data.batch_enabled,
    auto_apply: data.auto_apply
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
