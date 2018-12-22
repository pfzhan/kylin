export function _getAccelerationSettings (data) {
  return {
    project: data.project,
    threshold: data.threshold,
    batch_enabled: data.batch_enabled,
    auto_apply: data.auto_apply
  }
}

export function _getJobAlertSettings (data, isArrayDefaultValue) {
  let errorJobEmails = data.job_error_notification_emails
  let emptyJobEamils = data.data_load_empty_notification_emails

  if (isArrayDefaultValue) {
    !errorJobEmails.length && errorJobEmails.push('')
    !emptyJobEamils.length && emptyJobEamils.push('')
  }

  return {
    project: data.project,
    job_error_notification_enabled: data.job_error_notification_enabled,
    job_error_notification_emails: errorJobEmails,
    data_load_empty_notification_enabled: data.data_load_empty_notification_enabled,
    data_load_empty_notification_emails: emptyJobEamils
  }
}
