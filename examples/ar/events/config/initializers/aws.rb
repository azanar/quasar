require 'aws'

suffix    = (Quasar.env.production? or Quasar.env.sandbox?) ? 'files' : "files-#{Quasar.env}"
S3_BUCKET = "S3_BUCKET"

AWS.config(
  access_key_id:     'ACCESS_KEY_ID',
  secret_access_key: 'SECRET_ACCESS_KEY',
  stub_requests:     Quasar.env.test?,
)

