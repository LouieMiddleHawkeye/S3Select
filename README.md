# S3Select

See Open API Documentation at http://localhost:9069/S3Select/swagger-ui.html

If you use the `gimme-aws-creds` to get an okta profile credentials the app will use those

### Notes

- Files bigger than 1MB seem to break s3 select (so querying things like joints might be impossible)