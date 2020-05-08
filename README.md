# kafka-connect-api-client


This project contains PollableClientAPI implementations for [Pollable Source Connector](https://github.com/vrudenskyi/kafka-connect-pollable-source)


 ## azure-api-client
 | Client | Description
 |---|---
 | AzureLogAnalyticsClient|Client for Azure Log Analytics REST API: https://dev.loganalytics.io/documentation/Using-the-API
 |AzureEventHubClient | Client for Azure EventHub Receiver
 | AzureADAuditLogsClient| Client for Azure Active Directory Audit API: https://developer.microsoft.com/en-us/graph/docs/api-reference/beta/resources/directoryaudit
 |AzureActivityAPIClient | Client for Azure Activity Logs: [https://docs.microsoft.com/en-us/rest/api/monitor/activitylogs/list](https://docs.microsoft.com/en-us/rest/api/monitor/activitylogs/list)


  ## gcp-api-client
  | Client | Description
 |---|---
 |FindingsAPIClient | Client for reading findings from Google Cloud Security Command Center: https://cloud.google.com/security-command-center/docs/how-to-api-list-findings
 |GSuiteAdminReportsAPIClient | Client for polling GSuite Admin activities: https://developers.google.com/admin-sdk/reports/v1/reference/activities/list

  ## http-api-client
 Client to poll date from an http endpoint

 ## incapsula_logs-api-client
 | Client | Description
 |---|---
 |IncapulaLogsPollableClient | Client for Retrieve Imperva access and event logs from the Imperva cloud: https://docs.imperva.com/bundle/cloud-application-security/page/settings/log-integration.htm

 ## jira-api-client
 | Client | Description
 |---|---
 |JiraAPIClient| Client for poll issues using Jira REST API

 ## lastpass_ent-api-client
 Client for polling  lastpass enterprise logs

 ## o365_reports-api-client
 | Client | Description
 |---|---
 |O365ReportSvcPollableClient| Client O365 reporting service: https://docs.microsoft.com/en-us/previous-versions/office/developer/o365-enterprise-developers/jj984335%28v%3doffice.15%29

 ## okta_logs-api-client
 | Client | Description
 |---|---
 |OktaSystemLogAPIClient| Client for polling Okta security events: https://developer.okta.com/docs/reference/api/system-log/#list-events

 ## salesforce-api-client
| Client | Description
|---|---
|EventLogPollableAPIClient|  Client for polling Salesforce EventLogs
|SobjectPollableAPIClient| Client for polling Saleforce sObjects

 ## splunk-api-client
 | Client | Description
 |---|---
 |SplunkPollableAPIClient| Client for polling data from Splunk reports




>
>more clients  and more comprehensive documentation will follow

