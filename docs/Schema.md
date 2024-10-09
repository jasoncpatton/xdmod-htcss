# Overview

XDMoD requires data to be aggregated in four different intervals, day, month, quater and year.  

This data should be grouped by _dimension_: date, resource, resource institution, systemaccount, field of science, project, project lead/PI,  project lead/PI institution, person, person institution, processor_count, gpu_count.  The other data should be aggregated.

The fields required are listed below:

| Field Short Name  | Is a dimension  | Description |
| ----------------- | --- | ----------- |
| aggregation_unit  |    | The aggregation granularity for this record. It should be one of the following values, day, month, quarter, or year
| start_date              | ✅︎  | The start date for data in this record in YYYY-MM-DD format. For month, quater and year this would be the date of the first day in the period. For example 2024-04-01 for the second quater of 2024. All aggregate data in an XDMoD database must use the same timezone (typically the local timezone). The ACCESS XDMoD instance uses Eastern Time. |
| end_date              | ✅︎  | The end date for data in this record in YYYY-MM-DD format. For month, quater and year this would be the date of the last day in the period. For example 2024-06-30 for the second quater of 2024. All aggregate data in an XDMoD database must use the same timezone (typically the local timezone). The ACCESS XDMoD instance uses Eastern Time. |
| resource          | ✅︎  | The identifier of the compute resource where the jobs ran |
| resource institution  | ✅︎ | The identifier of the institution or facility where the jobs ran |
| system account    | ✅︎ | The username the jobs ran under. Usernames are not expected to be globally unique but must be unique for a given resource |
| field of science  | ✅︎ | The name of the field of science of the project that the jobs were associated. ACCESS XDMoD uses the fields of science that PIs must select from a from a fixed list |
| project           | ✅︎ | The name of the project or project identifier |
| project lead/PI   | ✅︎ | The identifier of the project lead (PI) |
| project lead/PI institution  | ✅︎ | The institution affiliation of the project lead (PI). Institution names in ACCESS XDMoD come from the fixed list that users must select when they register for an ACCESS account |
| person            | ✅︎ | The identifier of the person who ran the jobs. |
| person institution  | ✅︎ | The institution affiliation of the person who ran the jobs |
| processor_count   | ✅︎ | The number of CPUs that were allocated per job.|
| gpu_count         | ✅︎ | The number of GPUs that were allocated per job. |
| job_wall_time     | ✅︎ | A categorization of jobs into discrete groups based on the jobs wall time. The categories are 0-1 seconds, 1-30 seconds, 30 seconds - 30 minutes, 30-60 minutes, 1-5 hours, 5-10 hours, 10-18 hours, and 18+ hours.
| job_wait_time     | ✅︎ | A categorization of jobs into discrete groups based on the jobs wait time. The categories are 0-1 seconds, 1-30 seconds, 30 seconds - 30 minutes, 30-60 minutes, 1-5 hours, 5-10 hours, 10-18 hours, and 18+ hours.
| wallduration      |    | SUM() of the wallduration of the jobs that were running during this period in seconds. This should only count the walltime of the jobs that ran during this day. A job that runs for multiple days will have its walltime assigned to each day. For example, a job that runs from March 14 6:00PM to March 15 8:00AM will have 6 hours on March 14th and 8 hours on March 15th
| waitduration      |    | SUM() of all wait times for jobs started during the time period, in seconds. Where the wait time is the difference in time between sumbit and start.|
| submitted_job_count |  | SUM() of the number of jobs that were submitted |
| ended_job_count   |    | SUM() of the number of jobs that completed execution |
| started_job_count |    | SUM() of the number of jobs that started execution |
| running_job_count |    | SUM() of the number of jobs that were running  |

There is also additional information required for most dimension fields.

| Dimension Name | Information Required |
| ------------ | ----------- |
|  resource     | short and long names, resource specifications (node, core, gpu counts) |
| resource institution | short, long names, carnegie classification, location (state, country) |
| field of science |
| project | short, long names, project abstract, funding agency award number |
| person | first, middle, last names, email address, ORCiD, academic status (undergraduate, postgrad, research staff, etc.) |
| person institution | short, long names, carnegie classification, location state, location country |
| project lead/PI | _same as person_ |
| project lead/PI institution | _same as person instituion_ |

Every field could have a value of "N/A" meaning that field is not available or "Unknown"
meaning that the value was not reported. For example, government labs do not have a Carnegie classification
so the institution record for a Gov lab would have "N/A" in the Carnegie classification field.
If a user did not tell us their ORCiD then the field would be "Unknown" (if they did
tell us that they didn't have an ORCiD then it would be "N/A").

XDMoD is designed for long-term historical data analysis and is able to track changes to
resource sizes over time, changes to a persons academic status and institution over time, etc.

# Data exchange format

XDMoD supports ingestion of data from a wide variety of sources including relational
databases (MySQL, PostgreSQL), object databases (MongoDB), REST interfaces, and files in a variety
formats JSON, csv, XML, etc.

For file-based ingestion, the JSON data-interchange format is preferred for files because of its wide support and well-defined
data encoding mechanism.
