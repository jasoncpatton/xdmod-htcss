# Overview

XDMoD requires data to be aggregated in four different intervals, day, month, quater and year.  All aggregate data must use
the same timezone (typically the local timezone). The ACCESS XDMoD instance uses Eastern Time.

The fields required are listed below:

| Field Short Name   | Description |
| ------------ | ----------- |
| date         | date for data in this record in YYYY-MM-DD format. For month, quater and year this would be the date of the first day in the period. For example 2024-04-01 for the second quater of 2024. | 
| resource     | The identifier of the compute resource where the job ran | 
| resource institution | The identifier of the institution or facility where the job ran |
| systemaccount  | The username the job ran under. Usernames are not expected to be globally unique but must be unique for a given resource |
| field of science | The name of the field of science of the project that the job was associated. ACCESS XDMoD use the fields of science that PIs must select from a from a fixed list |
| project | The name of the project or project identifier | 
| project lead/PI | The identifier of the project lead (PI) | 
| project lead/PI institution | The institution affiliation of the project lead (PI). Institution names in ACCESS XDMoD come from the fixed list that users must select when they register for an ACCESS account |
| person | The identifier of the person who ran the job. |
| person institution | The institution affiliation of the person who ran the job |
| processor_count | The number of CPUs a job was allocated |
| gpu_count  | The number of GPUs a job was allocated |
| wallduration  | The wallduration of the jobs that were running during this period in seconds. This should only count the walltime of the jobs that ran during this day. A job that runs for multiple days will have its walltime assigned to each day. For example, a job that runs from March 14 6:00PM to March 15 8:00AM will have 6 hours on March 14th and 8 hours on March 15th
| waitduration  | The sum of all wait times for jobs started during the time period, in seconds. Where the wait time is the difference in time between sumbit and start.|
| submitted_job_count | The number of jobs that were submitted in this time period |
| ended_job_count | The number of jobs that completed execution in this time period |
| started_job_count | The number of jobs that started execution in this time period |
| running_job_count | The number of jobs that were running in this time period |

This data should be grouped by _dimension_: date, resource, resource institution, systemaccount, field of science, project, project lead/PI,  project lead/PI institution, person, person institution, processor_count, gpu_count.

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



