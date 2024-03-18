# Data exchange schema

XDMoD requires data to be aggregated in four different intervals, day, month, quater and year.  The timezone used for the aggregate periods does matter - typically
XDMoD install use the local timezone. The ACCESS XDMoD instance uses eastern time zone.

The fields required are listed below:

| Field Short Name   | Description |
| ------------ | ----------- |
| date         | date for data in this record in YYYY-MM-DD format. For month, quater and year this would be the date of the first day in the period. For example 2024-04-01 for the second quater of 2024. | 
| resource     | The name of the compute resource where the job ran | 
| resource institution | The name of the institution or facility where the job ran |
| systemaccount  | The username the job ran under. Usernames are not expected to be globally unique but must be unique for a given resource |
| field of science | The name of the field of science of the project that the job was associated. ACCESS XDMoD use the fields of science that PIs must select from a from a fixed list |
| project | The name of the project or project identifier | 
| project lead/PI | The name of the project lead (PI) | 
| project lead/PI institution | The institution affiliation of the project lead (PI). Institution names in ACCESS XDMoD come from the fixed list that users must select when they register for an ACCESS account |
| person | The name of the person who ran the job. |
| person institution | The institution affiliation of the person who ran the job |
| processor_count | The number of CPUs a job was allocated |
| gpu_count  | The number of GPUs a job was allocated |
| wallduration  | The wallduration of the jobs that were running during this period in seconds. This should only count the walltime of the jobs that ran during this day. A job that runs for multiple days will have its walltime assigned to each day. For example, a job that runs from March 14 6:00PM to March 15 8:00AM will have 6 hours on March 14th and 8 hours on March 15th
| waitduration  | The amount of time jobs waited to execute during this day, in seconds |
| submitted_job_count | The number of jobs that were submitted in this time period |
| ended_job_count | The number of jobs that completed execution in this time period |
| started_job_count | The number of jobs that started execution in this time period |
| running_job_count | The number of jobs that were running in this time period |

This data should be grouped by date, resource, resource institution, systemaccount, field of science, project, person, person institution, processor_count, gpu_count.

