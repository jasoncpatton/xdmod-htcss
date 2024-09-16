import sys
import re
import json
import argparse
import time
import shutil
import pickle
import math
from pprint import pprint
from collections import deque
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from urllib.error import HTTPError
from pathlib import Path
from tempfile import NamedTemporaryFile
from os import fsync
from typing import List, Dict, Set, Union

from elasticsearch import Elasticsearch
import htcondor


OSPOOL_ES_INDEX = "osg-schedd-*"
OSPOOL_APS = {
    "ap20.uc.osg-htc.org",
    "ap2007.chtc.wisc.edu",
    "ap21.uc.osg-htc.org",
    "ap22.uc.osg-htc.org",
    "ap23.uc.osg-htc.org",
    "ap40.uw.osg-htc.org",
    "ap41.uw.osg-htc.org",
    "ap42.uw.osg-htc.org",
    "ap7.chtc.wisc.edu",
    "ap7.chtc.wisc.edu@ap2007.chtc.wisc.edu",
    "ce1.opensciencegrid.org",
    "comses.sol.rc.asu.edu",
    "condor.scigap.org",
    "descmp3.cosmology.illinois.edu",
    "gremlin.phys.uconn.edu",
    "htcss-dev-ap.ospool.opensciencegrid.org",
    "huxley-osgsub-001.sdmz.amnh.org",
    "lambda06.rowan.edu",
    "login-el7.xenon.ci-connect.net",
    "login-test.osgconnect.net",
    "login.ci-connect.uchicago.edu",
    "login.collab.ci-connect.net",
    "login.duke.ci-connect.net",
    "login.snowmass21.io",
    "login.veritas.ci-connect.net",
    "login04.osgconnect.net",
    "login05.osgconnect.net",
    "mendel-osgsub-001.sdmz.amnh.org",
    "nsgosg.sdsc.edu",
    "os-ce1.opensciencegrid.org",
    "os-ce1.osgdev.chtc.io",
    "osg-prp-submit.nautilus.optiputer.net",
    "osg-vo.isi.edu",
    "ospool-eht.chtc.wisc.edu",
    "xd-submit0000.chtc.wisc.edu",
    "testbed",
}
OSPOOL_COLLECTORS = {
    "cm-1.ospool.osg-htc.org",
    "cm-2.ospool.osg-htc.org",
    "flock.opensciencegrid.org",
}
NON_OSPOOL_RESOURCES = {
    "SURFsara",
    "NIKHEF-ELPROD",
    "INFN-T1",
    "IN2P3-CC",
    "UIUC-ICC-SPT",
    "TACC-Frontera-CE2",
}

TOPOLOGY_PROJECT_DATA_URL = "https://topology.opensciencegrid.org/miscproject/xml"
TOPOLOGY_RESOURCE_DATA_URL = "https://topology.opensciencegrid.org/rgsummary/xml"

VALID_PERIODS = ["day", "month", "quarter", "year"]


def valid_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date string, should match format YYYY-MM-DD: {date_str}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--end", type=valid_date, help="Ending date, in YYYY-MM-DD format", required=True)
    parser.add_argument("--period", type=str, choices=VALID_PERIODS, help="Period to accumulate metrics over", required=True)
    parser.add_argument("--lookback", type=int, default=28, help="Minimum number of days to look back (default: %(default)s)")
    parser.add_argument("--output-dir", type=Path, help="Directory to store JSON output", required=True)
    parser.add_argument("--compute-buckets", action="store_true", help="Compute waittime and walltime buckets")
    args = parser.parse_args()
    return args


def get_topology_project_data(cache_file=Path("./topology_project_data.pickle")) -> Dict[str, str]:
    if cache_file.exists() and cache_file.stat().st_mtime > time.time() - 23*3600:
        try:
            projects_map = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
        else:
            return projects_map
    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(TOPOLOGY_PROJECT_DATA_URL) as xml:
                xmltree = ET.parse(xml)
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries:
                raise
        else:
            break
    projects = xmltree.getroot()
    projects_map = {
        "unknown": {
            "name": "Unknown",
            "pi": "Unknown",
            "pi_institution": "Unknown",
            "field_of_science": "Unknown",
            "id": "Unknown",
            "pi_institution_id": "Unknown",
            "field_of_science_id": "Unknown",
        }
    }

    for project in projects:
        project_map = {}
        project_map["name"] = project.find("Name").text
        project_map["pi"] = project.find("PIName").text
        project_map["pi_institution"] = project.find("Organization").text
        project_map["field_of_science"] = project.find("FieldOfScience").text
        project_map["id"] = project.find("ID").text
        project_map["pi_institution_id"] = project.find("InstitutionID").text
        project_map["field_of_science_id"] = project.find("FieldOfScienceID").text
        projects_map[project_map["name"].lower()] = project_map.copy()

    pickle.dump(projects_map, cache_file.open("wb"))
    return projects_map


def get_topology_resource_data(cache_file=Path("./topology_resource_data.pickle")) -> Dict[str, str]:
    if cache_file.exists() and cache_file.stat().st_mtime > time.time() - 23*3600:
        try:
            resources_map = pickle.load(cache_file.open("rb"))
        except Exception:
            pass
        else:
            return resources_map
    tries = 0
    max_tries = 5
    while tries < max_tries:
        try:
            with urlopen(TOPOLOGY_RESOURCE_DATA_URL) as xml:
                xmltree = ET.parse(xml)
        except HTTPError:
            time.sleep(2**tries)
            tries += 1
            if tries == max_tries:
                raise
        else:
            break
    resource_groups = xmltree.getroot()
    resources_map = {
        "unknown": {
            "institution": "Unknown",
            "institution_id": "Unknown",
            "name": "Unknown",
            "id": "Unknown",
        }
    }

    for resource_group in resource_groups:
        resource_institution = resource_group.find("Facility").find("Name").text
        resource_institution_id = resource_group.find("Facility").find("ID").text

        resources = resource_group.find("Resources")
        for resource in resources:
            resource_map = {}
            resource_map["institution"] = resource_institution
            resource_map["institution_id"] = resource_institution_id
            resource_map["name"] = resource.find("Name").text
            resource_map["id"] = resource.find("ID").text
            resources_map[resource_map["name"].lower()] = resource_map.copy()

    pickle.dump(resources_map, cache_file.open("wb"))
    return resources_map


def get_ospool_aps() -> Set[str]:
    current_ospool_aps = set()
    for collector_host in OSPOOL_COLLECTORS:
        try:
            collector = htcondor.Collector(collector_host)
            aps = collector.query(htcondor.AdTypes.Schedd, projection=["Machine", "CollectorHost"])
        except Exception:
            continue
        for ap in aps:
            if set(re.split(r"[\s,]+", ap["CollectorHost"])) & OSPOOL_COLLECTORS:
                current_ospool_aps.add(ap["Machine"])
    return current_ospool_aps | OSPOOL_APS


def get_query(
        index: str,
        period_start_ts: int,
        period_end_ts: int,
        final_aggs = {},
        extra_mappings = {},
        extra_filters = [],
        compute_buckets = False,
        add_end_date = False,
        add_start_date = False,
    ) -> dict:
    query = {
        "index": index,
        "size": 0,
        "track_scores": False,
        "track_total_hits": False,
        "_source": False,
        "runtime_mappings": {
            "ResourceName": {
                "type": "keyword",
                "script": {
                    "language": "painless",
                    "source": """
                        String res;
                        if (doc.containsKey("MachineAttrGLIDEIN_ResourceName0") && doc["MachineAttrGLIDEIN_ResourceName0.keyword"].size() > 0) {
                            res = doc["MachineAttrGLIDEIN_ResourceName0.keyword"].value;
                        } else if (doc.containsKey("MATCH_EXP_JOBGLIDEIN_ResourceName") && doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].size() > 0) {
                            res = doc["MATCH_EXP_JOBGLIDEIN_ResourceName.keyword"].value;
                        } else {
                            res = "Unknown";
                        }
                        emit(res);
                        """,
                },
            },
        },
        "aggs": {
            "gpu_count": {
                "terms": {
                    "field": "RequestGpus",
                    "missing": 0,
                    "size": 8,
                },
                "aggs": {
                    "processor_count": {
                        "terms": {
                            "field": "RequestCpus",
                            "missing": 1,
                            "size": 64,
                        },
                        "aggs": {
                            "system_account": {
                                "terms": {
                                    "field": "Owner.keyword",
                                    "missing": "Unknown",
                                    "size": 512,
                                },
                                "aggs": {
                                    "project": {
                                        "terms": {
                                            "field": "ProjectName.keyword",
                                            "missing": "Unknown",
                                            "size": 512,
                                        },
                                        "aggs": {
                                            "resource": {
                                                "terms": {
                                                    "field": "ResourceName",
                                                    "size": 512,
                                                },
                                                "aggs": final_aggs,
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        "query": {
            "bool": {
                "filter": [
                    {"range": {
                        "RecordTime": {
                            "gte": period_start_ts,
                            "lt": period_end_ts,
                        }
                    }},
                    {"term": {
                        "JobUniverse": 5,
                    }},
                    {"range": {
                        "RemoteWallClockTime": {
                            "gt": 0,
                        }
                    }},
                    {"range": {
                        "NumJobStarts": {
                            "gt": 0,
                        }
                    }},
                ],
                "minimum_should_match": 1,
                "should" : [
                    {"bool": {
                        "filter": [
                            {"terms": {
                                "ScheddName.keyword": list(get_ospool_aps()),
                            }},
                        ],
                        "must_not": [
                            {"exists": {
                                "field": "LastRemotePool",
                            }},
                        ],
                    }},
                    {"terms": {
                        "LastRemotePool.keyword": list(OSPOOL_COLLECTORS),
                    }},
                ],
                "must_not": [
                    {"terms": {
                        "ResourceName": list(NON_OSPOOL_RESOURCES),
                    }},
                ],
            }
        }
    }
    for mapping_name, mapping in extra_mappings.items():
        query["runtime_mappings"][mapping_name] = mapping
    for filt in extra_filters:
        query["query"]["bool"]["filter"].append(filt)

    if add_start_date or compute_buckets:
        start_date_mapping = {
            "JobThisStartDate": {
                "type": "long",
                "script": {
                    "language": "painless",
                    "source": """
                        long res;
                        if (doc.containsKey("JobCurrentStartDate") && doc["JobCurrentStartDate"].size() > 0) {
                            res = (long)doc["JobCurrentStartDate"].value;
                        } else if (doc.containsKey("JobStartDate") && doc["JobStartDate"].size() > 0) {
                            res = (long)doc["JobStartDate"].value;
                        } else if (doc.containsKey("JobLastStartDate") && doc["JobLastStartDate"].size() > 0) {
                            res = (long)doc["JobLastStartDate"].value;
                        } else if (doc.containsKey("EnteredCurrentStatus") && doc["EnteredCurrentStatus"].size() > 0) {
                            res = (long)doc["EnteredCurrentStatus"].value;
                        } else {
                            res = (long)doc["QDate"].value;
                        }
                        emit(res);
                    """,
                },
            },
        }
        query["runtime_mappings"].update(start_date_mapping)

    if add_end_date or compute_buckets:
        end_date_mapping = {
            "JobThisEndDate": {
                "type": "long",
                "script": {
                    "language": "painless",
                    "source": """
                        long res;
                        if (doc.containsKey("CompletionDate") && doc["CompletionDate"].size() > 0 && (long)doc["CompletionDate"].value > 0) {
                            res = (long)doc["CompletionDate"].value;
                        } else if (doc.containsKey("JobCurrentFinishTransferOutputDate") && doc["JobCurrentFinishTransferOutputDate"].size() > 0) {
                            res = (long)doc["JobCurrentFinishTransferOutputDate"].value;
                        } else if (doc.containsKey("TransferOutFinished") && doc["TransferOutFinished"].size() > 0) {
                            res = (long)doc["TransferOutFinished"].value;
                        } else if (doc.containsKey("EnteredCurrentStatus") && doc["EnteredCurrentStatus"].size() > 0) {
                            res = (long)doc["EnteredCurrentStatus"].value;
                        } else {
                            res = (long)doc["RecordTime"].value;
                        }
                        emit(res);
                    """,
                },
            },
        }
        query["runtime_mappings"].update(end_date_mapping)

    if compute_buckets:
        bucket_runtime_mappings = {
            "JobWaitTimeBucket": {
                "type": "long",
                "script": {
                    "language": "painless",
                    "source": """
                    long waittime = 0;
                    if (doc.containsKey("QDate") && doc["QDate"].size() > 0) {
                        waittime = (long)doc["JobThisStartDate"].value - (long)doc["QDate"].value;
                    }

                    byte bucket = 0;
                    if        (waittime >= 18*3600) {
                        bucket = 7;
                    } else if (waittime >= 10*3600) {
                        bucket = 6;
                    } else if (waittime >= 5*3600) {
                        bucket = 5;
                    } else if (waittime >= 3600) {
                        bucket = 4;
                    } else if (waittime >= 30*60) {
                        bucket = 3;
                    } else if (waittime >= 30) {
                        bucket = 2;
                    } else if (waittime >= 1) {
                        bucket = 1;
                    }
                    emit(bucket);
                    """
                }
            },
            "JobWallTimeBucket": {
                "type": "long",
                "script": {
                    "language": "painless",
                    "source": """
                    long walltime = 0;
                    if (doc.containsKey("LastRemoteWallClockTime") && doc["LastRemoteWallClockTime"].size() > 0) {
                        walltime = (long)doc["LastRemoteWallClockTime"].value;
                    } else {
                        long starttime = (long)doc["JobThisStartDate"].value;
                        long endtime = (long)doc["JobThisEndDate"].value;
                        walltime = endtime - starttime;
                    }

                    byte bucket = 0;
                    if        (walltime >= 18*3600) {
                        bucket = 7;
                    } else if (walltime >= 10*3600) {
                        bucket = 6;
                    } else if (walltime >= 5*3600) {
                        bucket = 5;
                    } else if (walltime >= 3600) {
                        bucket = 4;
                    } else if (walltime >= 30*60) {
                        bucket = 3;
                    } else if (walltime >= 30) {
                        bucket = 2;
                    } else if (walltime >= 1) {
                        bucket = 1;
                    }
                    emit(bucket);
                    """
                }
            }
        }
        query["runtime_mappings"].update(bucket_runtime_mappings)
        child_aggs = query.pop("aggs")
        query["aggs"] = {
            "job_wall_time_bucket": {
                "terms": {
                    "field": "JobWallTimeBucket",
                    "size": 9,
                },
                "aggs": {
                    "job_wait_time_bucket": {
                        "terms": {
                            "field": "JobWaitTimeBucket",
                            "size": 9,
                        },
                        "aggs": child_aggs,
                    }
                }
            }
        }

    return query


def print_error(d: dict, depth=0):
    pre = depth*"\t"
    for k, v in d.items():
        if k == "failed_shards" and len(v) > 0:
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif k == "root_cause" and len(v) > 0:
            print(f"{pre}{k}:")
            print_error(v[0], depth=depth+1)
        elif isinstance(v, dict):
            print(f"{pre}{k}:")
            print_error(v, depth=depth+1)
        elif isinstance(v, list):
            nt = f"\n{pre}\t"
            print(f"{pre}{k}:\n{pre}\t{nt.join(v)}")
        else:
            print(f"{pre}{k}:\t{v}")


def get_keys_from_query(query: dict, keys=[]) -> List[str]:
    result = []
    if "aggs" in query:
        aggs = query["aggs"]
        for key, fields in aggs.items():
            if "terms" in fields:
                result.extend(get_keys_from_query(fields, keys + [key]))
            else:
                return keys
    return result


def flatten_aggs(
        agg: dict,
        ordered_keys: list,
        this_key="",
        this_row_key={}
    ) -> Dict[tuple, Dict[str, Union[int, float]]]:
    results = {}
    this_row_key = this_row_key.copy()
    if "key" in agg:
        this_row_key[this_key] = agg["key"]
    next_keys = set(agg.keys()) - {"doc_count", "key"}
    for next_key in next_keys:
        if "buckets" in agg[next_key]:
            for bucket in agg[next_key]["buckets"]:
                results.update(flatten_aggs(bucket, ordered_keys, next_key, this_row_key))
            return results
        this_row_key_tuple = tuple(this_row_key[k] for k in ordered_keys)
        if this_row_key_tuple not in results:
            results[this_row_key_tuple] = {}
        results[this_row_key_tuple][next_key] = agg[next_key]["value"]
    return results


def merge_flattened_aggs(
        aggs_total: Dict[tuple, Dict[str, Union[int, float]]],
        aggs_in: Dict[tuple, Dict[str, Union[int, float]]]
    ) -> Dict[tuple, Dict[str, Union[int, float]]]:
    for k, v in aggs_in.items():
        if k not in aggs_total:
            aggs_total[k] = v
        else:
            aggs_total[k].update(v)
    return aggs_total


def map_resources_and_projects(rows: deque) -> deque:
    resources_map = get_topology_resource_data()
    projects_map = get_topology_project_data()
    for row in rows:
        resource_map = resources_map.get(row["resource"].lower(), resources_map["unknown"])
        project_map = projects_map.get(row["project"].lower(), projects_map["unknown"])

        row["resource"] = resource_map["name"]
        row["resource_institution"] = resource_map["institution"]
        row["system_account"] = row["system_account"].lower()
        row["field_of_science"] = project_map["field_of_science"]
        row["project"] = project_map["name"]
        row["project_pi"] = project_map["pi"]
        row["project_pi_institution"] = project_map["pi_institution"]
        row["person"] = row["system_account"]
        row["person_institution"] = project_map["pi_institution"]

    return rows


def write_json_file(obj, path: Path, indent=None):
    with NamedTemporaryFile(mode="w", delete=False) as f:
        tmp_path = Path(f.name)
        json.dump(obj, f, indent=indent)
        f.flush()
        fsync(f.fileno())
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:  # try atomic move first
            tmp_path.rename(path)
        except OSError:
            shutil.move(tmp_path.as_posix(), path.as_posix())
    except OSError:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def get_period_start(dt_in: datetime, period: str) -> datetime:
    dt_out = dt_in - timedelta(days=1)
    if period == "month":
        dt_out = dt_out.replace(day=1)
    elif period == "quarter":
        quarter_month = (1, 4, 7, 10)[math.floor((dt_out.month - 1)/3)]
        dt_out = dt_out.replace(month=quarter_month, day=1)
    elif period == "year":
        dt_out = dt_out.replace(month=1, day=1)
    return dt_out


def get_period_end(dt_in: datetime, period: str) -> datetime:
    dt_out = dt_in
    dt_start = get_period_start(dt_in, period)
    if period == "month":
        dt_out = (dt_start + timedelta(days=32)).replace(day=1)
    elif period == "quarter":
        dt_out = (dt_start + timedelta(days=93)).replace(day=1)
    elif period == "year":
        dt_out = (dt_start + timedelta(days=367)).replace(day=1)
    return dt_out


def main():
    args = parse_args()
    print(f"\n{datetime.now()} - Starting up with arguments:")
    pprint(vars(args))

    period_end = args.end
    period_end_ts = int(period_end.timestamp())
    period_end_str = (period_end - timedelta(seconds=1)).strftime(r"%Y-%m-%dT%H:%M:%S")

    initial_start = get_period_start(period_end, args.period)
    lookback_min_date = initial_start - timedelta(days=args.lookback)
    date_ranges = [(initial_start, period_end,)]

    # Only backfill if we're less than args.lookback days from the start of the period
    if period_end < initial_start + timedelta(days=args.lookback):
        lookback_end = initial_start
        while lookback_end > lookback_min_date:
            lookback_start = get_period_start(lookback_end, args.period)
            date_ranges.append((lookback_start, lookback_end,))
            lookback_end = lookback_start

    period_start = date_ranges[-1][0]
    period_start_ts = int(period_start.timestamp())
    period_start_str = period_start.strftime(r"%Y-%m-%dT%H:%M:%S")

    initial_key = {
        "start_date": "",
        "end_date": "",
        "aggregation_unit": args.period,
    }

    total_days = (period_end - period_start).days
    timeout = int(60 * total_days**0.35)
    client = Elasticsearch(timeout=timeout)

    queries = {
        "submitted_job_count": {
            "final_aggs": {
                "submitted_job_count": {
                    "value_count": {
                        "field": "RecordTime",
                    },
                },
            },
            "range_attr": "QDate",
        },
        "ended_job_count": {
            "final_aggs": {
                "ended_job_count": {
                    "value_count": {
                        "field": "RecordTime",
                    },
                },
            },
            "range_attr": "JobThisEndDate",
            "add_end_date": True,
        },
        "started_job_count": {
            "final_aggs": {
                "started_job_count": {
                    "value_count": {
                        "field": "RecordTime",
                    },
                },
            },
            "range_attr": "JobThisStartDate",
            "add_start_date": True,
        },
        "waitduration": {
            "final_aggs": {
                "waitduration": {
                    "sum": {
                        "field": "PeriodWaitTime",
                    },
                },
            },
            "range_attr_left": "QDate",
            "range_attr_right": "JobThisStartDate",
            "add_start_date": True,
        },
        "wallduration": {
            "final_aggs": {
                "wallduration": {
                    "sum": {
                        "field": "PeriodWallTime",
                    },
                },
                "running_job_count": {
                    "value_count": {
                        "field": "RecordTime",
                    },
                },
            },
            "range_attr_left": "JobThisStartDate",
            "range_attr_right": "JobThisEndDate",
            "add_start_date": True,
            "add_end_date": True,
        },
    }

    flat_results = {}

    incomplete = True
    try:

        for i_date, (start, end) in enumerate(date_ranges):

            start_ts = int(start.timestamp())
            initial_key["start_date"] = start.strftime(r"%Y-%m-%d")

            end_ts = int(end.timestamp())
            log_end_date = get_period_end(end, args.period)
            initial_key["end_date"] = (log_end_date - timedelta(seconds=1)).strftime(r"%Y-%m-%d")

            for i_query, (query_name, query_params) in enumerate(queries.items()):

                this_query_params = query_params.copy()
                range_attr = this_query_params.pop("range_attr", None)
                range_attr_left = this_query_params.pop("range_attr_left", None)
                range_attr_right = this_query_params.pop("range_attr_right", None)

                extra_filters = []

                if range_attr is not None:
                    extra_filter = {"range": {
                            range_attr: {
                                "gte": start_ts,
                                "lt": end_ts,
                                },
                            },
                        }
                    extra_filters.append(extra_filter)

                if range_attr_left is not None:
                    extra_filter = {"range": {
                            range_attr_left: {
                                "lt": end_ts,
                                },
                            },
                        }
                    extra_filters.append(extra_filter)

                if range_attr_right is not None:
                    extra_filter = {"range": {
                            range_attr_right: {
                                "gte": start_ts,
                                },
                            },
                        }
                    extra_filters.append(extra_filter)

                if query_name == "wallduration":
                    source = f"""
                        long period_wall_time;
                        long job_start_time;
                        long job_end_time;
                        long job_total_wall_time;
                        long job_today_wall_time_left;
                        long job_today_wall_time_right;
                        long period_start_time = {start_ts};
                        long period_end_time = {end_ts};
                        long period_max_time = {end_ts - start_ts};
                        long tmp_min1;
                        long tmp_min2;

                        job_total_wall_time = period_max_time;
                        job_today_wall_time_left = period_max_time;
                        job_today_wall_time_right = period_max_time;

                        job_start_time = (long)doc["JobThisStartDate"].value;
                        job_end_time = (long)doc["JobThisEndDate"].value;
                        if (job_end_time >= job_start_time) {{
                            job_total_wall_time = job_end_time - job_start_time;
                        }}
                        if (doc.containsKey("LastRemoteWallClockTime") && doc["LastRemoteWallClockTime"].size() > 0) {{
                            job_total_wall_time = (long)doc["LastRemoteWallClockTime"].value;
                        }}
                        if (job_end_time >= period_start_time) {{
                            job_today_wall_time_left = job_end_time - period_start_time;
                        }}
                        if (period_end_time >= job_start_time) {{
                            job_today_wall_time_right = period_end_time - job_start_time;
                        }}
                        tmp_min1 = (long)Math.min(job_today_wall_time_left, job_today_wall_time_right);
                        tmp_min2 = (long)Math.min(job_total_wall_time, period_max_time);
                        period_wall_time = (long)Math.min(tmp_min1, tmp_min2);

                        emit(period_wall_time);
                        """
                    extra_mappings = {
                        "PeriodWallTime": {
                            "type": "long",
                            "script": {
                                "language": "painless",
                                "source": source,
                            },
                        },
                    }
                    this_query_params["extra_mappings"] = extra_mappings

                elif query_name == "waitduration":
                    source = f"""
                        long period_wait_time;
                        long job_q_time;
                        long job_start_time;
                        long job_total_wait_time;
                        long job_today_wait_time_left;
                        long job_today_wait_time_right;
                        long period_start_time = {start_ts};
                        long period_end_time = {end_ts};
                        long period_max_time = {end_ts - start_ts};
                        long tmp_min1;
                        long tmp_min2;

                        job_total_wait_time = period_max_time;
                        job_today_wait_time_left = period_max_time;
                        job_today_wait_time_right = period_max_time;

                        job_q_time = (long)doc["QDate"].value;
                        job_start_time = (long)doc["JobThisStartDate"].value;
                        if (job_start_time >= job_q_time) {{
                            job_total_wait_time = job_start_time - job_q_time;
                        }}
                        if (job_start_time >= period_start_time) {{
                            job_today_wait_time_left = job_start_time - period_start_time;
                        }}
                        if (period_end_time >= job_q_time) {{
                            job_today_wait_time_right = period_end_time - job_q_time;
                        }}

                        tmp_min1 = (long)Math.min(job_today_wait_time_left, job_today_wait_time_right);
                        tmp_min2 = (long)Math.min(job_total_wait_time, period_max_time);
                        period_wait_time = (long)Math.min(tmp_min1, tmp_min2);

                        emit(period_wait_time);
                        """
                    extra_mappings = {
                        "PeriodWaitTime": {
                            "type": "long",
                            "script": {
                                "language": "painless",
                                "source": source,
                            },
                        },
                    }
                    this_query_params["extra_mappings"] = extra_mappings

                # Split up queries to reduce number of buckets
                walltime_buckets = [None]
                waittime_buckets = [None]
                if args.compute_buckets:
                    days_in_query = (period_end - start).days
                    if days_in_query < 60:
                        pass
                    elif days_in_query < 120:
                        walltime_buckets = [(0, 4), (4, 9)]
                    elif days_in_query < 240:
                        walltime_buckets = [(0, 4), (4, 9)]
                        waittime_buckets = [(0, 4), (4, 9)]
                    elif days_in_query < 480:
                        walltime_buckets = [(0, 2), (2, 4), (4, 6), (6, 9)]
                        waittime_buckets = [(0, 4), (4, 9)]
                    else:
                        walltime_buckets = [(0, 2), (2, 4), (4, 6), (6, 9)]
                        waittime_buckets = [(0, 2), (2, 4), (4, 6), (6, 9)]

                i_reduced_query = 0
                n_reduced_query = len(walltime_buckets) * len(waittime_buckets)
                for walltime_bucket in walltime_buckets:
                    for waittime_bucket in waittime_buckets:
                        i_reduced_query += 1

                        reduced_query_filters = []
                        if walltime_bucket is not None:
                            reduced_query_filters.append(
                                {"terms": {
                                    "JobWallTimeBucket": list(range(*walltime_bucket)),
                                }}
                            )
                        if waittime_bucket is not None:
                            reduced_query_filters.append(
                                {"terms": {
                                    "JobWaitTimeBucket": list(range(*waittime_bucket)),
                                }}
                            )

                        this_query_params["extra_filters"] = extra_filters + reduced_query_filters
                        query = get_query(
                            index=OSPOOL_ES_INDEX,
                            period_start_ts=start_ts,
                            period_end_ts=period_end_ts,
                            compute_buckets=args.compute_buckets,
                            **this_query_params,
                            )

                        print(f"{datetime.now()} - Running {query_name} ({i_query+1} of {len(queries)}) - {i_date+1} of {len(date_ranges)} date ranges - {i_reduced_query} of {n_reduced_query} subqueries...")
                        t0 = time.time()
                        try:
                            result = client.search(index=query.pop("index"), body=query)
                        except Exception as err:
                            try:
                                print_error(err.info)
                            except Exception:
                                pass
                            raise err
                        print(f"{datetime.now()} - ...took {time.time() - t0:0.2f} seconds")

                        keylist = list(initial_key.keys()) + get_keys_from_query(query)
                        this_flat_result = flatten_aggs(
                            agg=result["aggregations"],
                            ordered_keys=keylist,
                            this_row_key=initial_key.copy(),
                        )
                        flat_results = merge_flattened_aggs(flat_results, this_flat_result)

    except KeyboardInterrupt:
        print(f"{datetime.now()} - Exiting early due to Ctrl-C...")
    else:
        incomplete = False

    print(f"{datetime.now()} - Flattening results to full JSON...")
    flatter_results = deque()
    flatter_keys = set()
    for key, result in flat_results.items():
        result_dict = {}
        for ik, k in enumerate(keylist):
            result_dict[k] = key[ik]
            flatter_keys.add(k)
        for k, v in result.items():
            result_dict[k] = v
            flatter_keys.add(k)
        flatter_results.append(result_dict.copy())

    mapped_results = map_resources_and_projects(flatter_results)

    print(f"{datetime.now()} - Writing full JSON...")
    write_json_file(list(mapped_results), Path(args.output_dir) / "ospool" / f"{period_start_str}_{period_end_str}{'.incomplete' if incomplete else ''}.json", indent=2)
    print(f"{datetime.now()} - Done!\n")


if __name__ == "__main__":
    main()
