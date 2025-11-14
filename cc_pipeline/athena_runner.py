import time
import logging
from typing import Dict, Optional
from botocore.exceptions import ClientError
import boto3
import pandas as pd

logger = logging.getLogger(__name__)


class AthenaRunner:
    def __init__(self, cfg):
        self.cfg = cfg
        self.client = boto3.client("athena", region_name=cfg.aws_region)

    def ensure_custom_workgroup(self) -> str:
        if not self.cfg.athena_output_s3:
            return self.cfg.athena_workgroup       
        wg = self.cfg.athena_custom_workgroup_name

           # Check if the workgroup already exists
        try:
            logger.info("Checking if workgroup %s exists", wg)
            existing_workgroups = self.client.list_work_groups()
            workgroup_names = [wg["Name"] for wg in existing_workgroups["WorkGroups"]]
            
            if wg in workgroup_names:
                logger.debug("Workgroup %s already exists, no need to create it", wg)
                return wg
          
        except ClientError as e:
            logger.exception("Failed to list workgroups: %s", e)
            raise


        try:
            logger.info("Creating or ensuring workgroup %s", wg)
            self.client.create_work_group(
                Name=wg,
                Configuration={
                    "EnforceWorkGroupConfiguration": False,
                    "PublishCloudWatchMetricsEnabled": False,
                    "ResultConfiguration": {
                        "OutputLocation": self.cfg.athena_output_s3
                    }
                },
                Description="Auto-created workgroup for explicit Athena output"
            )
        except ClientError as e:
            if "already exists" in str(e):
                logger.debug("Workgroup exists: %s", wg)
            else:
                logger.exception("Failed to create workgroup: %s", e)
                raise
            # logger.exception("Failed to create workgroup: %s", e)
            # raise  # Re-raise the exception to handle it outside
        return wg

    def run_query(self, query: str, use_custom_output: bool = False) -> Dict:
    
        workgroup = self.cfg.athena_workgroup
        params = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": self.cfg.athena_database},
            "WorkGroup": workgroup,
        }   
      
        if use_custom_output and self.cfg.athena_output_s3:
            wg = self.ensure_custom_workgroup()
            params["WorkGroup"] = wg
            params["ResultConfiguration"] = {"OutputLocation": self.cfg.athena_output_s3}

        resp = self.client.start_query_execution(**params)
        qid = resp["QueryExecutionId"]
        logger.info("Started query %s", qid)
        while True: 
            
            info = self.client.get_query_execution(QueryExecutionId=qid)
           
            state = info["QueryExecution"]["Status"]["State"]
            if state == "FAILED":
                reason = info["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                logger.error("Query %s failed: %s", qid, reason)
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                logger.info("Query %s finished with state %s", qid, state)
                break
            time.sleep(self.cfg.athena_poll_interval)
        return info

    def fetch_query_results(self, query_execution_id: str) -> 'pd.DataFrame':
        paginator = self.client.get_paginator("get_query_results")
        rows = []
        columns = None
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            result_set = page["ResultSet"]
            if not columns:
                columns = [c["Label"] for c in result_set["ResultSetMetadata"]["ColumnInfo"]]
            for r in result_set.get("Rows", []):
                values = [c.get("VarCharValue", None) for c in r.get("Data", [])]
                rows.append(values)
        if columns and rows and rows[0] == columns:
            rows = rows[1:]
        df = pd.DataFrame(rows, columns=columns or [])
        logger.info("Fetched %d rows", len(df))
        return df