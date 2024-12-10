# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import json
import os
import tempfile
import pandas as pd
from datetime import datetime, timedelta
from typing import Any, Dict, Generator
import logging

#This is deprecated for python > 3.10. see https://app.slack.com/client/T01AB4DDR2N/search
#from airbyte_cdk.logger import AirbyteLogger

from airbyte_cdk.models import (
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
    AirbyteStateBlob,
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
    SyncMode,
    AirbyteStateMessage
)
from airbyte_cdk.sources import Source
from .config import configuration
from .report_query import report_job
from .utils import (
    parse_date_to_dict,
    get_start_date,
    get_end_date,
    create_ad_manager_client,
    get_state,
    update_report_job_config
)

import pytz
from io import StringIO

# Constants

scopes = configuration["stream"]["scopes"]
application_name = configuration["stream"]["application_name"]
stream_name = configuration["stream"]["stream_name"]
temp_file_mode = configuration["report"]["temp_file_mode"]
temp_file_delete = configuration["report"]["temp_file_delete"]
date_format = configuration["report"]["date_format"]
report_download_format = configuration["report"]["report_download_format"]
use_gzip_compression = configuration["report"]["use_gzip_compression"]
report_config_keys = configuration["report"]["config_keys"]
json_schema = configuration["report"]["json_schema"]
supported_sync_modes = configuration["stream"]["supported_sync_modes"] 
source_defined_cursor = configuration["stream"]["source_defined_cursor"]
default_cursor_field = configuration["stream"]["default_cursor_field"] 

#TODO: test start_date < end_date! 

class SourceGoogleAdManager(Source, CheckpointMixin):

    cursor_field = "published_date" #TODO: mettere cursor giusto
    
    @property
    def state(self) -> Mapping[str, Any]:
        return {"published_date": str(self._cursor_value)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value.get("published_date", self._cursor_value)

    def check(self, logger: logging.Logger, config: json) -> AirbyteConnectionStatus:
        try:
            ad_manager_client = create_ad_manager_client(config, scopes=scopes, application_name=application_name)
            networks = ad_manager_client.GetService('NetworkService').getAllNetworks()
            available_networks_code = [network['networkCode'] for network in networks]
            current_network_code = config["network_code"]
            if current_network_code in available_networks_code:
                return True, None
            else:
                return False, f"Network '{current_network_code}' not found. Available networks: {available_networks_code}"
        except Exception as e:
            msg = f"An exception occurred: {str(e)}"
            logger.exception(msg)
            return False, msg

    def discover(self, logger: logging.Logger, config: json) -> AirbyteCatalog:
        streams = []

        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema, supported_sync_modes=supported_sync_modes,
                                     source_defined_cursor=source_defined_cursor, default_cursor_field=default_cursor_field))
        return AirbyteCatalog(streams=streams)

    def read(self, logger: logging.Logger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, Any]) -> Generator[AirbyteMessage, None, None]:
        ad_manager_client = create_ad_manager_client(config, scopes=scopes, application_name=application_name)
        report_downloader = ad_manager_client.GetDataDownloader(version='v202405')

        update_report_job_config(report_job, config, report_config_keys)

        selected_stream = next((cs for cs in catalog.streams if cs.stream.name == stream_name), None)
        state_date, start_chunk_index = get_state(state)
        today = datetime.now()
        start_date = get_start_date(state_date, config, today, selected_stream.sync_mode,date_format=date_format)
        end_date = get_end_date(today, config, selected_stream.sync_mode, date_format=date_format)

        for date in pd.date_range(start_date, end_date, freq='d'):
            report_job['reportQuery']['startDate'] = parse_date_to_dict(date)
            report_job['reportQuery']['endDate'] = parse_date_to_dict(date)
            logger.info(f"Report_job is: {json.dumps(report_job, indent=4)}")

            report_job_id = report_downloader.WaitForReport(report_job=report_job)
            logger.info(f"Report job created with ID: {report_job_id}")

            with tempfile.NamedTemporaryFile(mode=temp_file_mode, delete=temp_file_delete) as temp_file:
                logger.info(f"Downloading report to temporary file: {temp_file.name}")
                report_downloader.DownloadReportToFile(report_job_id, report_download_format, temp_file, use_gzip_compression=use_gzip_compression)
                temp_file.close()

                temp_file_size = os.path.getsize(temp_file.name)
                logger.info(f"Downloaded report size: {temp_file_size} bytes")
                if temp_file_size > 0:
                    logger.info("Report downloaded to temporary file successfully.")
                    chunks = pd.read_csv(temp_file.name, chunksize=int(config.get('chunk_size')))
                    for chunk_index, df in enumerate(chunks):
                        if chunk_index < start_chunk_index:
                            continue
                        pd.set_option('display.max_columns', None)
                        logger.info(f"Preview of chunk: {df.head()}")
                        for _, row in df.iterrows():
                            record = row.to_dict()
                            yield AirbyteMessage(
                                type=Type.RECORD,
                                record=AirbyteRecordMessage(
                                    stream=stream_name,
                                    data={'DATE': date.strftime(date_format), 'record': record},
                                    emitted_at=int(datetime.now().timestamp()) * 1000
                                )
                            )
                        if selected_stream.sync_mode == SyncMode.incremental:
                            stream_state = AirbyteStateMessage(
                                type=AirbyteStateType.STREAM,
                                stream=AirbyteStreamState(
                                    stream_descriptor=StreamDescriptor(name=stream_name, namespace="public"),
                                    stream_state={"state_date": date.strftime(date_format), "start_chunk_index": chunk_index}
                                )
                            )
                            yield AirbyteMessage(type=Type.STATE, state=stream_state)
                            logger.info(f"State updated to: {stream_state}")
                    start_chunk_index = 0
                else:
                    logger.error("Temporary report file is empty.")
                os.remove(temp_file.name)
            logger.info(f"Fetched data up to: {date.strftime(date_format)}")
