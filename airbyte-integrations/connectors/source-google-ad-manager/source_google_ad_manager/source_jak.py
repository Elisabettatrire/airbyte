from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from source_google_ad_manager.utils import create_ad_manager_client



"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""

class Customers(GoogleAdManagerStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "customer_id"

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "customers"


# Basic incremental stream
class IncrementalGoogleAdManagerStream(GoogleAdManagerStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> \
            Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Employees(IncrementalGoogleAdManagerStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the cursor_field. Required.
    cursor_field = "start_date"

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
        return "single". Required.
        """
        return "employees"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


# Source
class SourceGoogleAdManager(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Test connection to google ad manager
        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        try:
            ad_manager_client = create_ad_manager_client(config, scopes=scopes, application_name=application_name)
            #TODO check if there is an alternative to getAllNetworks() checking only one network
            networks = ad_manager_client.GetService('NetworkService').getAllNetworks()
            available_networks_code = [network['networkCode'] for network in networks]
            current_network_code = config["network_code"]

            # In my opinion checking only the connection to AD Manager doesn't make much sense because
            # We might not have access to the network we need.

            if current_network_code in available_networks_code:
                return True, None
            else:
                return False, f"Network '{current_network_code}' not found. Available networks: {available_networks_code}"
        except Exception as e:
            msg = f"An exception occurred: {str(e)}"
            logger.exception(msg)
            # Should we return the exception as message? Shouldn't we say... see the logs?
            return False, msg

    def discover(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteCatalog:

        return [AirbyteStream(name=stream_name, json_schema=json_schema, supported_sync_modes=supported_sync_modes,
                                     source_defined_cursor=source_defined_cursor,
                              default_cursor_field=default_cursor_field)]


    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        streams = []

        streams.append(AirbyteStream(name=stream_name, json_schema=json_schema, supported_sync_modes=supported_sync_modes,
                                     source_defined_cursor=source_defined_cursor, default_cursor_field=default_cursor_field))
        return AirbyteCatalog(streams=streams)


    def read(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = TokenAuthenticator(token="api_key")  # Oauth2Authenticator is also available if you need oauth support
        return [Customers(authenticator=auth), Employees(authenticator=auth)]

        ad_manager_client = create_ad_manager_client(config, scopes=scopes, application_name=application_name)
        report_downloader = ad_manager_client.GetDataDownloader(version='v202405')

        update_report_job_config(report_job, config, report_config_keys)

        selected_stream = next((cs for cs in catalog.streams if cs.stream.name == stream_name), None)
        state_date, start_chunk_index = get_state(state)
        today = datetime.now()
        start_date = get_start_date(state_date, config, today, selected_stream.sync_mode, date_format=date_format)
        end_date = get_end_date(today, config, selected_stream.sync_mode, date_format=date_format)

        for date in pd.date_range(start_date, end_date, freq='d'):
            report_job['reportQuery']['startDate'] = parse_date_to_dict(date)
            report_job['reportQuery']['endDate'] = parse_date_to_dict(date)
            logger.info(f"Report_job is: {json.dumps(report_job, indent=4)}")

            report_job_id = report_downloader.WaitForReport(report_job=report_job)
            logger.info(f"Report job created with ID: {report_job_id}")

            with tempfile.NamedTemporaryFile(mode=temp_file_mode, delete=temp_file_delete) as temp_file:
                logger.info(f"Downloading report to temporary file: {temp_file.name}")
                report_downloader.DownloadReportToFile(report_job_id, report_download_format, temp_file,
                                                       use_gzip_compression=use_gzip_compression)
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
                                    stream_state={"state_date": date.strftime(date_format),
                                                  "start_chunk_index": chunk_index}
                                )
                            )
                            yield AirbyteMessage(type=Type.STATE, state=stream_state)
                            logger.info(f"State updated to: {stream_state}")
                    start_chunk_index = 0
                else:
                    logger.error("Temporary report file is empty.")
                os.remove(temp_file.name)
            logger.info(f"Fetched data up to: {date.strftime(date_format)}")

