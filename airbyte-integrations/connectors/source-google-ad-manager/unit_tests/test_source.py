from typing import Tuple
from unittest.mock import MagicMock, Mock

from googleads.errors import GoogleAdsValueError

from source_google_ad_manager.source import SourceGoogleAdManager


def patch_ad_manager_client(mocker) -> Tuple[Mock, Mock]:
    ad_manager_client = MagicMock()
    network_service = MagicMock()
    network_service.getAllNetworks.return_value = [{"networkCode": "network_1"}, {"networkCode": "network_2"}]
    ad_manager_client.GetService.return_value = network_service

    mocker.patch("source_google_ad_manager.source.create_ad_manager_client", return_value=ad_manager_client)

    return ad_manager_client, network_service


def test_check_valid_network_code(mocker):
    _, network_service = patch_ad_manager_client(mocker)

    source = SourceGoogleAdManager()
    logger_mock, config_mock = MagicMock(), MagicMock()

    config = {
        "network_code": "network_1"
    }

    config_mock.__getitem__.side_effect = lambda key: config.get(key)
    assert source.check_connection(logger_mock, config_mock) == (True, None)
    network_service.getAllNetworks.assert_called_once()


# TODO REFACTOR Those test can be grouped and parametrized
def test_wrong_network_code(mocker):
    _, network_service = patch_ad_manager_client(mocker)

    source = SourceGoogleAdManager()
    logger_mock, config_mock = MagicMock(), MagicMock()

    config = {
        "network_code": "esoteric_network_code"
    }

    config_mock.__getitem__.side_effect = lambda key: config.get(key)
    assert source.check_connection(logger_mock, config_mock) == (
        False, "Network 'esoteric_network_code' not found. Available networks: ['network_1', 'network_2']"
    )
    network_service.getAllNetworks.assert_called_once()


def test_connection_issues(mocker):
    ad_manager_client, network_service = patch_ad_manager_client(mocker)

    ad_manager_client.GetService.side_effect = GoogleAdsValueError("Any kind of client error")

    source = SourceGoogleAdManager()
    logger_mock, config_mock = MagicMock(), MagicMock()

    config = {
        "network_code": "esoteric_network_code"
    }

    config_mock.__getitem__.side_effect = lambda key: config.get(key)
    assert source.check_connection(logger_mock, config_mock) == (
        False, "An exception occurred: Any kind of client error"
    )
    network_service.getAllNetworks.assert_not_called()


def test_streams(mocker):
    source = SourceGoogleAdManager()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    # TODO: replace this with your streams number
    expected_streams_number = 2
    assert len(streams) == expected_streams_number
