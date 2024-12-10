configuration = {
    "stream": {
        "stream_name": "gam",
        "scopes": [
            "https://www.googleapis.com/auth/dfp"
        ],
        "application_name": "ad_manager",
        "supported_sync_modes": ["full_refresh", "incremental"],
        "default_cursor_field": ["DATE"],
        "source_defined_cursor": True
    },
    "report": {
        "temp_file_mode": "wb",
        "temp_file_delete": False,
        "date_format": "%Y-%m-%d",
        "report_download_format": "CSV",
        "use_gzip_compression": False,
        "config_keys": [
            "dimensions",
            "columns",
            "dimensionAttributes"
        ],
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "DATE": {"type": "string", "format": "date"},
                "record": {"type": "JSON"}
            }
        }
    }
}
