"""Azure Data Lake Storage Gen2 (ADLS / Blob Storage) Dagster Resource."""



import logging
from pathlib import Path
from typing import Dict

from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class AzureDataLakeResource(ConfigurableResource):
    """Uploads partitioned WWI Parquet files to Azure Blob Storage / ADLS Gen2."""

    storage_account_name: str = "stwwidatalakedev01"
    storage_account_key: str = ""
    connection_string: str = ""
    container_raw: str = "wwi-raw"
    raw_prefix: str = "wwi/v1"
    dry_run_local: bool = True

    def _get_blob_service_client(self):
        from azure.storage.blob import BlobServiceClient

        if self.connection_string:
            return BlobServiceClient.from_connection_string(self.connection_string)
        account_url = f"https://{self.storage_account_name}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=self.storage_account_key)

    def upload_parquet_files(self, table_files: Dict[str, Path]) -> Dict[str, str]:
        """Upload each WWI Parquet file to ADLS Gen2 and return its Azure URI."""
        uploaded_uris: Dict[str, str] = {}

        if self.dry_run_local:
            for table_name, file_path in table_files.items():
                blob_path = f"{self.raw_prefix}/{table_name}/{file_path.name}"
                azure_uri = (
                    f"azure://{self.storage_account_name}.blob.core.windows.net/"
                    f"{self.container_raw}/{blob_path}"
                )
                logger.info("[DRY_RUN_LOCAL] Simulated ADLS upload: %s -> %s", file_path, azure_uri)
                uploaded_uris[table_name] = azure_uri
            return uploaded_uris

        blob_service_client = self._get_blob_service_client()
        container_client = blob_service_client.get_container_client(self.container_raw)
        if not container_client.exists():
            container_client.create_container()

        for table_name, file_path in table_files.items():
            blob_path = f"{self.raw_prefix}/{table_name}/{file_path.name}"
            blob_client = container_client.get_blob_client(blob_path)
            with open(file_path, "rb") as data_stream:
                blob_client.upload_blob(data_stream, overwrite=True)

            azure_uri = (
                f"azure://{self.storage_account_name}.blob.core.windows.net/"
                f"{self.container_raw}/{blob_path}"
            )
            logger.info("Uploaded %s to Azure Data Lake: %s", file_path.name, azure_uri)
            uploaded_uris[table_name] = azure_uri

        return uploaded_uris
