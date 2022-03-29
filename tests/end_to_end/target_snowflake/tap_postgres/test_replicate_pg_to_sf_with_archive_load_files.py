import gzip
import tempfile

from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres

TAP_ID = "postgres_to_sf_archive_load_files"
TARGET_ID = "snowflake"
ARCHIVE_S3_PREFIX = "archive_folder"


class TestReplicatePGToSFWithArchiveLoadFiles(TapPostgres):
    """
    Fastsync tables from Postgres to Snowflake with archive load files enabled
    """

    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.s3_bucket = self.e2e_env._get_conn_env_var("TARGET_SNOWFLAKE", "S3_BUCKET")
        self.s3_client = self.e2e_env.get_aws_session().client("s3")

    def tearDown(self):
        super().tearDown()

    def delete_dangling_files_from_archive(self):
        files_in_s3_archive = self.s3_client.list_objects(
            Bucket=self.s3_bucket,
            Prefix=f"{ARCHIVE_S3_PREFIX}/postgres_to_sf_archive_load_files/",
        ).get("Contents", [])
        for file_in_archive in files_in_s3_archive:
            self.s3_client.delete_object(
                Bucket=self.s3_bucket, Key=(file_in_archive["Key"])
            )

    def get_files_from_s3_for_table(self, table: str):
        return self.s3_client.list_objects(
            Bucket=self.s3_bucket,
            Prefix=(f"{ARCHIVE_S3_PREFIX}/postgres_to_sf_archive_load_files/{table}"),
        ).get("Contents")

    def test_replicate_pg_to_sf_with_archive_load_files(self):

        self.delete_dangling_files_from_archive()

        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ["fastsync", "singer"]
        )

        expected_archive_files_count = {
            "public.city": 2,  # INCREMENTAL: fastsync and singer
            "public.country": 1,  # FULL_TABLE : fastsync only
            "public2.wearehere": 1,  # FULL_TABLE : fastsync only
        }

        # Assert expected files in archive folder
        for (
            schema_table,
            expected_archive_files,
        ) in expected_archive_files_count.items():

            schema, table = schema_table.split(".")
            files_in_s3_archive = self.get_files_from_s3_for_table(table)

            if (
                files_in_s3_archive is None
                or len(files_in_s3_archive) != expected_archive_files
            ):
                raise Exception(
                    f"files_in_archive for {table} is {files_in_s3_archive}."
                    f"Expected archive files count: {expected_archive_files}"
                )

            # Assert expected metadata
            archive_metadata = self.s3_client.head_object(
                Bucket=self.s3_bucket, Key=(files_in_s3_archive[0]["Key"])
            )["Metadata"]

            expected_metadata = {
                "tap": "postgres_to_sf_archive_load_files",
                "schema": schema,
                "table": table,
                "archived-by": "pipelinewise_fastsync_postgres_to_snowflake",
            }

            if archive_metadata != expected_metadata:
                raise Exception(f"archive_metadata for {table} is {archive_metadata}")

            # Assert expected file contents
            with tempfile.NamedTemporaryFile() as tmpfile:
                with open(tmpfile.name, "wb") as tmpf:
                    self.s3_client.download_fileobj(
                        self.s3_bucket, files_in_s3_archive[0]["Key"], tmpf
                    )
                with gzip.open(tmpfile, "rt") as gzipfile:
                    rows_in_csv = len(gzipfile.readlines())

            rows_in_table = self.e2e_env.run_query_tap_postgres(
                f"SELECT COUNT(1) FROM {schema_table}"
            )[0][0]

            if rows_in_csv != rows_in_table:
                raise Exception(
                    f"Rows in csv and db differ: {rows_in_csv} vs {rows_in_table}"
                )
