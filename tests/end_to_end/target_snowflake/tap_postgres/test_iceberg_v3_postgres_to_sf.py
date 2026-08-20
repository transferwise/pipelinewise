import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from tests.end_to_end.helpers import assertions, tasks
from tests.end_to_end.target_snowflake.tap_postgres import TapPostgres


TAP_ID = "postgres_to_sf_iceberg"
TARGET_ID = "snowflake"
LARGE_VARCHAR_LENGTH = 16_777_217


def _base64url(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _key_pair_jwt(account_identifier, user, private_key):
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(public_key).digest()).decode("ascii")
    normalized_account = account_identifier.upper().replace(".", "-")
    qualified_user = f"{normalized_account}.{user.upper()}"
    header = _base64url(
        json.dumps(
            {"alg": "RS256", "typ": "JWT"},
            separators=(",", ":"),
        ).encode("utf-8")
    )
    now = datetime.now(timezone.utc)
    payload = _base64url(
        json.dumps(
            {
                "iss": f"{qualified_user}.{fingerprint}",
                "sub": qualified_user,
                "iat": int(now.timestamp()),
                "exp": int((now + timedelta(minutes=5)).timestamp()),
            },
            separators=(",", ":"),
        ).encode("utf-8")
    )
    unsigned = f"{header}.{payload}".encode("ascii")
    signature = private_key.sign(unsigned, padding.PKCS1v15(), hashes.SHA256())
    return f"{unsigned.decode('ascii')}.{_base64url(signature)}"


def _horizon_credentials(identity):
    """Build the Horizon host and access token for the active Snowflake role."""
    organization, account, database, user, role = identity
    account_identifier = f"{organization}-{account}"
    private_key = serialization.load_pem_private_key(
        Path(os.environ["TARGET_SNOWFLAKE_PRIVATE_KEY"]).read_bytes(),
        password=None,
    )
    jwt_token = _key_pair_jwt(account_identifier, user, private_key)
    host = f"{account_identifier.lower()}.snowflakecomputing.com"
    token_request = Request(
        f"https://{host}/polaris/api/catalog/v1/oauth/tokens",
        data=urlencode(
            {
                "grant_type": "client_credentials",
                "scope": f"session:role:{role}",
                "client_secret": jwt_token,
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urlopen(token_request, timeout=30) as response:  # nosec B310
        access_token = json.load(response)["access_token"]
    return host, database, access_token


class TestIcebergV3PostgresToSnowflake(TapPostgres):
    """Exercise PostgreSQL Singer, FullSync, and PartialSync into Iceberg."""

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.target_schema = (f"PPW_E2E_TAP_POSTGRES{self.e2e_env.sf_schema_postfix}").upper()
        self.initial_s3_keys = self.iceberg_fastsync_s3_keys()

    def prepare_source(self):
        """Create Iceberg-only source tables before catalog discovery."""
        super().prepare_source()
        self.addCleanup(self._drop_iceberg_source_tables)
        self.e2e_env.run_query_tap_postgres(
            "ALTER TABLE public.edgydata ADD COLUMN large_text text"
        )
        self.e2e_env.run_query_tap_postgres("CREATE EXTENSION IF NOT EXISTS hstore")
        self._drop_iceberg_source_tables()
        self.e2e_env.run_query_tap_postgres(
            "CREATE TABLE public.iceberg_hstore ("
            "id integer NOT NULL PRIMARY KEY, attributes hstore)"
        )
        self.e2e_env.run_query_tap_postgres(
            "INSERT INTO public.iceberg_hstore (id, attributes) VALUES "
            "(1, %s::hstore), (2, hstore(%s, %s))",
            ('"alpha"=>"one", "beta"=>NULL', "unicode", "初"),
        )
        self.e2e_env.run_query_tap_postgres(
            "CREATE TABLE public.iceberg_composite_key ("
            "first_key integer NOT NULL, second_key integer NOT NULL, "
            "value_text text, PRIMARY KEY (second_key, first_key))"
        )
        self.e2e_env.run_query_tap_postgres(
            "INSERT INTO public.iceberg_composite_key VALUES "
            "(1, 20, 'first'), (2, 10, 'second')"
        )

    def _drop_iceberg_source_tables(self):
        self.e2e_env.run_query_tap_postgres(
            "DROP TABLE IF EXISTS public.iceberg_hstore CASCADE; "
            "DROP TABLE IF EXISTS public.iceberg_composite_key CASCADE"
        )

    def _assert_managed_v3(self, table_name):
        format_rows = self.e2e_env.run_query_target_snowflake(
            "SELECT IS_ICEBERG FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{self.target_schema}' "
            f"AND TABLE_NAME = '{table_name.upper()}'"
        )
        self.assertEqual(format_rows, [("YES",)])
        version_rows = self.e2e_env.run_query_target_snowflake(
            f'SHOW PARAMETERS LIKE \'ICEBERG_VERSION\' IN TABLE "{self.target_schema}"."{table_name.upper()}"'
        )
        self.assertEqual(len(version_rows), 1)
        self.assertEqual(str(version_rows[0][1]), "3")
        merge_on_read_rows = self.e2e_env.run_query_target_snowflake(
            "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' IN TABLE "
            f'"{self.target_schema}"."{table_name.upper()}"'
        )
        self.assertEqual(len(merge_on_read_rows), 1)
        self.assertEqual(str(merge_on_read_rows[0][1]).upper(), "DISABLED")
        self.assertEqual(str(merge_on_read_rows[0][3]).upper(), "TABLE")

    def _edgy_row(self, row_id):
        rows = self.e2e_env.run_query_target_snowflake(
            'SELECT "CID", TO_JSON("CJSON"), TO_JSON("CJSONB"), "CVARCHAR" '
            f'FROM "{self.target_schema}"."EDGYDATA" WHERE "CID" = {row_id}'
        )
        self.assertEqual(len(rows), 1)
        return rows[0]

    def _assert_large_text(self, row_id):
        rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT LENGTH("LARGE_TEXT") '
            f'FROM "{self.target_schema}".'
            f'"EDGYDATA" WHERE "CID" = {row_id}'
        )
        self.assertEqual(rows, [(LARGE_VARCHAR_LENGTH,)])

    def _assert_large_text_column_width(self):
        rows = self.e2e_env.run_query_target_snowflake(
            "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{self.target_schema}' "
            "AND TABLE_NAME = 'EDGYDATA' "
            "AND COLUMN_NAME = 'LARGE_TEXT'"
        )
        self.assertEqual(rows, [(134217728,)])

    def _horizon_table_metadata(self, table_name):
        """Load raw Iceberg metadata through Snowflake Horizon Catalog."""
        identity = self.e2e_env.run_query_target_snowflake(
            "SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME(), "
            "CURRENT_DATABASE(), CURRENT_USER(), CURRENT_ROLE()"
        )[0]
        host, database, access_token = _horizon_credentials(identity)

        database_path = quote(database, safe="")
        schema_path = quote(self.target_schema, safe="")
        table_path = quote(table_name.upper(), safe="")
        metadata_request = Request(
            f"https://{host}/polaris/api/catalog/v1/{database_path}/namespaces/{schema_path}/tables/{table_path}",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            method="GET",
        )
        with urlopen(metadata_request, timeout=30) as response:  # nosec B310
            return json.load(response)["metadata"]

    def _assert_country_approximate_numbers_are_double(self):
        """PostgreSQL approximate-number mappings retain 64-bit precision."""
        metadata = self._horizon_table_metadata("COUNTRY")
        current_schema = next(
            schema
            for schema in metadata["schemas"]
            if schema["schema-id"] == metadata["current-schema-id"]
        )
        field_types = {
            field["name"]: field["type"]
            for field in current_schema["fields"]
        }

        self.assertEqual(
            {
                field_types["SURFACEAREA"],
                field_types["LIFEEXPECTANCY"],
                field_types["GNP"],
                field_types["GNPOLD"],
            },
            {"double"},
        )

    def _assert_fastsync_content(self, fastsync_id, large_json):
        """Validate FastSync values and Iceberg primary-key metadata."""
        row = self._edgy_row(fastsync_id)
        self.assertEqual(json.loads(row[1]), json.loads(large_json))
        self.assertEqual(json.loads(row[2]), json.loads(large_json))
        self.assertEqual(row[3], "fastsync-json")
        hstore_rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "ID", TO_JSON("ATTRIBUTES") FROM "{self.target_schema}"."ICEBERG_HSTORE" ORDER BY "ID"'
        )
        self.assertEqual([item[0] for item in hstore_rows], [1, 2])
        self.assertEqual(
            json.loads(hstore_rows[0][1]),
            {"alpha": "one", "beta": None},
        )
        self.assertEqual(json.loads(hstore_rows[1][1]), {"unicode": "初"})
        primary_key_rows = self.e2e_env.run_query_target_snowflake(
            f'SHOW PRIMARY KEYS IN TABLE "{self.target_schema}"."ICEBERG_COMPOSITE_KEY"'
        )
        self.assertEqual(
            sorted(
                ((item[4], item[5]) for item in primary_key_rows),
                key=lambda item: item[1],
            ),
            [("SECOND_KEY", 1), ("FIRST_KEY", 2)],
        )
        metadata_rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(\'"{self.target_schema}"."ICEBERG_COMPOSITE_KEY"\')'
        )
        metadata = json.loads(metadata_rows[0][0])
        self.assertEqual(metadata["status"].lower(), "success")
        self.assertTrue(metadata["metadataLocation"].endswith(".metadata.json"))
        raw_metadata = self._horizon_table_metadata("ICEBERG_COMPOSITE_KEY")
        current_schema = next(
            schema for schema in raw_metadata["schemas"] if schema["schema-id"] == raw_metadata["current-schema-id"]
        )
        field_ids = {field["name"]: field["id"] for field in current_schema["fields"]}
        self.assertEqual(
            set(current_schema["identifier-field-ids"]),
            {field_ids["SECOND_KEY"], field_ids["FIRST_KEY"]},
        )

    def _exercise_singer_handoff(self, fastsync_id):
        """Add a source column, rediscover it, and hand the table to Singer."""
        singer_id = fastsync_id + 1
        singer_json = json.dumps({"singer": [None, {}]})
        self.e2e_env.run_query_tap_postgres(
            "ALTER TABLE public.edgydata ADD COLUMN singer_added_text text"
        )
        discovery_result = tasks.run_command(
            f"pipelinewise discover_tap --tap {self.tap_id} --target {self.target_id}"
        )
        assertions.assert_command_success(*discovery_result)
        self.e2e_env.run_query_tap_postgres(
            "INSERT INTO public.edgydata "
            "(cjson, cjsonb, cvarchar, singer_added_text) "
            "VALUES (%s::json, %s::jsonb, %s, %s)",
            (singer_json, singer_json, "singer-json", "added-by-singer"),
        )
        text_id = self.e2e_env.run_query_tap_postgres(
            'INSERT INTO public."table_with_space and UPPERCase" '
            "(cvarchar, updated_at, json_metadata) VALUES (%s, NOW(), %s) "
            "RETURNING id",
            ("singer-text", json.dumps({"large": "t" * 70000})),
        )[0][0]
        self.e2e_env.run_query_tap_postgres("DELETE FROM public.country WHERE code = 'UMI'")
        self.e2e_env.run_query_tap_postgres(
            "INSERT INTO public.iceberg_hstore (id, attributes) VALUES (3, %s::hstore)",
            ('"singer"=>"value", "nullable"=>NULL',),
        )
        self.e2e_env.run_query_tap_postgres("DELETE FROM public.no_pk_table WHERE id > 10")

        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ["fastsync", "singer"],
            expected_state_streams={
                "fastsync": {
                    "public-country": False,
                    "public-no_pk_table": False,
                    "public-iceberg_composite_key": False,
                }
            },
        )

        row = self._edgy_row(singer_id)
        self.assertEqual(json.loads(row[1]), {"singer": [None, {}]})
        self.assertEqual(json.loads(row[2]), {"singer": [None, {}]})
        self.assertEqual(row[3], "singer-json")
        added_column_rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "SINGER_ADDED_TEXT" FROM "{self.target_schema}".'
            f'"EDGYDATA" WHERE "CID" = {singer_id}'
        )
        self.assertEqual(added_column_rows, [("added-by-singer",)])
        text_rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "JSON_METADATA" FROM "{self.target_schema}".'
            f'"TABLE_WITH_SPACE AND UPPERCASE" WHERE "ID" = {text_id}'
        )
        self.assertGreater(len(text_rows[0][0]), 70000)
        full_rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "ID" FROM "{self.target_schema}"."NO_PK_TABLE" ORDER BY "ID"'
        )
        self.assertEqual([item[0] for item in full_rows], list(range(1, 11)))
        hstore_rows = self.e2e_env.run_query_target_snowflake(
            f'SELECT "ID", TO_JSON("ATTRIBUTES") FROM "{self.target_schema}"."ICEBERG_HSTORE" ORDER BY "ID"'
        )
        self.assertEqual([item[0] for item in hstore_rows], [1, 2, 3])
        self.assertEqual(
            json.loads(hstore_rows[-1][1]),
            {"nullable": None, "singer": "value"},
        )
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )

    def test_fullsync_hands_over_to_singer_on_managed_iceberg_v3(self):
        """Initial FastSync and recurring engines preserve PostgreSQL values."""
        fastsync_id = self.e2e_env.run_query_tap_postgres("SELECT MAX(cid) + 1 FROM public.edgydata")[0][0]
        large_json = json.dumps({"large": "x" * 70000, "nested": [None, {}]})
        self.e2e_env.run_query_tap_postgres(
            "INSERT INTO public.edgydata (cjson, cjsonb, cvarchar) VALUES (%s::json, %s::jsonb, %s)",
            (large_json, large_json, "fastsync-json"),
        )
        self.e2e_env.run_query_tap_postgres(
            "UPDATE public.edgydata SET large_text = repeat(%s, %s) "
            "WHERE cid = %s",
            (
                "p",
                LARGE_VARCHAR_LENGTH,
                fastsync_id,
            ),
        )

        assertions.assert_run_tap_success(
            self.tap_id,
            self.target_id,
            ["fastsync", "singer"],
            expected_state_streams={
                "fastsync": {
                    "public-edgydata": True,
                    "public-table_with_space and UPPERCase": True,
                    "public-iceberg_hstore": True,
                    "public-country": False,
                    "public-no_pk_table": False,
                    "public-iceberg_composite_key": False,
                }
            },
        )

        for table_name in (
            "edgydata",
            "table_with_space and UPPERCase",
            "country",
            "no_pk_table",
            "iceberg_hstore",
            "iceberg_composite_key",
        ):
            self._assert_managed_v3(table_name)
        self._assert_country_approximate_numbers_are_double()
        self._assert_fastsync_content(fastsync_id, large_json)
        self._assert_large_text(fastsync_id)
        self._assert_large_text_column_width()
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )
        self._exercise_singer_handoff(fastsync_id)

    def test_partial_sync_merges_a_bounded_range_into_managed_iceberg_v3(self):
        """PartialSync preserves VARIANT values in a PostgreSQL key range."""
        assertions.assert_resync_tables_success(
            self.tap_id,
            self.target_id,
            tables="public.edgydata",
            expected_state_streams={"fastsync": {"public-edgydata": True}},
        )
        self._assert_managed_v3("edgydata")
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )
        sentinel_before = self._edgy_row(1)

        self.e2e_env.run_query_tap_postgres(
            "UPDATE public.edgydata SET cjson = %s::json, cjsonb = %s::jsonb, cvarchar = %s WHERE cid = 1",
            (
                '{"outside": "source-only"}',
                '{"outside": "source-only"}',
                "source-outside-range",
            ),
        )
        self.e2e_env.run_query_tap_postgres(
            "UPDATE public.edgydata SET cjson = %s::json, cjsonb = %s::jsonb, "
            "cvarchar = %s, large_text = repeat(%s, %s) "
            "WHERE cid = 2",
            (
                '{"partial": true}',
                '{"partial": true}',
                "partial-updated",
                "u",
                LARGE_VARCHAR_LENGTH,
            ),
        )
        inserted_id = self.e2e_env.run_query_tap_postgres(
            "INSERT INTO public.edgydata (cjson, cjsonb, cvarchar) VALUES (%s::json, %s::jsonb, %s) RETURNING cid",
            (
                '{"partial": "insert"}',
                '{"partial": "insert"}',
                "partial-insert",
            ),
        )[0][0]
        self.e2e_env.run_query_tap_postgres(
            "UPDATE public.edgydata SET large_text = repeat(%s, %s) "
            "WHERE cid = %s",
            (
                "i",
                LARGE_VARCHAR_LENGTH,
                inserted_id,
            ),
        )
        assertions.assert_partial_sync_table_success(
            {
                "env": self.e2e_env,
                "tap": self.tap_id,
                "tap_type": "postgres",
                "target": self.target_id,
                "source_db": "public",
                "table": "edgydata",
                "column": "cid",
            },
            start_value=2,
            end_value=inserted_id,
        )

        updated = self._edgy_row(2)
        inserted = self._edgy_row(inserted_id)
        self.assertEqual(json.loads(updated[1]), {"partial": True})
        self.assertEqual(json.loads(updated[2]), {"partial": True})
        self.assertEqual(updated[3], "partial-updated")
        self.assertEqual(json.loads(inserted[1]), {"partial": "insert"})
        self.assertEqual(json.loads(inserted[2]), {"partial": "insert"})
        self.assertEqual(inserted[3], "partial-insert")
        self._assert_large_text(2)
        self._assert_large_text(inserted_id)
        self._assert_large_text_column_width()
        self.assertEqual(self._edgy_row(1), sentinel_before)
        self.assert_iceberg_fastsync_cleanup(
            self.target_schema,
            self.initial_s3_keys,
        )
