import logging
from datetime import datetime

import psycopg
from geopandas import GeoDataFrame
from pandas import DataFrame
from psycopg import sql
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def write_postgis(
    table: str,
    schema: str,
    temporary_schema: str,
    df: GeoDataFrame | DataFrame,
    connection: psycopg.Connection,
    subdivision_value: str | None = None
) -> None:
    if df.index is None or df.index.name is None:
        raise ValueError("DataFrame must have an index")
    logger.info("Dumping to temporary table")
    # TODO: create a temporary unlogged table instead
    if isinstance(df, GeoDataFrame):
        df.to_postgis(
            table, create_engine("postgresql+psycopg://", creator=lambda: connection), schema=temporary_schema, if_exists="replace", index=True
        )
    else:
        df.to_sql(
            table, create_engine("postgresql+psycopg://", creator=lambda: connection), schema=temporary_schema, if_exists="replace", index=True
        )

    id_column = df.index.name
    columns = df.reset_index().columns
    logger.debug("Data has columns: %s", columns)
    logger.debug("Data has index: %s", id_column)

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
            (schema, table),
        )
        if cursor.fetchone()[0]:
            logger.info("Upserting to existing table")
            if subdivision_value is None:
                query = sql.SQL("""
    MERGE INTO {}.{} AS target
    USING {}.{} AS source
    ON target.{} = source.{}
    WHEN MATCHED THEN UPDATE SET
        {}
    WHEN NOT MATCHED BY TARGET THEN INSERT ({}, _subdivision)
        VALUES ({}, {})
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
    """).format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(temporary_schema),
                        sql.Identifier(table),
                        sql.Identifier(id_column),
                        sql.Identifier(id_column),
                        sql.Composed(
                            [
                                sql.SQL("{}=source.{}").format(
                                    sql.Identifier(column), sql.Identifier(column)
                                )
                                for column in df.columns
                            ]
                        ).join(", "),
                        sql.Composed([sql.Identifier(i) for i in columns]).join(", "),
                        sql.Composed(
                            [
                                sql.SQL("source.{}").format(sql.Identifier(column))
                                for column in columns
                            ]
                        ).join(", "),
                        sql.Placeholder(),
                    )
                logger.debug("Executing query: %s", query.as_string(cursor))
                cursor.execute(
                    query,
                    (subdivision_value,),
                )
            else:
                query = sql.SQL("""
    MERGE INTO {}.{} AS target
    USING {}.{} AS source
    ON target.{} = source.{}
    WHEN MATCHED THEN UPDATE SET
        {}
    WHEN NOT MATCHED BY TARGET THEN INSERT ({}, _subdivision)
        VALUES ({}, {})
    WHEN NOT MATCHED BY SOURCE AND target._subdivision = {} THEN
        DELETE;
    """).format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(temporary_schema),
                        sql.Identifier(table),
                        sql.Identifier(id_column),
                        sql.Identifier(id_column),
                        sql.Composed(
                            [
                                sql.SQL("{}=source.{}").format(
                                    sql.Identifier(column), sql.Identifier(column)
                                )
                                for column in df.columns
                            ]
                        ).join(", "),
                        sql.Composed([sql.Identifier(i) for i in columns]).join(", "),
                        sql.Composed(
                            [
                                sql.SQL("source.{}").format(sql.Identifier(column))
                                for column in columns
                            ]
                        ).join(", "),
                        sql.Placeholder(),
                        sql.Placeholder(),
                    )
                logger.debug("Executing query: %s", query.as_string(cursor))
                cursor.execute(
                    query,
                    (subdivision_value, subdivision_value),
                )
        else:
            logger.info("Creating new table")
            if subdivision_value is None:
                cursor.execute(
                    sql.SQL("""CREATE TABLE {}.{} AS SELECT * FROM {}.{}""").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(temporary_schema),
                        sql.Identifier(table),
                    ),
                )
            else:
                cursor.execute(
                    sql.SQL("""CREATE TABLE {}.{} AS SELECT *, {} AS _subdivision FROM {}.{}""").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Placeholder(),
                        sql.Identifier(temporary_schema),
                        sql.Identifier(table),
                    ),
                    (subdivision_value,),
                )
            cursor.execute(sql.SQL("""ALTER TABLE {}.{} ADD PRIMARY KEY ({})""").format(sql.Identifier(schema), sql.Identifier(table), sql.Identifier(id_column)))
            if subdivision_value:
                cursor.execute(sql.SQL("""CREATE INDEX ON {}.{} (_subdivision)""").format(sql.Identifier(schema), sql.Identifier(table)))

        cursor.execute(sql.SQL("DROP TABLE {}.{}").format(sql.Identifier(temporary_schema), sql.Identifier(table)))

def create_metadata_table(schema: str, connection: psycopg.Connection) -> None:
    logger.info("Creating metadata table")
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL(
                """
            CREATE TABLE IF NOT EXISTS {}.metadata (
                "table" TEXT NOT NULL PRIMARY KEY,
                collection TEXT NOT NULL,
                "name" TEXT NOT NULL,
                "provider" TEXT NOT NULL,
                last_updated TIMESTAMP WITH TIME ZONE
            )"""
            ).format(sql.Identifier(schema))
        )
        cursor.execute(
            sql.SQL(
                """
            CREATE TABLE IF NOT EXISTS {}.metadata_assets (
                "table" TEXT NOT NULL REFERENCES {}.metadata ("table"),
                item TEXT NOT NULL,
                remote_updated TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY ("table", item)
            )"""
            ).format(sql.Identifier(schema), sql.Identifier(schema))
        )
    connection.commit()


def upsert_metadata(
    table: str,
    collection: str,
    name: str,
    provider: str,
    last_updated: datetime,
    schema: str,
    item: str | None,
    connection: psycopg.Connection,
) -> None:
    logger.info("Upserting metadata")
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("""
            INSERT INTO {}.metadata ("table", collection, "name", "provider", last_updated)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT ("table") DO UPDATE SET last_updated = EXCLUDED.last_updated
        """).format(sql.Identifier(schema)),
            (table, collection, name, provider, last_updated),
        )
        if item is not None:
            cursor.execute(
                sql.SQL("""
                INSERT INTO {}.metadata_assets ("table", item, remote_updated)
                VALUES (%s, %s, %s)
                ON CONFLICT ("table", item) DO UPDATE SET remote_updated = EXCLUDED.remote_updated
            """).format(sql.Identifier(schema)),
                (table, item, last_updated),
            )
    connection.commit()
