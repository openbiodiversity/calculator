import json
import os

import duckdb

# Configure DuckDB connection
if not os.getenv("motherduck_token"):
    raise Exception(
        "No motherduck token found. Please set the `motherduck_token` environment variable."
    )
else:
    con = duckdb.connect("md:climatebase")
    con.sql("USE climatebase;")
    # load extensions
    con.sql("""INSTALL spatial; LOAD spatial;""")


# to-do: pass con through decorator
def list_projects_by_author(author_id):
    return con.execute(
        "SELECT DISTINCT name FROM project WHERE (authorId = ? OR authorId = 'default') AND (geometry IS NOT NULL)",
        [author_id],
    ).df()


def get_project_geometry(project_name):
    return con.execute(
        "SELECT geometry FROM project WHERE name = ? LIMIT 1", [project_name]
    ).fetchall()


def get_project_centroid(project_name):
    # Workaround to get centroid of project
    # To-do: refactor to only use DuckDB spatial extension
    _geom = get_project_geometry(project_name)
    _polygon = json.dumps(json.loads(_geom[0][0])["features"][0]["geometry"])
    return con.sql(
        f"SELECT ST_X(ST_Centroid(ST_GeomFromGeoJSON('{_polygon}'))) AS longitude, ST_Y(ST_Centroid(ST_GeomFromGeoJSON('{_polygon}'))) AS latitude;"
    ).fetchall()[0]


def get_project_scores(project_name, start_year, end_year):
    return con.execute(
        "SELECT * FROM bioindicator WHERE (year >= ? AND year <= ? AND project_name = ?)",
        [start_year, end_year, project_name],
    ).df()


def check_if_project_exists_for_year(project_name, year):
    return con.execute(
        "SELECT COUNT(1) FROM bioindicator WHERE (year = ? AND project_name = ?)",
        [year, project_name],
    ).fetchall()[0][0]


def write_score_to_temptable(df):
    con.sql(
        "CREATE OR REPLACE TABLE _temptable AS SELECT *, ROUND((value * area), 2) AS score FROM (SELECT year, project_name, ROUND(AVG(value), 2) AS value, area FROM df GROUP BY year, project_name, area ORDER BY project_name)"
    )
    return True


def get_or_create_bioindicator_table():
    con.sql(
        """
            USE climatebase;
            CREATE TABLE IF NOT EXISTS bioindicator (year BIGINT, project_name VARCHAR(255), value DOUBLE, area DOUBLE, score DOUBLE, CONSTRAINT unique_year_project_name UNIQUE (year, project_name));
            """
    )
    return True


def upsert_project_record():
    con.sql(
        """
                INSERT INTO bioindicator FROM _temptable
                ON CONFLICT (year, project_name) DO UPDATE SET value = excluded.value;
            """
    )
    return True
