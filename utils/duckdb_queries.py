import os
import duckdb

import logging


# Configure DuckDB connection
logging.info("Configuring DuckDB connection...")

if not os.getenv("motherduck_token"):
    raise Exception(
        "No motherduck token found. Please set the `motherduck_token` environment variable."
    )
else:
    con = duckdb.connect("md:climatebase")
    con.sql("USE climatebase;")

# load extensions
con.sql("""INSTALL spatial; LOAD spatial;""")
logging.info("Configured DuckDB connection.")


def list_projects_by_author(author_id):
    return con.execute(
        "SELECT DISTINCT name FROM project WHERE authorId = ? AND geometry != 'null'",
        [author_id],
    ).df()

def get_project_geometry(project_name):
    return con.execute("SELECT geometry FROM project WHERE name = ? LIMIT 1", [project_name]).fetchall()