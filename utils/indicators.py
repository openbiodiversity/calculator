import datetime
import json
import os
from itertools import repeat

import ee
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import yaml

from utils import duckdb_queries as dq

from . import logging

GEE_SERVICE_ACCOUNT = (
    "climatebase-july-2023@ee-geospatialml-aquarry.iam.gserviceaccount.com"
)
INDICES_FILE = "indices.yaml"


class IndexGenerator:
    """
    A class to generate indices and compute zonal means.

        Args:
            indices (string[], required): Array of index names to include in aggregate index generation.
    """

    def __init__(
        self,
        indices,
    ):
        # Authenticate to GEE & DuckDB
        self._authenticate_ee(GEE_SERVICE_ACCOUNT)

        # Use defined subset of indices
        all_indices = self._load_indices(INDICES_FILE)
        self.indices = {k: all_indices[k] for k in indices}

    def _cloudfree(self, gee_path, daterange):
        """
        Internal method to generate a cloud-free composite.

        Args:
            gee_path (str): The path to the Google Earth Engine (GEE) image or image collection.

        Returns:
            ee.Image: The cloud-free composite clipped to the region of interest.
        """
        # Load a raw Landsat ImageCollection for a single year.
        collection = (
            ee.ImageCollection(gee_path).filterDate(*daterange).filterBounds(self.roi)
        )

        # Create a cloud-free composite with custom parameters for cloud score threshold and percentile.
        composite_cloudfree = ee.Algorithms.Landsat.simpleComposite(
            **{"collection": collection, "percentile": 75, "cloudScoreRange": 5}
        )
        return composite_cloudfree.clip(self.roi)

    def _load_indices(self, indices_file):
        # Read index configurations
        with open(indices_file, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as e:
                logging.error(e)
                return None

    def generate_index(self, index_config, year):
        """
        Generates an index based on the provided index configuration.

        Args:
            index_config (dict): Configuration for generating the index.

        Returns:
            ee.Image: The generated index clipped to the region of interest.
        """

        # Calculate date range, assume 1 year
        start_date = str(datetime.date(year, 1, 1))
        end_date = str(datetime.date(year, 12, 31))
        daterange = [start_date, end_date]

        # Calculate index based on type
        logging.info(
            f"Generating index: {index_config['name']} of type {index_config['gee_type']}"
        )
        match index_config["gee_type"]:
            case "image":
                dataset = ee.Image(index_config["gee_path"]).clip(self.roi)
                if index_config.get("select"):
                    dataset = dataset.select(index_config["select"])
            case "image_collection":
                dataset = (
                    ee.ImageCollection(index_config["gee_path"])
                    .filterBounds(self.roi)
                    .map(lambda image: image.clip(self.roi))
                    .mean()
                )
                if index_config.get("select"):
                    dataset = dataset.select(index_config["select"])
            case "feature_collection":
                dataset = (
                    ee.Image()
                    .float()
                    .paint(
                        ee.FeatureCollection(index_config["gee_path"]),
                        index_config["select"],
                    )
                    .clip(self.roi)
                )
            case "algebraic":
                image = self._cloudfree(index_config["gee_path"], daterange)
                # to-do: params should come from index_config
                dataset = image.normalizedDifference(["B4", "B3"])
            case _:
                dataset = None

        if not dataset:
            raise Exception("Failed to generate dataset.")

        logging.info(f"Generated index: {index_config['name']}")
        return dataset

    def zonal_mean_index(self, index_key, year):
        index_config = self.indices[index_key]
        dataset = self.generate_index(index_config, year)

        logging.info(f"Calculating zonal mean for {index_key}...")
        out = dataset.reduceRegion(
            **{
                "reducer": ee.Reducer.mean(),
                "geometry": self.roi,
                "scale": 2000,  # map scale
                "bestEffort": True,
                "maxPixels": 1e3,
            }
        ).getInfo()

        if index_config.get("bandname"):
            return out[index_config.get("bandname")]

        logging.info(f"Calculated zonal mean for {index_key}.")
        return out

    def generate_composite_index_df(self, year, project_geometry, indices=[]):
        data = {
            "metric": indices,
            "year": year,
            "centroid": "",
            "project_name": "",
            "value": list(map(self.zonal_mean_index, indices, repeat(year))),
            # to-do: calculate with duckdb; also, should be part of project table instead
            "area": self.roi.area().getInfo(),  # m^2
            "geojson": "",
            # to-do: coefficient
        }

        logging.info("data", data)
        df = pd.DataFrame(data)
        return df

    @staticmethod
    def _authenticate_ee(ee_service_account):
        """
        Huggingface Spaces does not support secret files, therefore authenticate with an environment variable containing the JSON.
        """
        logging.info("Authenticating to Google Earth Engine...")
        credentials = ee.ServiceAccountCredentials(
            ee_service_account, key_data=os.environ["ee_service_account"]
        )
        ee.Initialize(credentials)
        logging.info("Authenticated to Google Earth Engine.")

    def _calculate_yearly_index(self, years, project_name):
        dfs = []
        logging.info(years)
        project_geometry = dq.get_project_geometry(project_name)
        project_centroid = dq.get_project_centroid(project_name)
        # to-do: refactor to involve less transformations
        _polygon = json.dumps(
            json.loads(project_geometry[0][0])["features"][0]["geometry"]
        )
        # to-do: don't use self.roi and instead pass patameter strategically
        self.roi = ee.Geometry.Polygon(json.loads(_polygon)["coordinates"])

        # to-do: pararelize?
        for year in years:
            logging.info(year)
            self.project_name = project_name
            df = self.generate_composite_index_df(
                year, project_geometry, list(self.indices.keys())
            )
            dfs.append(df)

        # Concatenate all dataframes
        df_concat = pd.concat(dfs)
        df_concat["centroid"] = str(project_centroid)
        df_concat["project_name"] = project_name
        df_concat["geojson"] = str(project_geometry)
        return df_concat.round(2)

    # h/t: https://community.plotly.com/t/dynamic-zoom-for-mapbox/32658/12
    def _latlon_to_config(self, longitudes=None, latitudes=None):
        """Function documentation:\n
        Basic framework adopted from Krichardson under the following thread:
        https://community.plotly.com/t/dynamic-zoom-for-mapbox/32658/7

        # NOTE:
        # THIS IS A TEMPORARY SOLUTION UNTIL THE DASH TEAM IMPLEMENTS DYNAMIC ZOOM
        # in their plotly-functions associated with mapbox, such as go.Densitymapbox() etc.

        Returns the appropriate zoom-level for these plotly-mapbox-graphics along with
        the center coordinate tuple of all provided coordinate tuples.
        """

        # Check whether both latitudes and longitudes have been passed,
        # or if the list lenghts don't match
        if (latitudes is None or longitudes is None) or (
            len(latitudes) != len(longitudes)
        ):
            # Otherwise, return the default values of 0 zoom and the coordinate origin as center point
            return 0, (0, 0)

        # Get the boundary-box
        b_box = {}
        b_box["height"] = latitudes.max() - latitudes.min()
        b_box["width"] = longitudes.max() - longitudes.min()
        b_box["center"] = (np.mean(longitudes), np.mean(latitudes))

        # get the area of the bounding box in order to calculate a zoom-level
        area = b_box["height"] * b_box["width"]

        # * 1D-linear interpolation with numpy:
        # - Pass the area as the only x-value and not as a list, in order to return a scalar as well
        # - The x-points "xp" should be in parts in comparable order of magnitude of the given area
        # - The zpom-levels are adapted to the areas, i.e. start with the smallest area possible of 0
        # which leads to the highest possible zoom value 20, and so forth decreasing with increasing areas
        # as these variables are antiproportional
        zoom = np.interp(
            x=area,
            xp=[0, 5**-10, 4**-10, 3**-10, 2**-10, 1**-10, 1**-5],
            fp=[20, 15, 14, 13, 12, 7, 5],
        )

        # Finally, return the zoom level and the associated boundary-box center coordinates
        return zoom, b_box["center"]

    def show_project_map(self, project_name):
        project_geometry = dq.get_project_geometry(project_name)
        features = json.loads(project_geometry[0][0].replace("'", '"'))["features"]
        geometry = features[0]["geometry"]
        longitudes = np.array(geometry["coordinates"])[0, :, 0]
        latitudes = np.array(geometry["coordinates"])[0, :, 1]
        zoom, bbox_center = self._latlon_to_config(longitudes, latitudes)
        fig = go.Figure(
            go.Scattermapbox(
                mode="markers",
                lon=[bbox_center[0]],
                lat=[bbox_center[1]],
                marker={"size": 20, "color": ["cyan"]},
            )
        )

        fig.update_layout(
            mapbox={
                "style": "stamen-terrain",
                "center": {"lon": bbox_center[0], "lat": bbox_center[1]},
                "zoom": zoom,
                "layers": [
                    {
                        "source": {
                            "type": "FeatureCollection",
                            "features": [{"type": "Feature", "geometry": geometry}],
                        },
                        "type": "fill",
                        "below": "traces",
                        "color": "royalblue",
                    }
                ],
            },
            margin={"l": 0, "r": 0, "b": 0, "t": 0},
        )

        return fig

    def calculate_biodiversity_score(self, start_year, end_year, project_name):
        years = []
        for year in range(start_year, end_year):
            row_exists = dq.check_if_project_exists_for_year(project_name, year)
            if not row_exists:
                years.append(year)

        if len(years) > 0:
            df = self._calculate_yearly_index(years, project_name)

            # Write score table to `_temptable`
            dq.write_score_to_temptable(df)

            # Create `bioindicator` table IF NOT EXISTS.
            dq.get_or_create_bioindicator_table()

            # UPSERT project record
            dq.upsert_project_record()
            logging.info("upserted records into motherduck")
        scores = dq.get_project_scores(project_name, start_year, end_year)
        return scores
