import datetime
import os

import duckdb
import ee
import gradio as gr
import pandas as pd
import plotly.graph_objects as go
import yaml


# Define constants
DATE = "2020-01-01"
YEAR = 2020
LOCATION = [-74.653370, 5.845328]
ROI_RADIUS = 20000
GEE_SERVICE_ACCOUNT = (
    "climatebase-july-2023@ee-geospatialml-aquarry.iam.gserviceaccount.com"
)
GEE_SERVICE_ACCOUNT_CREDENTIALS_FILE = "ee_service_account.json"
INDICES_FILE = "indices.yaml"
START_YEAR = 2015
END_YEAR = 2022


class IndexGenerator:
    """
    A class to generate indices and compute zonal means.

        Args:
            centroid (tuple): The centroid coordinates (latitude, longitude) of the region of interest.
            year (int): The year for which indices are generated.
            roi_radius (int, optional): The radius (in meters) for creating a buffer around the centroid as the region of interest. Defaults to 20000.
            project_name (str, optional): The name of the project. Defaults to "".
            map (geemap.Map, optional): Map object for mapping. Defaults to None (i.e. no map created)
    """

    def __init__(
        self,
        centroid,
        roi_radius,
        year,
        indices_file,
        project_name="",
        map=None,
    ):
        self.indices = self._load_indices(indices_file)
        self.centroid = centroid
        self.roi = ee.Geometry.Point(*centroid).buffer(roi_radius)
        self.year = year
        self.start_date = str(datetime.date(self.year, 1, 1))
        self.end_date = str(datetime.date(self.year, 12, 31))
        self.daterange = [self.start_date, self.end_date]
        self.project_name = project_name
        self.map = map
        if self.map is not None:
            self.show = True
        else:
            self.show = False

    def _cloudfree(self, gee_path):
        """
        Internal method to generate a cloud-free composite.

        Args:
            gee_path (str): The path to the Google Earth Engine (GEE) image or image collection.

        Returns:
            ee.Image: The cloud-free composite clipped to the region of interest.
        """
        # Load a raw Landsat ImageCollection for a single year.
        collection = (
            ee.ImageCollection(gee_path)
            .filterDate(*self.daterange)
            .filterBounds(self.roi)
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
                print(e)
                return None

    def show_map(self, map=None):
        if map is not None:
            self.map = map
            self.show = True

    def disable_map(self):
        self.show = False

    def generate_index(self, index_config):
        """
        Generates an index based on the provided index configuration.

        Args:
            index_config (dict): Configuration for generating the index.

        Returns:
            ee.Image: The generated index clipped to the region of interest.
        """
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
                image = self._cloudfree(index_config["gee_path"])
                dataset = image.normalizedDifference(["B4", "B3"])
            case _:
                dataset = None

        if not dataset:
            raise Exception("Failed to generate dataset.")
        if self.show and index_config.get("show"):
            map.addLayer(dataset, index_config["viz"], index_config["name"])
        print(f"Generated index: {index_config['name']}")
        return dataset

    def zonal_mean_index(self, index_key):
        index_config = self.indices[index_key]
        dataset = self.generate_index(index_config)
        # zm = self._zonal_mean(single, index_config.get('bandname') or 'constant')
        out = dataset.reduceRegion(
            **{
                "reducer": ee.Reducer.mean(),
                "geometry": self.roi,
                "scale": 200,  # map scale
            }
        ).getInfo()
        if index_config.get("bandname"):
            return out[index_config.get("bandname")]
        return out

    def generate_composite_index_df(self, indices=[]):
        data = {
            "metric": indices,
            "year": self.year,
            "centroid": str(self.centroid),
            "project_name": self.project_name,
            "value": list(map(self.zonal_mean_index, indices)),
            "area": self.roi.area().getInfo(),  # m^2
            "geojson": str(self.roi.getInfo()),
        }

        print("data", data)
        df = pd.DataFrame(data)
        return df


def set_up_duckdb():
    print("set up duckdb")
    # use `climatebase` db
    if not os.getenv("motherduck_token"):
        raise Exception(
            "No motherduck token found. Please set the `motherduck_token` environment variable."
        )
    else:
        con = duckdb.connect("md:climatebase")
        con.sql("USE climatebase;")

    # load extensions
    con.sql("""INSTALL spatial; LOAD spatial;""")

    return con


def authenticate_gee(gee_service_account, gee_service_account_credentials_file):
    print("authenticate_gee")
    # to-do: alert if dataset filter date nan
    credentials = ee.ServiceAccountCredentials(
        gee_service_account, gee_service_account_credentials_file
    )
    ee.Initialize(credentials)


def load_indices(indices_file):
    # Read index configurations
    with open(indices_file, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as e:
            print(e)
            return None


def create_dataframe(years, project_name):
    dfs = []
    print(years)
    indices = load_indices(INDICES_FILE)
    for year in years:
        print(year)
        ig = IndexGenerator(
            centroid=LOCATION,
            roi_radius=ROI_RADIUS,
            year=year,
            indices_file=INDICES_FILE,
            project_name=project_name,
        )
        df = ig.generate_composite_index_df(list(indices.keys()))
        dfs.append(df)
    return pd.concat(dfs)


# def preview_table():
#     con.sql("FROM bioindicator;").show()

# if __name__ == '__main__':


# Map = geemap.Map()


# # Create a cloud-free composite with custom parameters for cloud score threshold and percentile.
# composite_cloudfree = ee.Algorithms.Landsat.simpleComposite(**{
#   'collection': collection,
#   'percentile': 75,
#   'cloudScoreRange': 5
# })

# Map.addLayer(composite_cloudfree, {'bands': ['B4', 'B3', 'B2'], 'max': 128}, 'Custom TOA composite')
# Map.centerObject(roi, 14)


# ig = IndexGenerator(centroid=LOCATION, year=2015, indices_file=INDICES_FILE, project_name='Test Project', map=Map)
# dataset = ig.generate_index(indices['Air'])

# minMax = dataset.clip(roi).reduceRegion(
#   geometry = roi,
#   reducer = ee.Reducer.minMax(),
#   scale= 3000,
#   maxPixels= 10e3,
# )


# minMax.getInfo()
def calculate_biodiversity_score(start_year, end_year, project_name):
    years = []
    for year in range(start_year, end_year):
        row_exists = con.sql(
            f"SELECT COUNT(1) FROM bioindicator WHERE (year = {year} AND project_name = '{project_name}')"
        ).fetchall()[0][0]
        if not row_exists:
            years.append(year)

    if len(years) > 0:
        df = create_dataframe(years, project_name)
        # con.sql('FROM df LIMIT 5').show()

        # Write score table to `_temptable`
        con.sql(
            "CREATE OR REPLACE TABLE _temptable AS SELECT *, (value * area) AS score FROM (SELECT year, project_name, AVG(value) AS value, area  FROM df GROUP BY year, project_name, area ORDER BY project_name)"
        )

        # Create `bioindicator` table IF NOT EXISTS.
        con.sql(
            """
            USE climatebase;
            CREATE TABLE IF NOT EXISTS bioindicator (year BIGINT, project_name VARCHAR(255), value DOUBLE, area DOUBLE, score DOUBLE, CONSTRAINT unique_year_project_name UNIQUE (year, project_name));
        """
        )

    return con.sql(
        f"SELECT * FROM bioindicator WHERE (year > {start_year} AND year <= {end_year} AND project_name = '{project_name}')"
    ).df()


def view_all():
    print("view_all")
    return con.sql(f"SELECT * FROM bioindicator").df()


def push_to_md():
    # UPSERT project record
    con.sql(
        """
        INSERT INTO bioindicator FROM _temptable
        ON CONFLICT (year, project_name) DO UPDATE SET value = excluded.value;
    """
    )
    print("Saved records")


#   preview_table()


def filter_map(min_price, max_price, boroughs):
    filtered_df = df[
        (df["neighbourhood_group"].isin(boroughs))
        & (df["price"] > min_price)
        & (df["price"] < max_price)
    ]
    names = filtered_df["name"].tolist()
    prices = filtered_df["price"].tolist()
    text_list = [(names[i], prices[i]) for i in range(0, len(names))]
    fig = go.Figure(
        go.Scattermapbox(
            customdata=text_list,
            lat=filtered_df["latitude"].tolist(),
            lon=filtered_df["longitude"].tolist(),
            mode="markers",
            marker=go.scattermapbox.Marker(size=6),
            hoverinfo="text",
            hovertemplate="<b>Name</b>: %{customdata[0]}<br><b>Price</b>: $%{customdata[1]}",
        )
    )

    fig.update_layout(
        mapbox_style="open-street-map",
        hovermode="closest",
        mapbox=dict(
            bearing=0,
            center=go.layout.mapbox.Center(lat=40.67, lon=-73.90),
            pitch=0,
            zoom=9,
        ),
    )

    return fig


with gr.Blocks() as demo:
    con = set_up_duckdb()
    authenticate_gee(GEE_SERVICE_ACCOUNT, GEE_SERVICE_ACCOUNT_CREDENTIALS_FILE)
    # Create circle buffer over point
    roi = ee.Geometry.Point(*LOCATION).buffer(ROI_RADIUS)

    # # Load a raw Landsat ImageCollection for a single year.
    # start_date = str(datetime.date(YEAR, 1, 1))
    # end_date = str(datetime.date(YEAR, 12, 31))
    # collection = (
    #     ee.ImageCollection('LANDSAT/LC08/C02/T1')
    #     .filterDate(start_date, end_date)
    #     .filterBounds(roi)
    # )

    # indices = load_indices(INDICES_FILE)
    # push_to_md(START_YEAR, END_YEAR, 'Test Project')
    with gr.Column():
        # map = gr.Plot().style()
        with gr.Row():
            start_year = gr.Number(value=2017, label="Start Year", precision=0)
            end_year = gr.Number(value=2022, label="End Year", precision=0)
            project_name = gr.Textbox(label="Project Name")
        # boroughs = gr.CheckboxGroup(choices=["Queens", "Brooklyn", "Manhattan", "Bronx", "Staten Island"], value=["Queens", "Brooklyn"], label="Select Methodology:")
        # btn = gr.Button(value="Update Filter")
        with gr.Row():
            calc_btn = gr.Button(value="Calculate!")
            view_btn = gr.Button(value="View all")
            save_btn = gr.Button(value="Save")
        results_df = gr.Dataframe(
            headers=["Year", "Project Name", "Score"],
            datatype=["number", "str", "number"],
            label="Biodiversity scores by year",
        )
    # demo.load(filter_map, [min_price, max_price, boroughs], map)
    # btn.click(filter_map, [min_price, max_price, boroughs], map)
    calc_btn.click(
        calculate_biodiversity_score,
        inputs=[start_year, end_year, project_name],
        outputs=results_df,
    )
    view_btn.click(view_all, outputs=results_df)
    save_btn.click(push_to_md)

demo.launch()
