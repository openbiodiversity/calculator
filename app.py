import gradio as gr
import plotly.graph_objects as go
from datasets import load_dataset
import ee
# import geemap

# GEE
service_account = 'climatebase-july-2023@ee-geospatialml-aquarry.iam.gserviceaccount.com'
credentials = ee.ServiceAccountCredentials(service_account, 'service_account.json')
ee.Initialize(credentials)

# Gradio dataset
dataset = load_dataset("gradio/NYC-Airbnb-Open-Data", split="train")
df = dataset.to_pandas()

def filter_map(min_price, max_price, boroughs):

    filtered_df = df[(df['neighbourhood_group'].isin(boroughs)) & 
          (df['price'] > min_price) & (df['price'] < max_price)]
    names = filtered_df["name"].tolist()
    prices = filtered_df["price"].tolist()
    text_list = [(names[i], prices[i]) for i in range(0, len(names))]
    fig = go.Figure(go.Scattermapbox(
            customdata=text_list,
            lat=filtered_df['latitude'].tolist(),
            lon=filtered_df['longitude'].tolist(),
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=6
            ),
            hoverinfo="text",
            hovertemplate='<b>Name</b>: %{customdata[0]}<br><b>Price</b>: $%{customdata[1]}'
        ))

    fig.update_layout(
        mapbox_style="open-street-map",
        hovermode='closest',
        mapbox=dict(
            bearing=0,
            center=go.layout.mapbox.Center(
                lat=40.67,
                lon=-73.90
            ),
            pitch=0,
            zoom=9
        ),
    )

    return fig

with gr.Blocks() as demo:
    with gr.Column():
        with gr.Row():
            min_price = gr.Number(value=250, label="Project Name")
            max_price = gr.Number(value=1000, label="Project Description")
        boroughs = gr.CheckboxGroup(choices=["Queens", "Brooklyn", "Manhattan", "Bronx", "Staten Island"], value=["Queens", "Brooklyn"], label="Select Methodology:")
        btn = gr.Button(value="Update Filter")
        btn = gr.Button(value="Save")
        btn = gr.Button(value="Run")
        map = gr.Plot().style()
    demo.load(filter_map, [min_price, max_price, boroughs], map)
    btn.click(filter_map, [min_price, max_price, boroughs], map)

demo.launch()
