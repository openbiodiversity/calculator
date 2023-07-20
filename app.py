import gradio as gr

from utils import duckdb_queries as dq
from utils.gradio import get_window_url_params
from utils.indicators import IndexGenerator

# Define constants
DATE = "2020-01-01"
YEAR = 2020
LOCATION = [-74.653370, 5.845328]
ROI_RADIUS = 20000
INDICES_FILE = "indices.yaml"


# Instantiate outside gradio app to avoid re-initializing GEE, which is slow
indexgenerator = IndexGenerator(
    centroid=LOCATION,
    roi_radius=ROI_RADIUS,
    indices_file=INDICES_FILE,
)

with gr.Blocks() as demo:
    print("start gradio app")

    with gr.Column():
        m1 = gr.Plot()
        with gr.Row():
            project_name = gr.Dropdown([], label="Project", value="Select project")
            start_year = gr.Number(value=2017, label="Start Year", precision=0)
            end_year = gr.Number(value=2022, label="End Year", precision=0)
        with gr.Row():
            view_btn = gr.Button(value="Show project map")
            calc_btn = gr.Button(value="Calculate!")
            # save_btn = gr.Button(value="Save")
        results_df = gr.Dataframe(
            headers=["Year", "Project Name", "Score"],
            datatype=["number", "str", "number"],
            label="Biodiversity scores by year",
        )
    calc_btn.click(
        indexgenerator.calculate_biodiversity_score,
        inputs=[start_year, end_year, project_name],
        outputs=results_df,
    )
    view_btn.click(
        fn=indexgenerator.show_project_map,
        inputs=[project_name],
        outputs=[m1],
    )

    def update_project_dropdown_list(url_params):
        username = url_params.get("username", "default")
        projects = dq.list_projects_by_author(author_id=username)
        # to-do: filter projects based on user
        return gr.Dropdown.update(choices=projects["name"].tolist())

    # Get url params
    url_params = gr.JSON({"username": "default"}, visible=False, label="URL Params")

    # Gradio has a bug
    # For dropdown to update by demo.load, dropdown value must be called downstream
    b1 = gr.Button("Hidden button that fixes bug.", visible=False)
    b1.click(lambda x: x, inputs=project_name, outputs=[])

    # Update project dropdown list on page load
    demo.load(
        fn=update_project_dropdown_list,
        inputs=[url_params],
        outputs=[project_name],
        _js=get_window_url_params,
        queue=False,
    )

demo.launch()
