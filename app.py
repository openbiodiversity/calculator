import gradio as gr

from utils import duckdb_queries as dq
from utils.gradio import get_window_url_params
from utils.indicators import IndexGenerator

# Instantiate outside gradio app to avoid re-initializing GEE, which is slow
indexgenerator = IndexGenerator(indices=["NDWI", "Water", "Protected", "Habitat"])

with gr.Blocks() as demo:
    with gr.Column():
        m1 = gr.Plot()
        with gr.Row():
            project_name = gr.Dropdown([], label="Project", value="Select project")
            start_year = gr.Number(value=2017, label="Start Year", precision=0)
            end_year = gr.Number(value=2022, label="End Year", precision=0)
        with gr.Row():
            view_btn = gr.Button(value="Show project map")
            calc_btn = gr.Button(value="Calculate!")
        results_df = gr.Dataframe(
            headers=["Year", "Project Name", "Score"],
            datatype=["number", "str", "number"],
            label="Scores by year",
        )
    calc_btn.click(
        indexgenerator.calculate_score,
        inputs=[start_year, end_year],
        outputs=results_df,
    )
    view_btn.click(
        fn=indexgenerator.show_project_map,
        outputs=[m1],
    )

    def update_project_dropdown_list(url_params):
        username = url_params.get("username", "default")
        projects = dq.list_projects_by_author(author_id=username)
        # Initialize the first project in the list
        project_names = projects['name'].tolist()
        default_project = project_names[0]
        return gr.Dropdown.update(choices=project_names, value=default_project)

    # Change the project name in the index generator object when the
    # user selects a new project
    project_name.change(
        indexgenerator.set_project,
        inputs=project_name
    )

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
