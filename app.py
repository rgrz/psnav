# Import Components for Dash Server
from dash import Dash, html, dcc, Input, Output, State
import dash_interactive_graphviz

import graphviz

from datetime import datetime

from graph_generator import update_graph

# Generate a Dash Server
# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

app = Dash(__name__)
# app = Dash(prevent_initial_callbacks=True)

global f

f = update_graph('Component', 'HRCD_CO_DIRECTORY', ['Component', 'Page'], [''], 'Only Standard')

colors = {
    'background': 'aliceblue',
    'text': 'darkslategray'
}

app.layout = html.Div(
    style={'backgroundColor': colors['background']},
    children=[
    html.Div(
        className="app-header",
        children=[
            html.Div('PeopleSoft Object Explorer - Graphviz Plotly Dash', className="app-header--title")
        ], style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),
    html.Div([
            html.Label('Select Input Object Type'),
            dcc.RadioItems(['Component', 'Page', 'Record'], 'Component', id='input_type'),
            html.Label(' '),
            dcc.Input(id='my_input', value='HRCD_CO_DIRECTORY', type='text'),
            html.Button(id='submit-button-state', n_clicks=0, children='Submit'),
            html.Br(),
            html.Br(),
            html.Label('Detail Level'),
            dcc.Checklist(['Component', 'Page', 'SubPages', 'Record'], ['Component', 'Page'], id='detail_level'),
            html.Br(),
            html.Label('PeopleCode Events'),
            dcc.Checklist(['Component PeopleCode', 'Component Record PeopleCode', 'Component RecField PeopleCode', 'Page PeopleCode', 'Application Packages', 'Function Calls', 'Function Definitions'], [''], id='pcode_detail'),
            html.Br(),
            html.Label('Show Customized Objects'),
            dcc.RadioItems(['Custom','Only Standard'], 'Only Standard', id='custom_obj'),
            html.Br(),
            html.Button("Download Source", id="btn-download-src"),
            dcc.Download(id="download-source"),
            html.Button("Download svg", id="btn-download-svg"),
            dcc.Download(id="download-svg"), 
        ], className="filter_div_class",),
    html.Div([
            dash_interactive_graphviz.DashInteractiveGraphviz(
                id="graph",
                dot_source=f.source
            ),
        ], className="graph_div_class",),
])

@app.callback(
    Output('graph', 'dot_source'),
    Input('submit-button-state', 'n_clicks'),
    State('input_type', 'value'),
    State('my_input', 'value'),
    Input('detail_level', 'value'),
    Input('pcode_detail', 'value'),
    Input('custom_obj', 'value'),
    prevent_initial_call=True,
)
def new_graph(n_clicks, input_type, my_input, detail_level, pcode_detail, custom_obj):
    global f
    print(f'Generating New Graph: {input_type}, {my_input}, {detail_level}, {pcode_detail}, {custom_obj}')
    f = update_graph(input_type, my_input, detail_level, pcode_detail, custom_obj)
    
    return f.source

@app.callback(
    Output("download-source", "data"),
    Input("btn-download-src", "n_clicks"),
    State('input_type', 'value'),
    State('my_input', 'value'),
    prevent_initial_call=True,
)
def download_source(n_clicks, input_type, my_input):
    now = datetime.now()
    format_data = "%d%m%y_%H%M%S"
    current_time = now.strftime(format_data)

    filename = ''.join(['graph_', input_type, '_', my_input, '_',current_time, '.txt'])

    with open(filename, 'w') as f_source:
        f_source.write(f.source)

    # return dcc.send_file("./graphviz_src.txt")
    return dcc.send_file(filename)

@app.callback(
    Output("download-svg", "data"),
    Input("btn-download-svg", "n_clicks"),
    State('input_type', 'value'),
    State('my_input', 'value'),
    prevent_initial_call=True,
)
def download_svg(n_clicks, input_type, my_input):
    now = datetime.now()

    format_data = "%d%m%y_%H%M%S"

    current_time = now.strftime(format_data)
    print("Current Time =", current_time)
    filename = ''.join(['graph_', input_type, '_', my_input, '_', current_time,'.svg'])
    with open(filename, 'wb') as f_svg:
        f_svg.write(graphviz.Source(f.source, format='svg').pipe())

    return dcc.send_file(filename)

if __name__ == '__main__':
    app.run(debug=True)