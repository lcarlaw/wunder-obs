import dash
from dash import html 
from dash import dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import json

import plotly.express as px 
from datetime import datetime
from processing.configs import MAPBOX_ACCESS_TOKEN, TOOLTIPS, WUNDER_DIR

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}],
)
app.title = "Weather Underground Observation Viewer"
server = app.server
px.set_mapbox_access_token(MAPBOX_ACCESS_TOKEN)

accum_periods = {
    '15-minutes': 'precip_15_min',
    '30-minutes': 'precip_30_min',
    '1-hour': 'precip_60_min',
    '3-hours': 'precip_180_min',
}

# Swap keys with items and merge with labels dict. This is used in scatter_mapbox 
# to control naming conventions for each column. 
res = dict((v,k) for k,v in accum_periods.items())
labels = {
    'siteid': 'Site ID',
    'latest_ob_time': 'Observation Time',
}
labels.update(res)

with open('assets/cwa.json', 'r') as f: cwas = json.load(f)
with open('assets/counties.json', 'r') as f: counties = json.load(f)

# Layout of Dash App
app.layout = html.Div(
    children=[
        html.Div(
            className="row",
            children=[
                # Column for user controls
                html.Div(
                    className="three columns div-user-controls",
                    children=[
                        html.A(
                            
                        ),
                        html.H2("WEATHER UNDERGROUND OBSERVATION VIEWER"),

                        html.H2(
                            'Select Weather Element to Display',
                            style={'font-size': '15px'}
                        ),

                        dcc.RadioItems(
                            id='weather-element',
                            options=['Precip Rates', 'Wind Gusts'],
                            value='Precip Rates', 
                            inputStyle={'margin-left': '50px', 'margin-right': '5px'}
                        ),

                        html.H2(
                            'Time period to display on map',
                            style={'font-size': '15px'}
                        ),

                        dcc.Dropdown(
                            id='accum-period',
                            options=[
                                {'label': i, 'value': i} for i in 
                                accum_periods.keys()
                            ],
                            value='30-minutes',
                            #disabled=False,
                        ),

                        # Thresholding filter (turn off lable hover and scatter plot)
                        html.H2(
                            'Filter out observations below this value',
                            #html.Span(
                            #    '[?]', 
                            #    id='tooltip-display-threshold',
                            #    style={'textDecoration': 'underline', 
                            #            'cursor': 'pointer'},
                            style={'font-size': '15px'}
                            #),    
                        ),
                        dbc.Tooltip(
                            TOOLTIPS['display-threshold'],
                            target='tooltip-display-threshold'
                        ),  
                                
                        dcc.Slider(
                            min=0.1, 
                            max=1., 
                            step=0.1, 
                            value=.1, 
                            id='display-threshold'
                        ),

                        html.H2(
                            'Set the maximum colorbar value',
                            style={'font-size': '15px'}
                        ),
                        dcc.Slider(
                            min=0.5,
                            max=8.,
                            step=0.5,
                            value=.5, 
                            id='cbar-max'
                        ),

                        html.Hr(),
                        html.H2(),
                        #dcc.Markdown(infostrings['general-info']),
                        #html.H2(),
                        #html.Hr(),
                        html.P(id='text-timestamp'),
                        html.P(id='data-age-alert',
                               style={'color': 'red',
                                      'font-weight': 'bold'}),
                        html.P(id='num-obs'),

                        # Change to side-by-side for mobile layout
                        html.Div(
                            className="row",
                            children=[
                                html.Div(
                                    className="div-for-dropdown",
                                    children=[        
                                        html.H2('POINT - TIME SERIES',
                                                style={'font-size': '18px'}),
                                        dcc.Graph(id='time-series'),
                                        html.P(),

                                        html.H2('CLICKABLE LINK',
                                                style={'font-size': '18px'}),
                                        dcc.Link(
                                            id='clickable-link', 
                                            href='',
                                            target='_blank',
                                            style={'font-size': '13px'}
                                            ),
                                        html.P(),

                                        #html.H2('PRECIPITATION HISTOGRAM',
                                        #        style={'font-size': '18px'}),
                                        #html.P('3-hour'),
                                        #dcc.Graph(id='histogram-3hour'),

                                        #html.P('1-hour'),
                                        #dcc.Graph(id='histogram-1hour'),

                                        html.Hr(),
                                        dcc.Interval(
                                            id="map-update", 
                                            interval=30000,
                                        ),
                                    ],
                                ),
                                html.Div(
                                    className="div-for-dropdown",
                                    children=[
                                        # Dropdown to select times            
                                    ],
                                ),
                            ],
                        ),
                        dcc.Markdown(
                            """
                            """
                        ),
                    ],
                ),
                # Column for app graphs and plots
                html.Div(
                    className="nine columns div-for-charts bg-grey",
                    children=[
                        dcc.Graph(id="map-graph"),
                    ],
                ),
            ],
        )
    ]
)

# Selected data point from the map pulls up a time series in the left navbar
@app.callback(
    Output('time-series', 'figure'),
    Output('clickable-link', 'href'),
    Input('map-graph', 'clickData'),
    Input('weather-element', 'value')
)
def generate_timeseries(clickData, weather_element):
    df = pd.read_parquet(f'{WUNDER_DIR}/merged_tiles.parquet')
    display_var = 'precip'
    if weather_element == 'Wind Gusts':
        display_var = 'windgust'

    #df.fillna(0, inplace=True)
    time_series_fig = px.line([], height=300)
    time_series_fig.update_layout(
        margin=dict(l=10, r=10, t=0, b=0),
        showlegend=False,
        plot_bgcolor="#1E1E1E",
        paper_bgcolor="#1E1E1E",
        font=dict(color="white"),
        xaxis=dict(
            showgrid=False,
        ),
        yaxis=dict(
            showgrid=False,
        ),
    )
    url_link = ""
    if clickData:
        NUM_HOURS = 3
        siteid = clickData['points'][0]['customdata'][0]
        rows = df.loc[df.siteid==siteid]
        rows.sort_values(by=['siteid', 'dateutc'], inplace=True)
        rows.reset_index(inplace=True)
        end_dt = rows['dateutc'].iloc[-1]
        start_dt = end_dt - pd.to_timedelta(NUM_HOURS, unit='hours')
        deltas_start = (start_dt - rows.dateutc).abs()
        deltas_end = (end_dt - rows.dateutc).abs()
        filtered = rows.loc[deltas_start.idxmin():deltas_end.idxmin()]

        time_series_fig = px.line(filtered, x='dateutc', y=display_var, height=300)
        time_series_fig.update_layout(
            margin=dict(l=10, r=10, t=0, b=0),
            showlegend=False,
            plot_bgcolor="#1E1E1E",
            paper_bgcolor="#1E1E1E",
            font=dict(color="white"),
            xaxis=dict(
                showgrid=False,
            ),
            yaxis=dict(
                showgrid=False,
            ),
        )

        url_link = f"https://www.wunderground.com/dashboard/pws/{siteid}"
    return time_series_fig, url_link

def normalize_precip_values(df, maxval=2.):
    #maxval = float(maxval)
    #df['norm'] =df['60_min'].clip(0, maxval) / maxval
    df['norm'] = 15.
    return 

@app.callback(
    Output('display-threshold', 'min'),
    Output('display-threshold', 'max'),
    Output('display-threshold', 'step'),
    Output('display-threshold', 'value'),
    Output('cbar-max', 'min'),
    Output('cbar-max', 'max'),
    Output('cbar-max', 'step'),
    Output('cbar-max', 'value'),
    Input('weather-element', 'value'),
)
def update_configs(weather_element):
    thresh_min_val = 0.1
    thresh_max_val = 1 
    thresh_step = 0.1 
    thresh_val = 0.1

    cbar_min_val = 3
    cbar_max_val = 15 
    cbar_step = 1
    cbar_val = 7
    if weather_element == 'Wind Gusts':
        thresh_min_val = 10
        thresh_max_val = 120 
        thresh_step = 10
        thresh_val = 10

        cbar_min_val = 20
        cbar_max_val = 120
        cbar_step = 10
        cbar_val = 70
    return (thresh_min_val, thresh_max_val, thresh_step, thresh_val, cbar_min_val, 
           cbar_max_val, cbar_step, cbar_val)
 

@app.callback(
    Output('map-graph', 'figure'),
    #Output('histogram-1hour', 'figure'),
    Output('text-timestamp', 'children'),
    Output('data-age-alert', 'children'),
    Output('num-obs', 'children'),
    Input('map-update', 'n_intervals'),
    Input('accum-period', 'value'),
    Input('display-threshold', 'value'),
    Input('cbar-max', 'value'),
    Input('weather-element', 'value'),
)
def update_graph(dummy, accum_period, display_threshold, cbar_max, weather_element):
    df = pd.read_parquet(f'{WUNDER_DIR}/latest_obs.parquet')
    df.fillna(0, inplace=True)
    data_time = df['latest_ob_time'].max()

    # Remove data below user-requested threshold for the displayed points
    color_var = accum_periods[accum_period]
    if weather_element == 'Wind Gusts':
        color_var = color_var.replace('precip', 'peakgust')
    df = df.loc[(df[color_var] >= display_threshold)]
    num_obs = f"{len(df)} observations displayed"

    # Need to figure out a way to normalize data points. scatter_mapbox seeems to take
    # min and max of data set each iteration, so sizes change each time. For now,
    # normalize_precip_values just makes a dummy variable with constant values.
    normalize_precip_values(df, maxval=300.)

    datestring = data_time.strftime("%Y-%m-%d %H:%M UTC")
    timestring = f'Most recent observations from: {datestring}'
    delta = datetime.utcnow() - data_time

    # Data age alert if more than 20 minutes old
    data_age_alert = ''
    if delta.total_seconds() > 1200:
        age_minutes = int(delta.total_seconds() / 60.)
        data_age_alert = f"WARNING: Observations are {age_minutes} minutes old"

    fig = px.scatter_mapbox(df, lat='lat', lon='lon', color=color_var, size='norm',
                            color_continuous_scale=px.colors.sequential.thermal,
                            size_max=10, hover_data=labels.keys(),
                            range_color=[0, cbar_max], height=1100,
                            center={'lat':41.8, 'lon':-88.5},
                            labels=labels, opacity=0.7, zoom=7,
                            )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        autosize=True,
        showlegend=False,
        mapbox_style='outdoors', # streets, dark, outdoors, light, basic
        mapbox_layers=[
            {
                "source": counties,
                "below": "traces",
                "type": "line",
                "color": "gray",
                "line": {"width": 1},
                "opacity": 0.5
            },
            {
                "source": cwas,
                "below": "traces",
                "type": "line",
                "color": "black",
                "line": {"width": 2},
            },
        ],
        )
    
    '''
    hist_1hour = px.histogram(df, x='60_min', log_y=True, height=200)
    hist_1hour.update_layout(
        xaxis_title='Preciptation Amounts',
        yaxis_title='',
        bargap=0,
        bargroupgap=0,
        margin=dict(l=10, r=10, t=0, b=0),
        showlegend=False,
        plot_bgcolor="#1E1E1E",
        paper_bgcolor="#1E1E1E",
        font=dict(color="white"),
        xaxis=dict(
            range=[0, 2],
            nticks=30,
            showgrid=False,
            fixedrange=True,
        ),
        yaxis=dict(
            showticklabels=True,
            showgrid=False,
            fixedrange=True,
        ),
    )
    '''

    # To maintain zoom level after auto-refresh
    fig['layout']['uirevision'] = 'something'
    return fig, timestring, data_age_alert, num_obs

if __name__ == "__main__":
    app.run_server(debug=True)


