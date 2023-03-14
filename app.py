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
from processing.configs import MAPBOX_ACCESS_TOKEN, infostrings, TOOLTIPS, WUNDER_DIR

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}],
)
app.title = "Weather Underground Precipitation Reports"
server = app.server
px.set_mapbox_access_token(MAPBOX_ACCESS_TOKEN)

accum_periods = {
    '15-minute': '15_min',
    '30-minute': '30_min',
    '1-hour': '60_min',
    '3-hours': '180_min',
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
                        html.H2("WEATHER UNDERGROUND PRECIP OBSERVATIONS"),

                        html.H2(
                            'Accumulation period to display on map',
                            style={'font-size': '15px'}
                        ),

                        dcc.Dropdown(
                            id='accum-period',
                            options=[
                                {'label': i, 'value': i} for i in 
                                accum_periods.keys()
                            ],
                            value='30-minute',
                        ),

                        # Thresholding filter (turn off lable hover and scatter plot)
                        html.H2(
                            'Threshold to filter out observations ',
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
                            0.0, 0.25, 0.02, value=.05, id='display-threshold'
                        ),

                        html.H2(
                            'Set the maximum colorbar value',
                            style={'font-size': '15px'}
                        ),
                        dcc.Slider(
                            0.5, 6., .5, value=.5, id='cbar-max'
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
                                            target='_blank'),
                                        html.P(),

                                        html.H2('PRECIPITATION HISTOGRAM',
                                                style={'font-size': '18px'}),
                                        #html.P('3-hour'),
                                        #dcc.Graph(id='histogram-3hour'),

                                        #html.P('1-hour'),
                                        dcc.Graph(id='histogram-1hour'),

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
)
def generate_timeseries(clickData):
    df = pd.read_parquet(f'{WUNDER_DIR}/merged_tiles.parquet')
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

        time_series_fig = px.line(filtered, x='dateutc', y='precip', height=300)
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
    Output('map-graph', 'figure'),
    Output('histogram-1hour', 'figure'),
    Output('text-timestamp', 'children'),
    Output('data-age-alert', 'children'),
    Output('num-obs', 'children'),
    Input('map-update', 'n_intervals'),
    Input('accum-period', 'value'),
    Input('display-threshold', 'value'),
    Input('cbar-max', 'value'),
)
def update_graph(dummy, accum_period, display_threshold, cbar_max):
    df = pd.read_parquet(f'{WUNDER_DIR}/latest_obs.parquet')
    df.fillna(0, inplace=True)
    data_time = df['latest_ob_time'].max()

    # Remove data below user-requested threshold for the displayed points
    color_var = accum_periods[accum_period]
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

    # To maintain zoom level after auto-refresh
    fig['layout']['uirevision'] = 'something'
    return fig, hist_1hour, timestring, data_age_alert, num_obs

if __name__ == "__main__":
    app.run_server(debug=True)


