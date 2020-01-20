# -*- coding: utf-8 -*-
import json
import base64
import datetime
import requests
import math
import pandas as pd
import flask
import dash
import numpy as np
import dash_core_components as dcc
import dash_html_components as html
import chart_studio
import plotly.graph_objs as go
import dash_daq as daq

from dash.dependencies import Input, Output, State
from plotly import tools
from pymongo import MongoClient
from datetime import datetime
from pandas.io.json import json_normalize

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)

server = app.server

# Loading data from MongoDB
cluster = MongoClient("mongodb://rimho:0000@cluster0-shard-00-00-yehww.mongodb.net:27017,cluster0-shard-00-01-yehww.mongodb.net:27017,cluster0-shard-00-02-yehww.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority")
db = cluster['yahoo_finance_db']
companies = ['GOOGL', 'FB', 'TWTR', 'SNAP']
collections = [db[c] for c in companies]
documents =  [collection.find() for collection in collections]

products = []
for document in documents:
    for p in document:
        products.append(p)
table = json_normalize(products)
initial_df = table.drop(table.columns[[0, 5, 6, 7, 8, 12, 13, 14]], axis=1)

def make_each_df_with_revenue(name):
    all_df = pd.concat([initial_df['name'].to_frame(), initial_df['revenue_infos'].to_frame()], axis=1, sort=False)
    pre_df = all_df.loc[all_df['name'] == name]
    Year_Peoriodes, Dates, Total_Revenues, SMI_US_Ad_Revenues, ORIs, Capture_Rates, LAG, MAU, YoY_Y, Y_test, Y_t_YoY = [],[],[],[],[],[],[],[],[],[],[]
    value = pre_df['revenue_infos'].values[0]
    for i in range(len(value)):
        Year_Peoriodes.append(value[i]['Year_Peoriode'])
        Total_Revenues.append(value[i]['Total_Revenue'])
        SMI_US_Ad_Revenues.append(value[i]['SMI_US_Ad_Revenue'])
        ORIs.append(value[i]['ORI'])
        Capture_Rates.append(value[i]['Capture_Rate'])
        LAG.append(value[i]['LAG'])
        MAU.append(value[i]['MAU'])
        YoY_Y.append(value[i]['YoY_Y'])
        Y_test.append(value[i]['Y_test'])
        Y_t_YoY.append(value[i]['Y_t_YoY'])
    df = pd.DataFrame({'Year_Peoriode' : Year_Peoriodes, 'Total_Revenue' : Total_Revenues, 'SMI_US_Ad_Revenue' : SMI_US_Ad_Revenues, 'ORI' : ORIs, 'Capture_Rate' : Capture_Rates, 'LAG':LAG, 'MAU':MAU, 'YoY_Y':YoY_Y, 'Y_test':Y_test, 'Y_t_YoY':Y_t_YoY})
    return df

def make_each_df_with_stock(name):
    all_df = pd.concat([initial_df['name'].to_frame(), initial_df['stock_infos'].to_frame()], axis=1, sort=False)
    pre_df = all_df.loc[all_df['name'] == name]
    Date, Close_Stock = [],[]
    find_index = companies.index(name)
    key = list(pre_df['stock_infos'][find_index][0].keys())
    value = list(pre_df['stock_infos'][find_index][0].values())
    for i in range(len(value)):
        Date.append((key[i]))
        Close_Stock.append(value[i])
    df = pd.DataFrame({'Date' : Date, 'Stock' : Close_Stock})
    return df

def make_combined_df(company):
    revenue_df = make_each_df_with_revenue(company)
    stock_df = make_each_df_with_stock(company)
    stock_df = stock_df.append(pd.DataFrame([np.nan],columns=['Year_Peoriode']))[:-1]
    stock_df['Year_Peoriode'] = [str(datetime.strptime(d, '%Y-%m-%d').date().year) + '-' + 'Q1' if datetime.strptime(d, '%Y-%m-%d').date().month in [1,2,3] else str(datetime.strptime(d, '%Y-%m-%d').date().year) + '-' + 'Q2' if datetime.strptime(d, '%Y-%m-%d').date().month in [4,5,6] else str(datetime.strptime(d, '%Y-%m-%d').date().year) + '-' + 'Q3' if datetime.strptime(d, '%Y-%m-%d').date().month in [7,8,9] else str(datetime.strptime(d, '%Y-%m-%d').date().year) + '-' + 'Q4' for d in stock_df['Date']]
    df = pd.merge(revenue_df, stock_df, on='Year_Peoriode', sort=True)
    df = df.groupby('Year_Peoriode').agg({'Year_Peoriode': 'first', 'Total_Revenue': 'first', 'Stock': 'mean', 'Date': 'first', 'SMI_US_Ad_Revenue' : 'first', 'ORI' : 'first', 'Capture_Rate' : 'first', 'LAG': 'first', 'MAU':'first', 'YoY_Y': 'first', 'Y_test': 'first', 'Y_t_YoY':'first'})
    df = df.set_index([pd.Series([i for i in range(len(df['Year_Peoriode']))])])
    df = pd.concat([df, df.index.to_frame()], axis=1, sort=True)
    df.columns = ['Year_Peoriode','Total_Revenue','Stock','Date','SMI_US_Ad_Revenue','ORI','Capture_Rate','LAG','MAU','YoY_Y','Y_test','Y_t_YoY','Index']
    return df

def find_single_index_by_index(df, col_names, index):
    index_arr = df.index.values.tolist()
    return df[col_names].loc[index_arr[index]]

def find_single_value_by_index(df, col_names, index):
    return df[col_names][index]

def find_all_cells_with_index(df, col_names, index_range):
    return df.iloc[index_range[0]:index_range[1]+1]

# Returns dataset for currency pair with nearest datetime to current time
def first_ask_bid(company):
    items = currency_pair_data[currency_pair]
    dates = items.index.to_pydatetime()
    index = min(dates, key=lambda x: abs(x - t))
    df_row = items.loc[index]
    int_index = items.index.get_loc(index)
    return index_arr 

# Creates HTML Total Revenue and Stock Price
def get_row(company):
    estimated_total_revenue = find_single_index_by_index(make_combined_df(company), 'Y_test', -1)
    percent_change = find_single_index_by_index(make_combined_df(company), 'Y_t_YoY', -1)

    return html.Div(
        children=[
            # Summary
            html.Div(
                className="row summary",
                children=[
                    html.Div(
                        id = company + ' row',
                        className="row",
                        children=[
                            html.P(
                                className="four-col",
                                children=[company]
                            ),
                            html.P(
                                className="four-col",
                                children=['${:0,.0f}'.format(int(estimated_total_revenue))[:-4]],
                                style={"color":"#45df7e"}
                            ),
                            html.P(
                                className="four-col",
                                children=[f'+{percent_change}'],
                                style={"color":"#45df7e"}
                            )
                        ]
                    )
                ],
            ),
        ]
    )

def update_news():
    return html.Div(
        children=[
            html.Table(
                className="table-news",
                children=[
                    html.Tr(
                        children=[
                            html.Td(
                                children=[
                                    html.P(
                                        className="td-link",
                                        children=initial_df.iloc[i]["name"]
                                    ),
                                    html.A(
                                        className="td-link",
                                        children=f'{initial_df.iloc[i]["summary.website"]}',
                                        href=initial_df.iloc[i]["summary.website"],
                                        target="_blank",
                                    ),
                                    html.P(
                                        className="td-link",
                                        children='Full-Time Employees: {:0,.0f}'.format(initial_df.iloc[i]["summary.fullTimeEmployees"])
                                    ),
                                    html.P(
                                        className="td-link",
                                        children=f'{initial_df.iloc[i]["summary.longBusinessSummary"][:initial_df.iloc[i]["summary.longBusinessSummary"].find(" The company")]}'
                                    )
                                ]
                            )
                        ]
                    )
                    for i in range(len(initial_df))
                ],
            ),
        ]
    )

# Dash App Layout
app.layout = html.Div(
    className="row",
    children=[
        # Interval component for pridict value and last value
        dcc.Interval(id="i_bis", interval=1 * 3000, n_intervals=0),
        # Left Panel Div
        html.Div(
            className="three columns div-left-panel",
            children=[
                # Div for Left Panel App Info
                html.Div(
                    className="div-info",
                    children=[
                        html.Img(
                            className="logo", src=app.get_asset_url("logo.png")
                        ),
                        html.P(
                            """
                            This app queries MongDB Cients for major communication services's total revenue
                            and other indicators as well as SMI US Ad revenue, ORI, capture rate and stock.
                            You can also see the estimated total Revenue for 2019-Q4.
                            """
                        ),
                    ],
                ),
                # Recent Total Revenue and Stock Div
                html.Div(
                    className="div-currency-toggles",
                    children=[
                        html.P(className="four-col", children=""),
                        html.P(className="four-col", children="Est. Revenue 2019-Q4"),
                        html.P(className="four-col", children="% Change"),
                        html.Div(
                            id="convert-total-revenue",
                            className="div-bid-ask",
                            children=[
                                get_row(company) for company in companies
                            ],
                        ),
                    ],
                ),
                # Div for News Headlines
                html.Div(
                    className="div-news",
                    children=[html.Div(id="news", children=update_news())],
                )
            ],
        ),
        # Right Panel Div
        html.Div(
            className="nine columns div-right-panel",
            children=[
                html.Div([
                    dcc.Dropdown(
                        id="company-selection",
                        className="display-inlineblock bottom-dropdown",
                        options=[{"label": "Google", "value": "GOOGL"},
                                 {"label": "FaceBook", "value": "FB"},
                                 {"label": "Twitter", "value": "TWTR"},
                                 {"label": "SnapChat", "value": "SNAP"}
                                 ],
                        value="GOOGL",
                        clearable=False,
                        style={"border": "0px solid black", 'font-weight': '600', "font-size": "20px"},
                    )
                ]),
                html.Div(
                    className='padding-bottom',
                    children=[
                        dcc.RangeSlider(
                            id="range-slider",
                            min=find_single_index_by_index(make_combined_df('GOOGL'), 'Index', -1),
                            max=find_single_index_by_index(make_combined_df('GOOGL'), 'Index', 0),
                            value=[find_single_index_by_index(make_combined_df('GOOGL'), 'Index', -1), find_single_index_by_index(make_combined_df('GOOGL'), 'Index', 0)],
                            marks={str(index): {'label': year_period, 'style':{'fontSize': 9, 'writing-mode': 'vertical-lr', 'text-orientation': 'sideways'}}
                                    for index, year_period in zip(make_combined_df('GOOGL')['Index'], make_combined_df('GOOGL')['Year_Peoriode'])
                            },
                            step=None
                        )
                    ]
                ),
                # Charts Div
                html.Div(
                    id="graph-with-slider" ,
                    className="row charts",
                    children=[
                        html.Div(
                            id="graph_div",
                            className="twelve columns",
                            children=[
                                # Chart Top Bar 1
                                html.Div(
                                    className="chart-top-bar",
                                    children=[
                                        html.Span(
                                            id="top-bar-1",
                                            className="inline-block chart-title",
                                            children=[html.H6(children='Total Revenue')]
                                        ),
                                        html.Div(
                                            className="inline-block float-right",
                                            children=[html.P(children='All numbers in thousands')],
                                            style={'color': '#b2b2b2', 'font-size': '12px', 'margin': '20px 6px 0 0'}
                                        )
                                    ]
                                ),
                                # Graph div 1
                                html.Div(
                                    dcc.Graph(
                                        id="Date_Revenue-chart",
                                        className="chart-graph",
                                        config={"displayModeBar": False, "scrollZoom": True},
                                    )
                                )
                            ]
                        ),
                        html.Div(
                            className="twelve columns",
                            children=[
                                # Chart Top Bar 2
                                html.Div(
                                    className="chart-top-bar",
                                    children=[
                                        html.Span(
                                            id="top-bar-2",
                                            className="inline-block chart-title",
                                            children=[html.H6(children='Stock')]
                                        )
                                    ],
                                ),
                                # Graph div 
                                html.Div(
                                    dcc.Graph(
                                        id="Stock-chart",
                                        className="chart-graph",
                                        config={"displayModeBar": False, "scrollZoom": True},
                                    )
                                )
                            ]
                        )
                    ]
                )
            ]
        )
    ],
)

@app.callback(Output('range-slider', 'min'),[Input('company-selection', 'value')])
def update_slider_min(company):   
    return find_single_index_by_index(make_combined_df(company), 'Index', 0)

@app.callback(Output('range-slider', 'max'),[Input('company-selection', 'value')])
def update_slider_max(company):
    return find_single_index_by_index(make_combined_df(company), 'Index', -1)

@app.callback(Output('range-slider', 'marks'),[Input('company-selection', 'value')])
def update_slider_marks(company):
    return {str(index): {'label': year_period, 'style':{'color': '#9fd5ff79', 'fontSize': 9, 'writing-mode': 'vertical-lr', 'text-orientation': 'sideways'}}
                        for index, year_period in zip(make_combined_df(company)['Index'], make_combined_df(company)['Year_Peoriode'])
            }

@app.callback(Output('range-slider', 'value'),[Input('range-slider', 'min'), Input('range-slider', 'max')])
def update_slider_value(min_value, max_value): 
    return [min_value, max_value]

@app.callback(Output("Date_Revenue-chart", "figure"),[Input('company-selection', 'value'), Input('range-slider', 'value')])
def update_graph(company, value): 
    dff = find_all_cells_with_index(make_combined_df(company), 'Index', value)
    dff['Total_Revenue'] = [int(str(revenue)[:-4])  if len(revenue) > 5 else 0 for revenue in dff['Total_Revenue']]
    dff['Y_test'] = [int(str(revenue)[:-4]) for revenue in dff['Y_test']]
    colors = ['#176BEF', '#3B5998', '#1C9DEB', '#d3d030']
    c = colors[0] if company == 'GOOGL' else colors[1] if company == 'FB' else colors[2] if company == 'TWTR' else colors[3]

    fig = tools.make_subplots(
            rows=1,
            shared_xaxes=True,
            shared_yaxes=True,
            cols=1,
            print_grid=False,
            vertical_spacing=0.12,
    )

    # Add main trace (style) to figure
    fig.append_trace(go.Scatter(x = dff['Year_Peoriode'].values, y = dff['Total_Revenue'], mode = 'lines+markers', name = 'Actual Revenue', marker=dict(color=c, size=4, line=dict(color=c, width=5))), 1, 1)
    fig.append_trace(go.Scatter(x = dff['Year_Peoriode'].values, y = dff['Y_test'], mode = 'lines', name = 'Est. Revenue', line=dict(width=3)), 1, 1)

    fig["layout"]["margin"] = {"t": 20, "l": 10, "b": 80, "r": 50}
    fig["layout"]["autosize"] = True
    fig["layout"]["height"] = 400
    fig["layout"]["legend_orientation"] = "h"
    fig["layout"]["legend"]["x"] = 0
    fig["layout"]["legend"]["y"] = 1
    fig["layout"]["title"]['x']: 0.9
    fig["layout"]["title"]['y']: 0.5
    fig["layout"]["xaxis"]["rangeslider"]["visible"] = False
    fig["layout"]["xaxis"]["showgrid"] = False
    fig["layout"]["xaxis"]["title"] = "Year-Quarter"
    fig["layout"]["yaxis"]["showgrid"] = True
    fig["layout"]["yaxis"]["tickformat"] = "$,"
    fig["layout"]["yaxis"]["gridcolor"] = "#c4d1ed"
    fig["layout"]["yaxis"]["gridwidth"] = 1
    fig["layout"].update(paper_bgcolor="#E5ECF6", plot_bgcolor="#E5ECF6")

    return fig

@app.callback(Output("Stock-chart", "figure"),[Input('company-selection', 'value'), Input('range-slider', 'value')])
def update_graph(company, value): 
    dff = find_all_cells_with_index(make_combined_df(company), 'Index', value)
    colors = ['#176BEF', '#3B5998', '#1C9DEB', '#d3d030']
    c = colors[0] if company == 'GOOGL' else colors[1] if company == 'FB' else colors[2] if company == 'TWTR' else colors[3]

    fig = tools.make_subplots(
            rows=1,
            shared_xaxes=True,
            shared_yaxes=True,
            cols=1,
            print_grid=False,
            vertical_spacing=0.12,
    )

    # Add main trace (style) to figure
    fig.append_trace(go.Scatter(x = dff['Year_Peoriode'].values, y = ['${:0,.0f}'.format(stock) for stock in dff["Stock"]], mode = 'lines', name = 'Stock', line=dict(color=c, width=3)), 1, 1)
    fig.add_trace(go.Indicator(mode = "number+delta", 
            value = np.flipud(dff["Stock"])[0],
            delta = {"reference": np.flipud(dff['Stock'])[3:12].mean(), "valueformat": ".0f"},
            title = {"text": company + " Stock Prices"},
            domain = {'y': [0, 1], 'x': [.25, .75]}
    ))
    
    fig["layout"]["margin"] = {"t": 10, "l": 10, "b": 80, "r": 40}
    fig["layout"]["autosize"] = True
    fig["layout"]["height"] = 400
    fig["layout"]["xaxis"]["rangeslider"]["visible"] = False
    fig["layout"]["xaxis"]["showgrid"] = False
    fig["layout"]["xaxis"]["title"] = "Year-Quarter"
    fig["layout"]["yaxis"]["tickformat"] = "$,"
    fig["layout"]["yaxis"]["showgrid"] = False
    fig["layout"]["yaxis"]["gridcolor"] = "#c4d1ed"
    fig["layout"]["yaxis"]["gridwidth"] = 1
    fig["layout"].update(paper_bgcolor="#E5ECF6", plot_bgcolor="#E5ECF6")

    return fig

if __name__ == "__main__":
    app.run_server(debug=True)
