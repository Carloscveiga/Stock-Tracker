import dash
from dash import Dash, html, dcc, callback, Output, Input, State
import dash_bootstrap_components as dbc
from equity_list import equities
from get_stock_data import get_stock_data
from handle_stock_data import handle_stock_data_prices
from calc_sma_data import calc_sma_data
from calc_ma_data_signals import calc_sma_signal_data
from calc_lin_and_poly_data_multi import calc_lin_and_poly_data_multi, calc_lin_and_poly_signal_data_multi
import dash_ag_grid as dag
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl



app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
    
stock_data = get_stock_data(equities)
price_data = stock_data.tail(1200)
closed_price_data = handle_stock_data_prices(stock_data, equities)
sma_data = calc_sma_data(closed_price_data, 0, len(closed_price_data))
sma_signal_data = calc_sma_signal_data(sma_data)
lin_and_poly_data_multi = calc_lin_and_poly_data_multi(closed_price_data, 1318, len(closed_price_data))
lin_and_poly_signal_data = calc_lin_and_poly_signal_data_multi(lin_and_poly_data_multi, 0.83)

def last_close(ticker):
    return stock_data[ticker]["Close"].iloc[-1]

data = {
    "ticker": [ticker for ticker in equities],
    "company": [name for name in equities.values()],
    "price": [last_close(ticker) for ticker in equities],
    "last MA position": [sma_signal_data[f"{ticker}_Signal"][-1] for ticker in equities],
    "last linear position": [lin_and_poly_signal_data[f"{ticker}_Lin_Signal"][-1] for ticker in equities],
    "last polynomial position": [lin_and_poly_signal_data[f"{ticker}_Poly_Signal"][-1] for ticker in equities],
}
df = pd.DataFrame(data)
df['last MA position'] = df['last MA position'].map({1: 'BUY', -1: 'SELL', 0: 'HOLD'}).fillna('Unknown')
df['last linear position'] = df['last linear position'].map({1: 'BUY', -1: 'SELL', 0: 'HOLD'}).fillna('Unknown')
df['last polynomial position'] = df['last polynomial position'].map({1: 'BUY', -1: 'SELL', 0: 'HOLD'}).fillna('Unknown')

columnDefs = [
    {
        "headerName": "Stock Ticker",
        "field": "ticker",
        "filter": True,
    },
    {
        "headerName": "Company",
        "field": "company",
        "filter": True,
    },
    {
        "headerName": "Last Close Price",
        "field": "price",
        "type": "rightAligned",
        "valueFormatter": {"function": "d3.format('$,.2f')(params.value)"},
        "cellRenderer": "agAnimateShowChangeCellRenderer",
    },
        {
        "headerName": "Last MA Signal",
        "field": "last MA position",
        "type": "rightAligned",
        "cellEditorParams": {"values": ['BUY', 'SELL', 'HOLD']},
        "cellRenderer": "agAnimateShowChangeCellRenderer",
    },
    {
        "headerName": "Last Linear Signal",
        "field": "last linear position",
        "type": "rightAligned",
        "cellEditorParams": {"values": ['BUY', 'SELL', 'HOLD']},
        "cellRenderer": "agAnimateShowChangeCellRenderer",
    },
    {
        "headerName": "Last Polynomial Signal",
        "field": "last polynomial position",
        "type": "rightAligned",
        "cellEditorParams": {"values": ['BUY', 'SELL', 'HOLD']},
        "cellRenderer": "agAnimateShowChangeCellRenderer",
    },
]

cellStyle = {
    "styleConditions": [
        {
            "condition": "params.value == 'BUY'",
            "style": {"backgroundColor": "#196A4E", "color": "white", "text-align": "center"},
        },
        {
            "condition": "params.value == 'SELL'",
            "style": {"backgroundColor": "#800000", "color": "white","text-align": "center"},
        },
        {
            "condition": "params.value == 'HOLD'",
            "style": {"text-align": "center"},
        },
    ]
}
defaultColDef = {
    "filter": "agNumberColumnFilter",
    "resizable": True,
    "sortable": True,
    "editable": False,
    "floatingFilter": True,
    "minWidth": 125,
    "cellStyle": cellStyle,
}

grid = dag.AgGrid(
    id="portfolio-grid",
    className="ag-theme-alpine-dark",
    columnDefs=columnDefs,
    rowData=df.to_dict("records"),
    columnSize="sizeToFit",
    defaultColDef=defaultColDef,
    dashGridOptions={"undoRedoCellEditing": True, "rowSelection": "single"},
    
)

candlestick = dbc.Card(dcc.Graph(id="candlestick"), body=True)
mas = dbc.Card(dcc.Graph(id="mas"), body=True)
lin = dbc.Card(dcc.Graph(id="lin"), body=True)
poly = dbc.Card(dcc.Graph(id="poly"), body=True)

header = html.Div(
    
    children=[
        html.Div(children='DashBoard v1.0',
                 style={'textAlign': 'center', 'fontSize': '44px','margin-bottom': '40px','margin-top': '40px'}
                 ),
        html.Div(children='DashBoard v1.0: A web-based application for your daily trade.',
                 style={'textAlign': 'center', 'fontSize': '18px','margin-bottom': '38px'}
                 )
    ],
    
)

app.layout = dbc.Container(
    [
        header,
        dbc.Row([dbc.Col(candlestick),dbc.Col(mas)]),
        dbc.Row([dbc.Col(lin),dbc.Col(poly)]),
        dbc.Row(dbc.Col(grid, className="py-4")),
    ],
)


@app.callback(
    Output("candlestick", "figure"),
    Input("portfolio-grid", "selectedRows"),
)
def update_candlestick(selected_row):
    if selected_row is None:
        ticker = "AAPL"
        company = "Apple Inc."
    else:
        ticker = selected_row[0]["ticker"]
        company = selected_row[0]["company"]

    dff_ticker_hist = stock_data[ticker].reset_index()
    dff_ticker_hist["Date"] = pd.to_datetime(dff_ticker_hist["Date"])

    fig = go.Figure(
        go.Candlestick(
            x=dff_ticker_hist["Date"],
            open=dff_ticker_hist["Open"],
            high=dff_ticker_hist["High"],
            low=dff_ticker_hist["Low"],
            close=dff_ticker_hist["Close"],
        )
    )
    fig.update_layout(
        title_text=f"{ticker} {company} Daily Price", template="plotly_dark"
    )
    return fig

@dash.callback(
    Output("mas", "figure"),
    Input("portfolio-grid", "selectedRows"),
)
def update_mas(selected_row):
    if selected_row is None:
        ticker = "AAPL"
        company = "Apple Inc."
    else:
        ticker = selected_row[0]["ticker"]
        company = selected_row[0]["company"]

    fig = go.Figure()
    dates = sma_data["Date"]

    # Add the 5-minute moving average trace
    ma5 = sma_data[f"{ticker}_SMA_5"]  # Access the 5-minute moving average data for the selected equity
    fig.add_trace(go.Scatter(
        x=dates,
        y=ma5,
        mode='lines',
        name="5-Min MA",
        showlegend=False,
    ))

    # Add the 15-minute moving average trace
    ma15 = sma_data[f"{ticker}_SMA_15"]  # Access the 15-minute moving average data for the selected equity
    fig.add_trace(go.Scatter(
        x=dates,
        y=ma15,
        mode='lines',
        name="15-Min MA",
        showlegend=False,
    ))

    # Add the 20-minute moving average trace
    ma20 = sma_data[f"{ticker}_SMA_20"]  # Access the 20-minute moving average data for the selected equity
    fig.add_trace(go.Scatter(
        x=dates,
        y=ma20,
        mode='lines',
        name="20-Min MA",
        showlegend=False,
    ))

    # Add the 200-minute moving average trace
    ma200 = sma_data[f"{ticker}_SMA_200"] # Access the 200-minute moving average data for the selected equity
    fig.add_trace(go.Scatter(
        x=dates,
        y=ma200,
        mode='lines',
        name="200-Min MA",
        showlegend=False,
    ))
    
    # Filter for buy and sell signals
    buy_signals = sma_signal_data.filter(sma_signal_data[f"{ticker}_Signal"] == 1).to_dicts()
    sell_signals = sma_signal_data.filter(sma_signal_data[f"{ticker}_Signal"] == -1).to_dicts()

    # Adding markers for buy signals
    for row in buy_signals:
        date = row['Date']
        close_price = sma_data.filter(sma_data["Date"] == date)[f"{ticker}_Close"][0]
        fig.add_trace(go.Scatter(
            x=[date],
            y=[close_price],
            mode='markers',
            marker=dict(symbol='circle', color='green', size=10),
            showlegend=False
        ))
    # Adding markers for sell signals
    for row in sell_signals:
        date = row['Date']
        close_price = sma_data.filter(sma_data["Date"] == date)[f"{ticker}_Close"][0]
        fig.add_trace(go.Scatter(
            x=[date],
            y=[close_price],
            mode='markers',
            marker=dict(symbol='circle', color='red', size=10),
            showlegend=False
        ))

    fig.update_layout(
        title_text=f"{ticker} {company} Moving Averages",template="plotly_dark")
    return fig

@app.callback(
    Output("lin", "figure"),
    Input("portfolio-grid", "selectedRows"),
)
def update_multi_lin_chart(selected_row):
    if selected_row is None:
        ticker = "AAPL"
        company = "Apple Inc."
    else:
        ticker = selected_row[0]["ticker"]
        company = selected_row[0]["company"]
    
    dates = lin_and_poly_signal_data["Date"]
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=dates,
        y=lin_and_poly_signal_data[f"{ticker}_Close"],
        mode="lines",
        name=f"Closing Prices - {company}",
        showlegend=False,
    ))

    fig.add_trace(go.Scatter(
        x=dates,
        y=lin_and_poly_signal_data[f"{ticker}_Lin_Trendline"],
        mode="lines",
        name=f"Trendline - {company}",
        showlegend=False,
    ))
    
    buy_signals = lin_and_poly_signal_data.filter(lin_and_poly_signal_data[f"{ticker}_Lin_Signal"] == 1).to_dicts()
    sell_signals = lin_and_poly_signal_data.filter(lin_and_poly_signal_data[f"{ticker}_Lin_Signal"] == -1).to_dicts()

    # Adding markers for buy signals
    for row in buy_signals:
        date = row['Date']
        close_price = lin_and_poly_signal_data.filter(lin_and_poly_signal_data["Date"] == date)[f"{ticker}_Close"][0]
        fig.add_trace(go.Scatter(
            x=[date],
            y=[close_price],
            mode='markers',
            marker=dict(symbol='circle', color='green', size=10),
            showlegend=False
        ))
    # Adding markers for sell signals
    for row in sell_signals:
        date = row['Date']
        close_price = lin_and_poly_signal_data.filter(lin_and_poly_signal_data["Date"] == date)[f"{ticker}_Close"][0]
        fig.add_trace(go.Scatter(
            x=[date],
            y=[close_price],
            mode='markers',
            marker=dict(symbol='circle', color='red', size=10),
            showlegend=False
        ))
    
    fig.update_layout(
        title_text=f"{ticker} {company} Linear Trendline", template="plotly_dark"
    )
    return fig

@app.callback(
    Output("poly", "figure"),
    Input("portfolio-grid", "selectedRows"),
)
def update_multi_poly_chart(selected_row):
    if selected_row is None:
        ticker = "AAPL"
        company = "Apple Inc."
    else:
        ticker = selected_row[0]["ticker"]
        company = selected_row[0]["company"]
    
    dates = lin_and_poly_signal_data["Date"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates,
        y=lin_and_poly_signal_data[f"{ticker}_Close"],
        mode="lines",
        name=f"Closing Prices - {company}",
        showlegend=False,
    ))

    fig.add_trace(go.Scatter(
        x=dates,
        y=lin_and_poly_signal_data[f"{ticker}_Poly Trendline"],
        mode="lines",
        name=f"Trendline - {company}",
        showlegend=False,
    ))
    
    buy_signals = lin_and_poly_signal_data.filter(lin_and_poly_signal_data[f"{ticker}_Poly_Signal"] == 1).to_dicts()
    sell_signals = lin_and_poly_signal_data.filter(lin_and_poly_signal_data[f"{ticker}_Poly_Signal"] == -1).to_dicts()

    # Adding markers for buy signals
    for row in buy_signals:
        date = row['Date']
        close_price = lin_and_poly_signal_data.filter(lin_and_poly_signal_data["Date"] == date)[f"{ticker}_Close"][0]
        fig.add_trace(go.Scatter(
            x=[date],
            y=[close_price],
            mode='markers',
            marker=dict(symbol='circle', color='green', size=10),
            showlegend=False
        ))
    # Adding markers for sell signals
    for row in sell_signals:
        date = row['Date']
        close_price = lin_and_poly_signal_data.filter(lin_and_poly_signal_data["Date"] == date)[f"{ticker}_Close"][0]
        fig.add_trace(go.Scatter(
            x=[date],
            y=[close_price],
            mode='markers',
            marker=dict(symbol='circle', color='red', size=10),
            showlegend=False
        ))    
    
    fig.update_layout(
        title_text=f"{ticker} {company} Polynomial Trendline", template="plotly_dark"
    )
    return fig

if __name__ == "__main__":
    app.run_server(debug=False, use_reloader=False)
