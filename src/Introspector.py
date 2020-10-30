from io import StringIO
import sys
import glob
import pandas as pd
import networkx as nx

import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

from .transaction import TransactionManager

import dash_core_components as dcc
import dash_table
import dash_html_components as html
from dash.dependencies import Input, Output
from dash import no_update, Dash
from io import BytesIO
import base64

from io import StringIO
import sys

import argparse
parser = argparse.ArgumentParser(description='Dash app for introspection.')
parser.add_argument('--test', help='Path to test to run.')
args = parser.parse_args()
t = args.test

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

def rep_site_as_df(site):
    rows = {}
    for x,V in site.memory.items():
        rows[x] = {'Value':V.value,
                   'Replicated':V.replicated,
                   'Available': V.available}
        
    for x,L in site.lock_table.items():
        rows[x].update({
            'RL': [T.name for T in L['RL']],
            'WL': L['WL'].name if L['WL'] is not None else None,
            'Waiting': [T.transaction.name for T in L['waiting']]
        })
        
    df = pd.DataFrame(rows)
    
    col_sort = sorted(df.columns,key=lambda x: int(x.strip('x')))
    
    return df[col_sort]


def fig_to_uri(in_fig, close_all=True, **save_args):
    # type: (plt.Figure) -> str
    """
    Save a figure as a URI
    """
    out_img = BytesIO()
    in_fig.savefig(out_img, format='png', **save_args)
    if close_all:
        in_fig.clf()
        plt.close('all')
    out_img.seek(0)  # rewind file
    encoded = base64.b64encode(out_img.read()).decode("ascii").replace("\n", "")
    return "data:image/png;base64,{}".format(encoded)

def rep_transaction(T):

    start_time = TM.transactions[T].start_time
    READ_ONLY = TM.transactions[T].READ_ONLY
    
    if READ_ONLY:
        return dcc.Markdown(f"#### Transaction {T}:\n* Start Time = {start_time}\n* Read-only = {READ_ONLY}")
    
    else:
        read_locks = {k.site_number: v for k,v in TM.transactions[T].read_locks.items() if len(v)>0}
        write_locks = {k.site_number: v for k,v in TM.transactions[T].write_locks.items() if len(v)>0}
        locks_needed = {k.site_number: v for k,v in TM.transactions[T].locks_needed.items() if len(v)>0}
        m = dcc.Markdown(f"#### Transaction {T}:\n* Start Time = {start_time}\n* Read-only = {READ_ONLY}" +\
                         f"\n* Read locks = {read_locks}\n* Write locks = {write_locks}"+\
                         f"\n* Locks needed = {locks_needed}",
                         style={'padding-bottom':25})
        
        after_image = pd.DataFrame({f"Site {k.site_number}": v for k,v in TM.transactions[T].after_image.items()}).T
        after_image.index.name = 'Site'
        after_image = after_image.reset_index()
        cols = [{"name": i, "id": i} for i in after_image.columns]
        data = after_image.to_dict('records')
        
        children = [
            m,
            html.H6('After images:'),
            dash_table.DataTable(id='transaction_after_image',columns=cols,data=data)
        ]
        return(children)
    
# Build App
app = Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div([
    html.H1(f"RepCRec State"),
    html.H6(f"Test file:{t}"),
    dcc.Markdown(id='test-script',style={'padding-bottom':25,"white-space": "pre"}),
    html.Button('Tick', id='ticker', n_clicks=0),
    html.H4("Time = 0",id='clock',style={'padding-bottom':25}),
    html.H4("Choose a Site:",id='site-header',style={'padding-top':25}),
    dcc.Slider(id='site',min=1,max=10,value=1,step=None,marks={v:str(v) for v in range(1,11)}),
    html.Div(id='slider-output',style={'padding-top':25}),
    html.Div([html.Img(id = 'cur_plot', src = '')], id='plot_div'),
    html.Div([dash_table.DataTable(id='detailed_site_table')],style={'padding-bottom':25}),
    html.H4("Choose a transaction:",id='transaction-header',style={'padding-top':25}),
    dcc.Dropdown(id='transaction-dropdown',placeholder='Select transaction'),
    html.Div(id='transaction-summary'),
    html.H4("Transaction manager:",id='TM-header',style={'padding-top':25}),
    html.Div(id='tm-summary'),
    html.H4("Standard output:",id='output-header',style={'padding-top':25}),
    html.Div(id='stdout')
])

# Test script
@app.callback(
    Output('test-script', 'children'),
    [Input("ticker", "n_clicks")]
)
def read_script(n_clicks):
    if n_clicks == 0:
        with open(t, 'r') as f:
            t_lines = f.read()
        return t_lines
    else:
        with open(t, 'r') as f:
            t_lines = f.read()
        t_lines = t_lines.split('\n')
        for ix in range(len(t_lines)):
            if ix == n_clicks-1:
                t_lines[ix] = f"**{t_lines[ix]}**"
        t_lines = '\n'.join(t_lines)
        return t_lines

# Callback to advance clock
@app.callback(
    Output('clock', 'children'),
    [Input("ticker", "n_clicks")]
)
def update_clock(n_clicks):
    if n_clicks > 0:
        TM.tick()
    return f"Time = {n_clicks}"
    
# Callback to update site
@app.callback(
    Output('slider-output', 'children'),
    [Input('site', 'value')]
)
def update_site_label(v):
    site_uptime = TM.sites[v].uptime
    alive = TM.sites[v].alive
    return dcc.Markdown(f"#### Site {v}:\n* Uptime = {site_uptime}\n* Alive = {alive}")

# Callback to update site tables
@app.callback(
    [Output('detailed_site_table', 'columns'),
     Output('detailed_site_table', 'data')],
    [Input("site", "value"),
     Input('clock','children')]
)
def update_table(site,_):
    df = rep_site_as_df(TM.sites[site])
    df.index.name = 'Attribute'
    df = df.reset_index()
    cols = [{"name": i, "id": i} for i in df.columns]
    data = df.to_dict('records')
    return cols,data

# Callback to update site figure
@app.callback(
    Output('cur_plot', 'src'),
    [Input("site", "value"),
     Input('clock','children')]
)
def update_fig(site,_):
    fig,ax=plt.subplots(figsize=(9,6))
    nx.draw_networkx(TM.sites[site].waits_for, 
                     labels={T:T.name for T in TM.sites[site].waits_for.nodes},
                     ax=ax)
    out_url = fig_to_uri(fig)
    return out_url

# Callback to update transaction options
@app.callback(
    [Output('transaction-dropdown', 'options'),
     Output('transaction-dropdown', 'value')],
    [Input('clock', 'children')]
)
def update_T_options(_):
    options = [{'label':t, 'value':t} for t in TM.transactions.keys()]
    if len(options)>0:
        value = options[0]['value']
    else:
        value = None
    return options,value

# Callback to update the transaction div
@app.callback(
    Output('transaction-summary', 'children'),
    [Input('transaction-dropdown', 'value')]
)
def update_T_div(T):
    if len(TM.transactions) == 0:
        return None
    else:
        return(rep_transaction(T))
    
# Callback to update the TM div
@app.callback(
    Output('tm-summary', 'children'),
    [Input('clock', 'children')]
)
def update_tm_div(T):
    request_queue = [t.transaction.name for t in TM.request_queue]
    composed_graph = TM.compose_waits_for_graphs()
    fig,ax=plt.subplots(figsize=(9,6))
    nx.draw_networkx(composed_graph, 
                     labels={T:T.name for T in composed_graph.nodes},
                     ax=ax)
    out_url = fig_to_uri(fig)
    
    r = [
        html.H6(f'Transactions in queue: {request_queue}'),
        html.H6('Composed waits-for graphs:'),
        html.Div([html.Img(id = 'composed-g', src = out_url)], id='composed-div')
    ]
    return r
    
# Callback to update standard output
@app.callback(
    Output('stdout', 'children'),
    [Input('clock', 'children')]
)
def update_stdout(T):
    return(dcc.Markdown(buff.getvalue(),style={"white-space": "pre"}))


if __name__ == '__main__':

    TM = TransactionManager(test=t)

    buff = StringIO()
    # Replace default stdout (terminal) with our stream
    sys.stdout = buff

    app.run_server(debug=True)