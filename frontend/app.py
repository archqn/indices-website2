import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from sqlalchemy import create_engine

# Connect to MySQL database
engine = create_engine('mysql+mysqlconnector://indices_website_user:Indices-MySQL@localhost:3306/indices_dash_db')

# Create a Dash app
app = dash.Dash(__name__)
server = app.server 

# App layout
app.layout = html.Div([
    html.H1("Solar Wind Data Visualization"),
    dcc.DatePickerRange(
        id='date-picker-range',
        start_date='2024-01-01',
        end_date='2024-01-02'
    ),
    # dcc.Graph(id='ae-prediction-graph'),
    dcc.Graph(id='solar-wind-graph')
])

# Define callback to update the graph based on selected date range
@app.callback(
    Output('solar-wind-graph', 'figure'),
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date')]
)
def update_graph(start_date, end_date):
    # ---------------------------- MYSQL QUERY ----------------------------
    query = f"SELECT * FROM solar_wind_new WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'"
    df = pd.read_sql(query, engine)
    
    # Create the figure using Plotly
    fig = make_subplots(
        rows=4, cols=1,  # 4 rows for the 4 parameters
        shared_xaxes=True,  # Share x-axis (timestamp) across all plots
        vertical_spacing=0.025  # Space between plots
    )

    # Add a trace for BZ_GSM in the first row
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['BZ_GSM'], mode='lines', name='BZ_GSM'), row=1, col=1)

    # Add a trace for flow_speed in the second row
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['flow_speed'], mode='lines', name='Flow Speed'), row=2, col=1)

    # Add a trace for proton_density in the third row
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['proton_density'], mode='lines', name='Proton Density'), row=3, col=1)

    # Add a trace for T (Temperature) in the fourth row
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['T'], mode='lines', name='Temperature'), row=4, col=1)

    fig.update_xaxes(title_text='Timestamp', row=4, col=1)
    fig.update_yaxes(title_text='BZ_GSM', row=1, col=1)
    fig.update_yaxes(title_text='Flow Speed', row=2, col=1)
    fig.update_yaxes(title_text='Proton Density', row=3, col=1)
    fig.update_yaxes(title_text='Temperature', row=4, col=1)

    # Update layout to include title and labels
    fig.update_layout(
        height=800,  # Adjust height to fit all subplots
        title_text="Solar Wind Data",
        hovermode='x unified',  # Shows values for all plots at the same x (timestamp) on hover
    )

    # Return the figure
    return fig


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
