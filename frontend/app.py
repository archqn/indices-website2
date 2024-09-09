import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine

# Connect to MySQL database
engine = create_engine('mysql+mysqlconnector://indices_website_user:Indices-MySQL@localhost:3306/indices_dash_db')

# Create a Dash app
app = dash.Dash(__name__)

# App layout
app.layout = html.Div([
    html.H1("Solar Wind Data Visualization"),
    dcc.DatePickerRange(
        id='date-picker-range',
        start_date='2024-01-01',
        end_date='2024-01-02'
    ),
    dcc.Graph(id='solar-wind-graph')
])

# Define callback to update the graph based on selected date range
@app.callback(
    Output('solar-wind-graph', 'figure'),
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date')]
)
def update_graph(start_date, end_date):
    # Query data from MySQL based on the date range
    query = f"SELECT * FROM solar_wind WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date}'"
    df = pd.read_sql(query, engine)
    
    # Create the figure using Plotly
    fig = px.line(df, x='timestamp', y='BZ_GSM', title='Solar Wind Data')
    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
