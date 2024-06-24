from dash import Dash, dcc, html, Input, Output
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CovidTrendAnalysis").getOrCreate()

# Load data
file_path = "dbfs:/FileStore/tables/synthetic_covid_data_realistic.csv"
spark_df = spark.read.csv(file_path, header=True, inferSchema=True)
covid_data = spark_df.toPandas()

# Convert Date column to datetime
covid_data['Date'] = pd.to_datetime(covid_data['Date'])

# Initialize Dash app
app = Dash(__name__)
server = app.server  # Needed for deploying on Render

# Define the layout of the app
app.layout = html.Div([
    html.H1("COVID-19 Trend Analysis"),
    dcc.Dropdown(
        id='metric-dropdown',
        options=[
            {'label': 'Cases', 'value': 'Cases'},
            {'label': 'Deaths', 'value': 'Deaths'}
        ],
        value='Cases',
        clearable=False
    ),
    dcc.Graph(id='covid-graph')
])

# Define callback to update graph based on dropdown selection
@app.callback(
    Output('covid-graph', 'figure'),
    [Input('metric-dropdown', 'value')]
)
def update_graph(selected_metric):
    fig = px.line(covid_data, x='Date', y=selected_metric, color='Country',
                  title=f'COVID-19 {selected_metric} Over Time by Country')
    fig.update_layout(xaxis_title='Date', yaxis_title=f'Total {selected_metric}')
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
