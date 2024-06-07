import threading
import time
from flask import Flask
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import plotly.graph_objs as go
from kafka_consumer import KafkaDataConsumer

# Kafka Configuration
KAFKA_TOPIC = 'output_topic'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Initialize Kafka Consumer
kafka_consumer = KafkaDataConsumer(KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)

# Flask server
server = Flask(__name__)

# Dash app
app = dash.Dash(__name__, server=server)

app.layout = html.Div([
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # Update every second
        n_intervals=0
    )
])

@app.callback(
    Output('live-update-graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_graph_live(n):
    data = kafka_consumer.get_data()
    x = [d['timestamp'] for d in data]
    y = [d['value'] for d in data]

    fig = go.Figure(
        data=[go.Scatter(x=x, y=y, mode='lines+markers')],
        layout=go.Layout(
            title='Live Data from Kafka',
            xaxis=dict(range=[min(x), max(x)]),
            yaxis=dict(range=[min(y), max(y)])
        )
    )
    return fig

def consume_kafka_data():
    kafka_consumer.consume_data()

if __name__ == '__main__':
    # Start Kafka consumer in a separate thread
    threading.Thread(target=consume_kafka_data, daemon=True).start()
    app.run_server(debug=True)
