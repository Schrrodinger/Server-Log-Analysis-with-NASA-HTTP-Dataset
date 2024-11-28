import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, to_timestamp, hour
import os
import sys

def plot_page():

    # Initialize Spark Session
    @st.cache_resource
    def init_spark():
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        spark = SparkSession.builder \
            .appName("NASA_Logs_Analysis") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        return spark

    # Function to process the log file
    @st.cache_data
    def load_and_process_logs():
        spark = init_spark()

        # Define the regex patterns
        host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
        ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
        method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        status_pattern = r'\s(\d{3})\s'
        content_size_pattern = r'\s(\d+)$'

        # Read the log file
        base_df = spark.read.text("access_log_JulAug_95.txt")

        # Extract fields using regex
        logs_df = base_df.select(
            regexp_extract('value', host_pattern, 1).alias('host'),
            regexp_extract('value', ts_pattern, 1).alias('timestamp'),
            regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
            regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
            regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
            regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
            regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size')
        )

        # Convert to Pandas for Streamlit
        pandas_df = logs_df.toPandas()

        # Convert timestamp to datetime
        pandas_df['timestamp'] = pd.to_datetime(pandas_df['timestamp'],
                                              format='%d/%b/%Y:%H:%M:%S %z')

        # Fill NaN values in content_size with 0
        pandas_df['content_size'] = pandas_df['content_size'].fillna(0)

        return pandas_df

    # Load data with a loading message
    with st.spinner('Loading and processing log data...'):
        try:
            df = load_and_process_logs()
            st.success('Data loaded successfully!')
        except Exception as e:
            st.error(f'Error loading data: {str(e)}')
            st.stop()

    # Title and description
    st.title("NASA Web Logs Analysis Dashboard Page 1")
    st.write("Analysis of NASA web server logs from July-August 1995")

    # Create selection box for different plots
    plot_option = st.selectbox(
        "Choose a visualization:",
        [
            "Status Code Distribution",
            "Top Endpoints by Requests",
            "Request Methods Distribution",
            "Content Size Distribution",
            "Host Request Frequency",
            "Hourly Traffic Pattern",
            "Daily Traffic Pattern"
        ]
    )

    def plot_status_distribution(data):
        # Create a function to categorize status codes
        def get_status_category(status):
            if 200 <= status < 300:
                return '2xx (Success)'
            elif 300 <= status < 400:
                return '3xx (Redirection)'
            elif 400 <= status < 500:
                return '4xx (Client Error)'
            elif 500 <= status < 600:
                return '5xx (Server Error)'
            else:
                return 'Other'

        # Add category column
        data['status_category'] = data['status'].apply(get_status_category)

        # Count by category
        status_counts = data['status_category'].value_counts().reset_index()
        status_counts.columns = ['Status Category', 'Count']

        # Define colors for each category
        colors = {
            '2xx (Success)': '#2ecc71',      # Green
            '3xx (Redirection)': '#3498db',  # Blue
            '4xx (Client Error)': '#e74c3c', # Red
            '5xx (Server Error)': '#f1c40f'  # Yellow
        }

        # Create the bar plot
        fig = px.bar(
            status_counts,
            x='Status Category',
            y='Count',
            title='HTTP Status Code Distribution by Category',
            color='Status Category',
            color_discrete_map=colors,
            text='Count'  # Display count on bars
        )

        # Update layout
        fig.update_layout(
            xaxis_title="Status Code Category",
            yaxis_title="Number of Requests",
            showlegend=False,
            plot_bgcolor='white',
            height=500,
            # Format the count labels on bars
            uniformtext_minsize=8,
            uniformtext_mode='hide'
        )

        # Update bar text position and format
        fig.update_traces(
        textposition='outside',
        texttemplate='%{text:,.0f}',  # Format numbers with commas
        textfont=dict(
            color='black',  # Set text color to black
            family='Arial',  # Choose a font family
            size=14,         # Adjust font size
        ),
        hovertemplate="<br>".join([
            "Category: %{x}",
            "Count: %{y:,.0f}",
            "<extra></extra>"
        ])
        )

        return fig

    def plot_top_endpoints(data):
        # Get top 20 endpoints
        endpoint_counts = data['endpoint'].value_counts().head(20).reset_index()
        endpoint_counts.columns = ['Endpoint', 'Count']

        # Create the horizontal bar plot
        fig = px.bar(
            endpoint_counts,
            x='Count',
            y='Endpoint',
            title='Top 20 Most Requested Endpoints',
            orientation='h',
            color='Count',
            color_continuous_scale='Viridis'
        )

        # Update layout
        fig.update_layout(
            xaxis_title="Number of Requests",
            yaxis_title="Endpoint",
            yaxis={'categoryorder':'total ascending'},
            height=800,  # Increased height for better readability
            hoverlabel=dict(
                bgcolor="white",
                font_size=14,
                font_family="Courier New"
            ),
            # Format number with commas in axis
            xaxis=dict(
                tickformat=",",
                separatethousands=True
            )
        )

        # Update hover template to show only endpoint and formatted count
        fig.update_traces(
            hovertemplate="<b>%{y}<br>" +
                          "<b>Requests:</b> %{x:,.0f}<br>" +
                          "<extra></extra>",  # This removes the secondary box

            textposition='outside',
            textfont=dict(
                color='black',  # Set text color to black
                family='Arial',  # Choose a font family
                size=14,         # Adjust font size
                )
        )
        fig.update_layout(
        hoverlabel=dict(
            font=dict(
                color='black',  # Set hover text color to black
                size=14,
                family='Arial'
            ),
            bgcolor='white'  # Optional: Set the hover background to white for clarity
        ))
        return fig
    # Function to create method distribution plot
    def plot_method_distribution(data):
        method_counts = data['method'].value_counts().reset_index()
        method_counts.columns = ['Method', 'Count']

        # Calculate percentages
        total_requests = method_counts['Count'].sum()
        method_counts['Percentage'] = (method_counts['Count'] / total_requests * 100).round(2)

        fig = px.pie(
            method_counts,
            values='Count',
            names='Method',
            title='Distribution of HTTP Methods',
            hole=0.4
        )

        # Update hover and text information
        fig.update_traces(
            textposition='outside',
            texttemplate='%{label}<br>%{percent:.1%}',  # Show method name and percentage
            textfont=dict(
                color='black',
                family='Arial',
                size=14
            ),
            hovertemplate="<b>%{label}</b><br>" +
                          "Requests: %{value:,.0f}<br>" +
                          "Percentage: %{percent:.1%}<br>" +
                          "<extra></extra>"  # This removes the secondary box
        )

        # Update layout
        fig.update_layout(
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.2,
                xanchor="right",
                x=1
            ),
            hoverlabel=dict(
                bgcolor="white",
                font=dict(
                    family="Arial",
                    size=14,
                    color="black"
                )
            )
        )

        return fig


    def plot_host_frequency(data):
        # Get top 20 hosts and calculate percentages
        host_counts = data['host'].value_counts().head(20).reset_index()
        host_counts.columns = ['Host', 'Count']
        total_requests = host_counts['Count'].sum()
        host_counts['Percentage'] = (host_counts['Count'] / total_requests * 100).round(2)

        # Create horizontal bar plot
        fig = px.bar(
            host_counts,
            x='Count',
            y='Host',
            title='Top 20 Hosts by Number of Requests',
            orientation='h',
            color='Count',
            color_continuous_scale='Viridis'
        )

        # Update layout
        fig.update_layout(
            xaxis_title="Number of Requests",
            yaxis_title="Host",
            height=800,  # Increased height for better readability
            showlegend=False,
            hoverlabel=dict(
                bgcolor="white",
                font=dict(
                    family="Arial",
                    size=14,
                    color="black"
                )
            ),
            # Format number with commas in x-axis
            xaxis=dict(
                tickformat=",",
                separatethousands=True
            )
        )

        # Update hover template for bar chart
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>" +
                          "Requests: %{x:,.0f}<br>" +
                          "Percentage: %{customdata:.1f}%<br>" +
                          "<extra></extra>",
            customdata=host_counts['Percentage'],  # Add percentage data for hover
            textposition='outside',
            textfont=dict(
                family="Arial",
                size=14,
                color="black"
            )
        )

        return fig
    
    def plot_content_size_distribution(data):
    # Convert content size to KB for better readability
        data['content_size_kb'] = data['content_size'] / 1024
    
    # Create histogram with custom bins
        fig = px.histogram(
            data,
            x='content_size_kb',
            nbins=50,
            title='Distribution of Response Content Sizes',
            color_discrete_sequence=['#3498db']  # Use a pleasant blue color
    )
    
    # Calculate statistics for annotations
        mean_size = data['content_size_kb'].mean()
        median_size = data['content_size_kb'].median()
    
    # Update layout
        fig.update_layout(
            xaxis_title="Content Size (KB)",
            yaxis_title="Number of Requests",
            showlegend=False,
            plot_bgcolor='white',
            height=500,
            annotations=[
                dict(
                    x=mean_size,
                    y=fig.data[0].y.max(),
                    xref="x",
                    yref="y",
                    text=f"Mean: {mean_size:.2f} KB",
                    showarrow=True,
                    arrowhead=2,
                    ax=0,
                    ay=-40
            ),
                dict(
                    x=median_size,
                    y=fig.data[0].y.max() * 0.85,
                    xref="x",
                    yref="y",
                    text=f"Median: {median_size:.2f} KB",
                    showarrow=True,
                    arrowhead=2,
                    ax=0,
                    ay=-40
            )
        ],
        # Format axis labels
            xaxis=dict(
                tickformat=",.1f",  # Add comma separator and show one decimal
                rangemode="tozero"
        ),
            yaxis=dict(
                tickformat=",",  # Add comma separator for thousands
                rangemode="tozero"
        ),
            hoverlabel=dict(
                bgcolor="white",
                font_size=14,
                font_family="Arial"
        )
    )
    
    # Update hover template
        fig.update_traces(
            hovertemplate="<b>Content Size:</b> %{x:.1f} KB<br>" +
                      "<b>Count:</b> %{y:,}<br>" +
                      "<extra></extra>"  # This removes the secondary box
    )
    
        return fig

    # Function to create hourly traffic pattern plot
    def plot_hourly_traffic(data):
        data['hour'] = data['timestamp'].dt.hour
        hourly_traffic = data['hour'].value_counts().sort_index().reset_index()
        hourly_traffic.columns = ['Hour', 'Count']

        fig = px.line(
            hourly_traffic,
            x='Hour',
            y='Count',
            title='Hourly Traffic Pattern',
            markers=True
        )

        fig.update_layout(
            xaxis_title="Hour of Day",
            yaxis_title="Number of Requests",
            xaxis=dict(tickmode='linear', tick0=0, dtick=1)
        )
        return fig

    def plot_daily_traffic(data):
        # Extract day of week and create mapping for sorting
        day_mapping = {
            'Monday': 0,
            'Tuesday': 1,
            'Wednesday': 2,
            'Thursday': 3,
            'Friday': 4,
            'Saturday': 5,
            'Sunday': 6
        }

        # Add day of week column
        data['day_of_week'] = data['timestamp'].dt.day_name()

        # Calculate average traffic for each day of the week
        daily_traffic = data.groupby('day_of_week')['timestamp'].count().reset_index()
        daily_traffic.columns = ['Day', 'Count']

        # Add sort key and sort by day of week
        daily_traffic['sort_key'] = daily_traffic['Day'].map(day_mapping)
        daily_traffic = daily_traffic.sort_values('sort_key')

        # Create the line plot
        fig = px.line(
            daily_traffic,
            x='Day',
            y='Count',
            title='Average Daily Traffic Pattern by Day of Week',
            markers=True
        )

        # Update layout
        fig.update_layout(
            xaxis_title="Day of Week",
            yaxis_title="Number of Requests",
            xaxis=dict(
                tickmode='array',
                ticktext=daily_traffic['Day'],
                tickvals=list(range(len(daily_traffic))),
                tickangle=0
            ),
            yaxis=dict(
                tickformat=",",  # Add comma separator for thousands
                gridcolor='lightgray'
            ),
            plot_bgcolor='white',
            hoverlabel=dict(
                bgcolor="white",
                font=dict(
                    family="Arial",
                    size=14,
                    color="black"
                )
            )
        )

        # Update hover template
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>" +
                          "Requests: %{y:,.0f}<br>" +
                          "<extra></extra>",
            line=dict(width=3),  # Make line thicker
            marker=dict(
                size=10,  # Make markers larger
                line=dict(width=2, color='white')  # Add white border to markers
            )
        )

        return fig

    # Sidebar filters
    st.sidebar.header("Filters")

    # Date range filter
    min_date = df['timestamp'].min().date()
    max_date = df['timestamp'].max().date()
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Status code filter
    status_codes = sorted(df['status'].unique())
    selected_status = st.sidebar.multiselect(
        "Status Codes",
        status_codes,
        default=status_codes
    )

    # Method filter
    methods = sorted(df['method'].unique())
    selected_methods = st.sidebar.multiselect(
        "HTTP Methods",
        methods,
        default=methods
    )

    # Apply filters
    mask = (
        (df['timestamp'].dt.date >= date_range[0]) &
        (df['timestamp'].dt.date <= date_range[1]) &
        (df['status'].isin(selected_status)) &
        (df['method'].isin(selected_methods))
    )
    filtered_df = df[mask]

    # Display the selected plot
    if plot_option == "Status Code Distribution":
        st.plotly_chart(plot_status_distribution(filtered_df), use_container_width=True)
        st.markdown("""
        This visualization shows the distribution of HTTP status codes in the server logs:
        - 2xx codes indicate successful requests
        - 3xx codes indicate redirections
        - 4xx codes indicate client errors
        - 5xx codes indicate server errors
        """)

    elif plot_option == "Top Endpoints by Requests":
        st.plotly_chart(plot_top_endpoints(filtered_df), use_container_width=True)
        st.markdown("""
        This chart displays the most frequently requested endpoints on the NASA web server,
        helping identify the most popular content and potential bottlenecks.
        """)

    elif plot_option == "Request Methods Distribution":
        st.plotly_chart(plot_method_distribution(filtered_df), use_container_width=True)
        st.markdown("""
        This pie chart shows the distribution of HTTP methods (GET, POST, etc.) used in requests,
        indicating how clients are interacting with the server.
        """)

    elif plot_option == "Content Size Distribution":
        st.plotly_chart(plot_content_size_distribution(filtered_df), use_container_width=True)
        st.markdown("""
        This histogram shows the distribution of response content sizes,
        helping identify patterns in the size of content being served.
        """)

    elif plot_option == "Host Request Frequency":
        st.plotly_chart(plot_host_frequency(filtered_df), use_container_width=True)
        st.markdown("""
        This visualization shows the hosts making the most requests to the server,
        helping identify heavy users and potential abuse.
        """)

    elif plot_option == "Hourly Traffic Pattern":
        st.plotly_chart(plot_hourly_traffic(filtered_df), use_container_width=True)
        st.markdown("""
        This line chart shows the pattern of requests throughout the day,
        helping identify peak usage times and traffic patterns.
        """)

    else:  # Daily Traffic Pattern
        st.plotly_chart(plot_daily_traffic(filtered_df), use_container_width=True)
        st.markdown("""
        This line chart shows the daily pattern of requests throughout the dataset period,
        helping identify trends and unusual days.
        """)

    # Display data statistics
    st.sidebar.markdown("---")
    st.sidebar.subheader("Data Statistics")
    st.sidebar.write(f"Total Requests: {len(filtered_df):,}")
    st.sidebar.write(f"Unique Hosts: {filtered_df['host'].nunique():,}")
    st.sidebar.write(f"Total Data Transferred: {filtered_df['content_size'].sum() / 1024**2:.2f} MB")

    # Display raw data option
    if st.sidebar.checkbox("Show Raw Data"):
        st.subheader("Raw Data")
        st.write(filtered_df)