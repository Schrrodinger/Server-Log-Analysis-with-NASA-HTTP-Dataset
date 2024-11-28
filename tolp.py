import streamlit as st
import seaborn as sns
from pyspark.sql.functions import regexp_extract, col
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import glob
from pyspark.sql.functions import regexp_extract, col, to_timestamp, hour
import os
import re
import sys
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
import matplotlib.pyplot as plt
import requests

response = requests.get("http://localhost:8001/logs/status_distribution")
status_data = response.json()



def tolp_page():
    # Set up Spark session
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder \
        .appName("MyApp") \
        .getOrCreate()

# Set the path for the dataset in the root directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_file = os.path.join(current_dir, "access_log_JulAug_95.txt")

# Debug log to confirm the path
    st.write(f"Looking for file at: {raw_data_file}")

# Load the file
    base_df = spark.read.text(raw_data_file)

    # Debug log for the file path
    st.write(f"Attempting to load file from: {raw_data_file}")

# Check if the file exists
    if not os.path.exists(raw_data_file):
        st.error(f"File not found at: {raw_data_file}")
    else:
        st.success(f"File found at: {raw_data_file}")

# Load the file using Spark
    base_df = spark.read.text(raw_data_file)
    base_df.show()

    # Define patterns for extraction
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
    ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    status_pattern = r'\s(\d{3})\s'
    content_size_pattern = r'\s(\d+)$'

    # Extract data using regex
    logs_df = base_df.select(
        F.regexp_extract('value', host_pattern, 1).alias('host'),
        F.regexp_extract('value', ts_pattern, 1).alias('timestamp'),
        F.regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
        F.regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
        F.regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
        F.regexp_extract('value', status_pattern, 1).cast(IntegerType()).alias('status'),
        F.regexp_extract('value', content_size_pattern, 1).cast(IntegerType()).alias('content_size')
    )

    # Fill null values
    logs_df = logs_df.na.fill({'content_size': 0})

    # Define a function to parse timestamps
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }

    def parse_clf_time(text):
        return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
            int(text[7:11]),
            month_map[text[3:6]],
            int(text[0:2]),
            int(text[12:14]),
            int(text[15:17]),
            int(text[18:20])
        )

    # Register UDF for timestamp parsing
    udf_parse_time = F.udf(parse_clf_time, StringType())

    # Add parsed time to DataFrame
    logs_df = logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp')


    # Plot functions
    def plot_top_5_host_activity_weekday(logs_df):
        # Add a 'weekday' column to the DataFrame
        logs_df = logs_df.withColumn('weekday', F.date_format('time', 'E'))  # 'E' gives abbreviated day name

        # Group by 'weekday' and 'host' to count requests
        host_activity_weekday = logs_df.groupBy('weekday', 'host').count()

        # Get the top 5 hosts based on total activity during weekdays
        top_hosts = host_activity_weekday.groupBy('host').agg(F.sum('count').alias('total_requests')) \
                                           .orderBy('total_requests', ascending=False) \
                                           .limit(5)

        # Get the top 5 hosts for plotting
        top_5_hosts = top_hosts.limit(5).toPandas()

        # Prepare data for plotting
        host_activity_weekday_pd = host_activity_weekday.toPandas()
        top_5_hosts_data = host_activity_weekday_pd[host_activity_weekday_pd['host'].isin(top_5_hosts['host'])]

        # Define the custom order for weekdays
        weekday_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

        # Convert 'weekday' to a categorical type with the defined order
        top_5_hosts_data['weekday'] = pd.Categorical(top_5_hosts_data['weekday'], categories=weekday_order, ordered=True)

        # Sort the data by weekday
        top_5_hosts_data = top_5_hosts_data.sort_values('weekday')

        # Create the figure and axis
        plt.figure(figsize=(14, 8))

        # Define colors and markers for different hosts
        colors = ['blue', 'orange', 'green', 'red', 'violet']
        markers = ['o', 's', 'D', '^', 'x']  # Circle, Square, Diamond, Triangle, X

        # Plot each host's activity over weekdays
        for i, host in enumerate(top_5_hosts['host']):
            host_data = top_5_hosts_data[top_5_hosts_data['host'] == host]
            if not host_data.empty:
                plt.plot(host_data['weekday'], host_data['count'], label=host, color=colors[i], marker=markers[i])
            else:
                st.warning(f"No data available for host: {host}")

        # Add titles and labels
        plt.title("Top 5 Host Activity Pattern (Weekday)", fontsize=16)
        plt.xlabel("Weekday", fontsize=14)
        plt.ylabel("Number of Requests", fontsize=14)
        plt.legend(title="Host", fontsize=12)
        plt.xticks(rotation=45)
        plt.grid()

        # Show the plot in Streamlit
        st.pyplot(plt)
        plt.clf()

        # Create a table below the plot for the top 5 hosts
        top_5_hosts_pd = top_hosts.toPandas()
        st.table(top_5_hosts_pd)
    def plot_top_5_host_activity_hour(logs_df):
        # Add an 'hour' column to the DataFrame
        logs_df = logs_df.withColumn('hour', F.hour('time'))  # Extract hour from the timestamp

        # Group by 'hour' and 'host' to count requests
        host_activity_hour = logs_df.groupBy('hour', 'host').count()

        # Get the top 5 hosts based on total activity during hours
        top_hosts = host_activity_hour.groupBy('host').agg(F.sum('count').alias('total_requests')) \
                                       .orderBy('total_requests', ascending=False) \
                                       .limit(5)

        # Get the top 5 hosts for plotting
        top_5_hosts = top_hosts.limit(5).toPandas()

        # Prepare data for plotting
        host_activity_hour_pd = host_activity_hour.toPandas()
        top_5_hosts_data = host_activity_hour_pd[host_activity_hour_pd['host'].isin(top_5_hosts['host'])]

        # Sort the data by hour
        top_5_hosts_data = top_5_hosts_data.sort_values('hour')

        # Create the figure and axis
        plt.figure(figsize=(14, 8))

        # Define colors and markers for different hosts
        colors = ['blue', 'orange', 'green', 'red', 'violet']
        markers = ['o', 's', 'D', '^', 'x']  # Circle, Square, Diamond, Triangle, X

        # Plot each host's activity over hours
        for i, host in enumerate(top_5_hosts['host']):
            host_data = top_5_hosts_data[top_5_hosts_data['host'] == host]
            plt.plot(host_data['hour'], host_data['count'], label=host, color=colors[i], marker=markers[i])

        # Add titles and labels
        plt.title("Top 5 Host Activity Pattern (Hourly)", fontsize=16)
        plt.xlabel("Hour of the Day", fontsize=14)
        plt.ylabel("Number of Requests", fontsize=14)
        plt.legend(title="Host", fontsize=12)
        plt.xticks(range(24))  # Set x-ticks to show all hours
        plt.grid()

        # Show the plot in Streamlit
        st.pyplot(plt)
        plt.clf()

        # Create a table below the plot for the top 5 hosts
        top_5_hosts_pd = top_hosts.toPandas()
        st.table(top_5_hosts_pd)
    def plot_hourly_distribution_methods_status(logs_df):
        # Ensure that the 'time' column is in the correct timestamp format
        logs_df = logs_df.withColumn("time", F.to_timestamp("time", "yyyy-MM-dd HH:mm:ss"))

        # Extract the hour from each timestamp and create the 'hour' column
        logs_df = logs_df.withColumn('hour', F.hour('time'))

        # Filter for '4xx' status codes
        fourxx_df = logs_df.filter((F.col('status') >= 400) & (F.col('status') < 500))
        fourxx_hourly_counts = fourxx_df.groupBy('hour').count().orderBy('hour')
        fourxx_hourly_counts_pd = fourxx_hourly_counts.toPandas()

        # Filter for '2xx' status codes
        twoxx_df = logs_df.filter((F.col('status') >= 200) & (F.col('status') < 300))
        twoxx_hourly_counts = twoxx_df.groupBy('hour').count().orderBy('hour')
        twoxx_hourly_counts_pd = twoxx_hourly_counts.toPandas()

        # Filter for '3xx' status codes
        threexx_df = logs_df.filter((F.col('status') >= 300) & (F.col('status') < 400))
        threexx_hourly_counts = threexx_df.groupBy('hour').count().orderBy('hour')
        threexx_hourly_counts_pd = threexx_hourly_counts.toPandas()

        # Group by hour and HTTP method, count requests for each combination
        hourly_method_counts = logs_df.groupBy('hour', 'method').count()

        # Convert to Pandas DataFrame for plotting
        hourly_method_counts_pd = hourly_method_counts.toPandas()

        # Filter to include only the desired HTTP methods: GET, HEAD, POST
        filtered_methods = ['GET', 'HEAD', 'POST']
        hourly_method_counts_pd = hourly_method_counts_pd[hourly_method_counts_pd['method'].isin(filtered_methods)]

        # Pivot the DataFrame to have hours on the x-axis, methods as columns, and counts as values
        stacked_data = hourly_method_counts_pd.pivot(index='hour', columns='method', values='count').fillna(0)

        # Count the number of each status code category
        status_counts = {
            '2xx': logs_df.filter((F.col('status') >= 200) & (F.col('status') < 300)).count(),
            '3xx': logs_df.filter((F.col('status') >= 300) & (F.col('status') < 400)).count(),
            '4xx': logs_df.filter((F.col('status') >= 400) & (F.col('status') < 500)).count(),
        }

        # Count the number of each HTTP method
        method_counts = {
            'GET': logs_df.filter(F.col('method') == 'GET').count(),
            'HEAD': logs_df.filter(F.col('method') == 'HEAD').count(),
            'POST': logs_df.filter(F.col('method') == 'POST').count(),
        }

        # Create the figure and axis
        fig, ax = plt.subplots(figsize=(14, 10))

        # Plot the hourly distribution of HTTP methods
        stacked_data.plot(kind='bar', stacked=True, colormap="viridis", ax=ax, alpha=0.7)

        # Overlay the status lines
        ax.plot(twoxx_hourly_counts_pd['hour'], twoxx_hourly_counts_pd['count'],
                color='green', label='2xx Status Codes', linewidth=2, marker='o')
        ax.plot(threexx_hourly_counts_pd['hour'], threexx_hourly_counts_pd['count'],
                color='blue', label='3xx Status Codes', linewidth=2, marker='o')
        ax.plot(fourxx_hourly_counts_pd['hour'], fourxx_hourly_counts_pd['count'],
                color='red', label='4xx Status Codes', linewidth=2, marker='o')

        # Add titles and labels
        ax.set_title("Hourly Distribution of HTTP Methods with Status Codes (2xx, 3xx, 4xx)", fontsize=16)
        ax.set_xlabel("Hour of the Day", fontsize=14)
        ax.set_ylabel("Request Count", fontsize=14)
        ax.legend(title="HTTP Status Codes")
        ax.grid()
        # Show the plot in Streamlit
        st.pyplot(fig)
        plt.clf()  # Clear the figure after displaying to avoid overlap in subsequent plots

        # Create a table below the plot
        table_data = [
            ['Status Code', 'Count'],
            ['2xx', status_counts['2xx']],
            ['3xx', status_counts['3xx']],
            ['4xx', status_counts['4xx']],
            ['GET', method_counts['GET']],
            ['HEAD', method_counts['HEAD']],
            ['POST', method_counts['POST']],
        ]

        # Convert to DataFrame for better display
        table_df = pd.DataFrame(table_data)

        # Display the table in Streamlit
        st.table(table_df)

    def plot_endpoint_distribution_by_weekday(logs_df):
        # Add a 'weekday' column to the DataFrame
        logs_df = logs_df.withColumn('weekday', F.date_format('time', 'E'))  # 'E' gives abbreviated day name

        # Create a new column for status categories
        logs_df = logs_df.withColumn(
            'status_code',
            F.when((F.col('status') >= 200) & (F.col('status') < 300), '2xx')
            .when((F.col('status') >= 300) & (F.col('status') < 400), '3xx')
            .when((F.col('status') >= 400) & (F.col('status') < 500), '4xx')
            .otherwise('Other')
        )

        # Group by 'weekday' and 'status_code' to count total endpoints
        endpoint_counts = logs_df.groupBy(['weekday', 'status_code']) \
            .agg(F.count('endpoint').alias('total_endpoints'))

        # Convert the aggregated DataFrame to Pandas
        endpoint_counts_pd = endpoint_counts.toPandas()

        # Define the order for weekdays
        weekday_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

        # Convert 'weekday' to a categorical type with the defined order
        endpoint_counts_pd['weekday'] = pd.Categorical(endpoint_counts_pd['weekday'], categories=weekday_order, ordered=True)

        # Sort the data by weekday
        endpoint_counts_pd = endpoint_counts_pd.sort_values('weekday')

        # Filter to keep only 4xx and other relevant status codes
        endpoint_counts_pd = endpoint_counts_pd[endpoint_counts_pd['status_code'].isin(['2xx', '3xx', '4xx'])]

        # Set the aesthetic style of the plots
        sns.set(style="whitegrid")

        # Create a bar plot for total endpoints by weekday and status code
        plt.figure(figsize=(12, 6))
        sns.barplot(data=endpoint_counts_pd, x='weekday', y='total_endpoints', hue='status_code', errorbar=None)

        # Add titles and labels
        plt.title("Distribution of Total Endpoints by Status Code in Weekday Order", fontsize=16)
        plt.xlabel("Weekday", fontsize=12)
        plt.ylabel("Total Number of Endpoints", fontsize=12)
        plt.legend(title='Status Code')

        # Show the plot in Streamlit
        st.pyplot(plt)
        plt.clf()  # Clear the figure after displaying to avoid overlap in subsequent plots

        # Generate a summary table for total endpoints by status code
        summary_table = endpoint_counts_pd.groupby('status_code')['total_endpoints'].sum().reset_index()

        # Display the summary table
        st.write("\nSummary Table of Total Endpoints by Status Code:")
        st.dataframe(summary_table)


    def plot_unique_endpoints_by_status(logs_df, status_code_range, title):
        # Step 1: Filter for the specified status codes and the GET method
        filtered_df = logs_df.filter(
            (F.col('status') >= status_code_range[0]) & (F.col('status') < status_code_range[1]) & (
                        F.col('method') == 'GET')
        )

        # Step 2: Add 'hour' and 'weekday' columns to the DataFrame
        filtered_df = filtered_df.withColumn('hour', F.hour('time'))  # Extract hour from the timestamp
        filtered_df = filtered_df.withColumn('weekday', F.date_format('time', 'E'))  # Get abbreviated day name

        # Step 3: Create a new column for hour ranges
        filtered_df = filtered_df.withColumn(
            'hour_range',
            F.when((F.col('hour') >= 1) & (F.col('hour') <= 3), '1-3')
            .when((F.col('hour') >= 4) & (F.col('hour') <= 6), '4-6')
            .when((F.col('hour') >= 7) & (F.col('hour') <= 9), '7-9')
            .when((F.col('hour') >= 10) & (F.col('hour') <= 12), '10-12')
            .when((F.col('hour') >= 13) & (F.col('hour') <= 15), '13-15')
            .when((F.col('hour') >= 16) & (F.col('hour') <= 18), '16-18')
            .when((F.col('hour') >= 19) & (F.col('hour') <= 21), '19-21')
            .when((F.col('hour') >= 22) & (F.col('hour') <= 24), '22-24')
        )

        # Step 4: Group by 'weekday' and 'hour_range' to count unique endpoints
        hourly_endpoint_counts = filtered_df.groupBy(['weekday', 'hour_range']).agg(
            F.countDistinct('endpoint').alias('unique_endpoints'))

        # Step 5: Convert to Pandas DataFrame for plotting
        hourly_endpoint_counts_pd = hourly_endpoint_counts.toPandas()

        # Step 6: Drop rows with NaN values
        hourly_endpoint_counts_pd.dropna(inplace=True)

        # Step 7: Define the order for weekdays
        weekday_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        hourly_endpoint_counts_pd['weekday'] = pd.Categorical(hourly_endpoint_counts_pd['weekday'],
                                                              categories=weekday_order, ordered=True)

        # Step 8: Define the order for hour ranges
        hour_range_order = ['1-3', '4-6', '7-9', '10-12', '13-15', '16-18', '19-21', '22-24']
        hourly_endpoint_counts_pd['hour_range'] = pd.Categorical(hourly_endpoint_counts_pd['hour_range'],
                                                                 categories=hour_range_order, ordered=True)

        # Step 9: Sort the data by weekday and hour_range
        hourly_endpoint_counts_pd = hourly_endpoint_counts_pd.sort_values(['hour_range', 'weekday'])

        # Step 10: Create a line plot for each weekday
        plt.figure(figsize=(12, 6))  # Adjust the figure size as needed

        # Define colors and markers for each weekday
        colors = ['blue', 'orange', 'green', 'red', 'purple', 'brown', 'pink']
        markers = ['o', 's', 'D', '^', 'v', '<', '>']

        # Plot each weekday
        for i, weekday in enumerate(weekday_order):
            weekday_data = hourly_endpoint_counts_pd[hourly_endpoint_counts_pd['weekday'] == weekday]

            # Plot the line for the current weekday
            plt.plot(weekday_data['hour_range'].astype(str), weekday_data['unique_endpoints'],
                     color=colors[i], marker=markers[i], label=weekday, alpha=0.7)

        # Add titles and labels
        plt.title(title, fontsize=16)
        plt.xlabel("Hour Range", fontsize=12)
        plt.ylabel("Number of Unique Endpoints", fontsize=12)
        plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
        plt.grid()
        plt.legend(title='Weekdays')
        plt.tight_layout()

        # Show the plot in Streamlit
        st.pyplot(plt)
        plt.clf()  # Clear the figure after displaying to avoid overlap in subsequent plots


    # Function to plot unique endpoints for 2xx status codes
    def plot_unique_endpoints_2xx(logs_df):
        plot_unique_endpoints_by_status(logs_df, (200, 300),
                                        "Unique Endpoints for 2xx Status Codes (GET Method) by Hour Range")


    # Function to plot unique endpoints for 4xx status codes
    def plot_unique_endpoints_4xx(logs_df):
        plot_unique_endpoints_by_status(logs_df, (400, 600),
                                        "Unique Endpoints for 4xx Status Codes (GET Method) by Hour Range")


    # Function to plot both 2xx and 4xx unique endpoints
    def plot_unique_endpoints_combined(logs_df):
        plot_unique_endpoints_2xx(logs_df)
        plot_unique_endpoints_4xx(logs_df)



    def plot_unique_hosts_by_status(logs_df, status_code_range, title):
        # Step 1: Filter for the specified status codes
        filtered_df = logs_df.filter((F.col('status') >= status_code_range[0]) & (F.col('status') < status_code_range[1]))

        # Step 2: Add 'hour' and 'weekday' columns to the DataFrame
        filtered_df = filtered_df.withColumn('hour', F.hour('time'))  # Extract hour from the timestamp
        filtered_df = filtered_df.withColumn('weekday', F.date_format('time', 'E'))  # Get abbreviated day name

        # Step 3: Create a new column for hour ranges
        filtered_df = filtered_df.withColumn(
            'hour_range',
            F.when((F.col('hour') >= 1) & (F.col('hour') <= 3), '1-3')
            .when((F.col('hour') >= 4) & (F.col('hour') <= 6), '4-6')
            .when((F.col('hour') >= 7) & (F.col('hour') <= 9), '7-9')
            .when((F.col('hour') >= 10) & (F.col('hour') <= 12), '10-12')
            .when((F.col('hour') >= 13) & (F.col('hour') <= 15), '13-15')
            .when((F.col('hour') >= 16) & (F.col('hour') <= 18), '16-18')
            .when((F.col('hour') >= 19) & (F.col('hour') <= 21), '19-21')
            .when((F.col('hour') >= 22) & (F.col('hour') <= 24), '22-24')
        )

        # Step 4: Group by 'weekday' and 'hour_range' to count unique hosts
        hourly_host_counts = filtered_df.groupBy('weekday', 'hour_range').agg(F.countDistinct('host').alias('unique_hosts'))

        # Step 5: Convert to Pandas DataFrame for plotting
        hourly_host_counts_pd = hourly_host_counts.toPandas()

        # Step 6: Drop rows with NaN values
        hourly_host_counts_pd.dropna(inplace=True)

        # Step 7: Define the order for weekdays
        weekday_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        hourly_host_counts_pd['weekday'] = pd.Categorical(hourly_host_counts_pd['weekday'], categories=weekday_order,
                                                          ordered=True)

        # Step 8: Define the order for hour ranges
        hour_range_order = ['1-3', '4-6', '7-9', '10-12', '13-15', '16-18', '19-21', '22-24']
        hourly_host_counts_pd['hour_range'] = pd.Categorical(hourly_host_counts_pd['hour_range'],
                                                             categories=hour_range_order, ordered=True)

        # Step 9: Sort the data by weekday and hour_range
        hourly_host_counts_pd = hourly_host_counts_pd.sort_values(['hour_range', 'weekday'])

        # Step 10: Create a line plot for each weekday
        plt.figure(figsize=(12, 6))  # Adjust the figure size as needed

        # Define colors and markers for each weekday
        colors = ['blue', 'orange', 'green', 'red', 'purple', 'brown', 'pink']
        markers = ['o', 's', 'D', '^', 'v', '<', '>']

        # Plot each weekday
        for i, weekday in enumerate(weekday_order):
            weekday_data = hourly_host_counts_pd[hourly_host_counts_pd['weekday'] == weekday]

            # Plot the line for the current weekday
            plt.plot(weekday_data['hour_range'].astype(str), weekday_data['unique_hosts'],
                     color=colors[i], marker=markers[i], label=weekday, alpha=0.7)

        # Add titles and labels
        plt.title(title, fontsize=16)
        plt.xlabel("Hour Range", fontsize=12)
        plt.ylabel("Number of Unique Hosts", fontsize=12)
        plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
        plt.grid()
        plt.legend(title='Weekdays')
        plt.tight_layout()

        # # Show the plot in Streamlit
        st.pyplot(plt)
        plt.clf()  # Clear the figure after displaying to avoid overlap in subsequent plots


    # Function to plot unique hosts for 2xx status codes
    def plot_unique_hosts_2xx(logs_df):
        plot_unique_hosts_by_status(logs_df, (200, 300), "Unique Hosts for 2xx Status Codes by Hour Range")


    # Function to plot unique hosts for 4xx status codes
    def plot_unique_hosts_4xx(logs_df):
        plot_unique_hosts_by_status(logs_df, (400, 600), "Unique Hosts for 4xx Status Codes by Hour Range")


    # Call the functions to display the plots in the Streamlit app
    def plot_unique_hosts_combined(logs_df):
        plot_unique_hosts_2xx(logs_df)
        plot_unique_hosts_4xx(logs_df)


    # Streamlit App

    st.title("NASA Web Logs Analysis Dashboard Page 2")
    st.write("Analysis of NASA web server logs from July-August 1995")
    st.sidebar.header("Select Plot")

    plot_options = {
        "Top 5 Host Activity Pattern (Weekday)": plot_top_5_host_activity_weekday,
        "Top 5 Host Activity Pattern (Hourly)": plot_top_5_host_activity_hour,
        "Hourly Distribution of HTTP Methods with Status": plot_hourly_distribution_methods_status,
        "Distribution of Total Endpoints by Status Code": plot_endpoint_distribution_by_weekday,
        "Unique Endpoints by Status Codes (GET Method)": plot_unique_endpoints_combined,
        "Unique Hosts for 2xx Status Codes": plot_unique_hosts_combined,
    }

    selected_plot = st.sidebar.radio("Choose a plot to visualize:", list(plot_options.keys()))

    if selected_plot == "Top 5 Host Activity Pattern (Weekday)":
        plot_top_5_host_activity_weekday(logs_df)
        st.markdown("""
        This visualization shows the activity pattern of the top 5 hosts across the week:
    
        **Hosts:**
    
        * **piweba3y.prodigy.com**: Shows a steady increase in activity throughout the week, starting from around 2500 requests on Monday and peaking at over 3500 requests on Sunday.
        * **piweba4y.prodigy.com**: Exhibits a more gradual increase in activity, starting at around 2000 requests on Monday and reaching over 3200 requests by Sunday.
        * **piweba1y.prodigy.com**: Shows a modest upward trend, with an initial number of requests around 1700 on Monday and ending at just over 2300 on Sunday.
        * **edams.ksc.nasa.gov**: Experiences a peak on Thursday, reaching around 2500 requests. Activity then drops significantly, finishing at under 400 requests on Sunday.
        * **163.206.89.4**: Has a distinct pattern, with a peak on Tuesday at around 2300 requests, then a sharp decline throughout the week, reaching a low of around 100 requests by Sunday.
    
        **Overall:**
    
        * The plot shows a general increase in activity throughout the week for most of the hosts.
        * The highest activity is observed on the weekend (Saturday and Sunday) for 3 of the hosts, whereas edams.ksc.nasa.gov and 163.206.89.4 see a sharp drop in activity on those days.
        * The peak day for edams.ksc.nasa.gov is Thursday.
        * 163.206.89.4 has a unique pattern, with a peak on Tuesday and a significant decline throughout the week.
        """)
    elif selected_plot == "Top 5 Host Activity Pattern (Weekday)":
        plot_top_5_host_activity_hour(logs_df)
        st.markdown("""
    
        This chart displays the hourly activity pattern for the top 5 most frequently requested endpoints on the NASA web server. 
        Each line represents the number of requests per hour for a specific host. 
        The information shown on the graph can be used to understand the usage patterns, identify peak hours, and potentially optimize resource allocation.
    
        - **piweba3y.prodigy.com** has the highest number of requests overall, with a peak around hour 15.
        - **piweba4y.prodigy.com** shows a steady increase in requests towards the end of the day, reaching a peak at hour 22.
        - **piweba1y.prodigy.com** experiences a fluctuating pattern with a prominent peak around hour 20. 
        - **edams.ksc.nasa.gov** has a more consistent activity pattern, with a peak around hour 13.
        - **163.206.89.4** displays a relatively consistent pattern, with a peak around hour 12.
    
        While **edams** and **163.206.89.4** have the lowest total number of requests, they are the two most requested hosts from hour 5 to approximately hour 14; while **piweba3y.prodigy.com** is the most requested host for the remaining 13 hours. 
        """)
    elif selected_plot == "Hourly Distribution of HTTP Methods with Status":
        plot_hourly_distribution_methods_status(logs_df)
        st.markdown("""
            This chart displays the most frequently requested endpoints on the NASA web server, helping identify the most popular content and potential bottlenecks.
    
            The plot shows a breakdown of requests by HTTP method and status code.
    
            - **GET** method is the most used, with a peak around 14.00-15.00. It shows a steady increase of requests throughout the day.
    
            - **HEAD** method has a similar trend, with a peak around 14.00-15.00. It's considerably lower than GET requests.
    
            - **POST** method has a very low volume of requests, with no significant peak.
    
            - **2xx Status Codes** represent successful responses, with a peak around 14.00-15.00. This peak is similar to the peak of the most used GET method.
    
            - **3xx Status Codes** represent redirection responses, with a small and steady volume. It shows a peak around 9.00-10.00.
    
            - **4xx Status Codes** represent client errors, with a very low volume and no significant peaks.
    
            - **5xx Status Codes** represent server errors, with a very low volume and no significant peaks.
    
            **Insights:**
    
            The plot highlights the busiest hours on the NASA web server, with a significant peak around 14.00-15.00. This information can help identify potential bottlenecks and optimize system performance. The high volume of GET requests compared to other methods suggests that the majority of users are accessing static content, such as images, videos, or text files. The low volume of POST requests indicates that the server is not handling many form submissions or other dynamic content requests. The low volume of client and server errors suggests that the server is operating efficiently.
            """)
    elif selected_plot == "Distribution of Total Endpoints by Status Code":
        plot_endpoint_distribution_by_weekday(logs_df)
        st.markdown("""
            This chart displays the most frequently requested endpoints on the NASA web server,
            helping identify the most popular content and potential bottlenecks.
            **High Volume on Thursday:** The highest number of endpoints are observed on Thursday, particularly for the 2xx status code.
            **Dominant Status Code:** 2xx status code represents the majority of endpoints across all days of the week, with significantly higher numbers compared to 3xx and 4xx.
            **Weekday Trends:** The number of endpoints generally increases from Monday to Thursday, then declines toward the weekend.
            **Status Code Distribution:**
            * **2xx:** High volume on Thursday, followed by Wednesday, Tuesday, and Monday. Lower numbers on Sunday and Saturday.
            * **3xx:** Relatively low numbers compared to 2xx, with highest volume on Tuesday.
            * **4xx:** Lowest volume compared to 2xx and 3xx, with highest volume on Thursday.
            Overall, the plot suggests a pattern of higher endpoint activity during the week, with a peak on Thursday. The 2xx status code dominates across all days, highlighting the successful processing of requests.
            """)
    elif selected_plot == "Unique Endpoints by Status Codes (GET Method)":
        plot_unique_endpoints_combined(logs_df)
        st.markdown("""
            This chart displays the most frequently requested endpoints on the NASA web server,
            helping identify the most popular content and potential bottlenecks.
    
            **2xx Status Code Analysis:**
    
            * **Highest Peaks:** Tuesday and Thursday show the highest number of unique endpoints for 2xx status codes, peaking around 2700.
            * **Overall Trend:** The number of unique endpoints generally increases throughout the day, peaking in the afternoon hours (13-15), and then decreasing towards evening.
    
            **4xx Status Code Analysis:**
    
            * **Highest Peak:** Wednesday shows the highest number of unique endpoints for 4xx status codes, peaking around 350 at the 10-12 hour range.
            * **Overall Trend:** The number of unique endpoints rises sharply from the early hours, reaching its peak around 10-12. It then decreases gradually throughout the day.
    
            **Comparison:**
    
            * Although Tuesday and Thursday have the highest peaks for 2xx status codes, Wednesday shows a much higher peak for 4xx status codes. This difference could be due to various factors, such as different traffic patterns, system stability.
            * The overall trend for 2xx and 4xx status codes is different. While 2xx endpoints generally increase during the day, 4xx endpoints reach their peak earlier in the day and then decrease. 
            """)
    elif selected_plot == "Unique Hosts for 2xx Status Codes":
        plot_unique_hosts_combined(logs_df)
        st.markdown("""
            Both plots show a similar pattern with a peak around 13-15, indicating a higher concentration of unique hosts during that hour range for both 2xx and 4xx status codes.
            The scale of the two plots is different, with the 2xx plot reaching over 8,000 unique hosts compared to the 4xx plot reaching over 300. This suggests that significantly more hosts are involved in 2xx status codes than 4xx status codes during these hours.
            Overall, the plots suggest that both 2xx and 4xx status codes exhibit a similar pattern of unique host activity throughout the day, with a peak around 13-15. However, the magnitude of unique hosts is significantly higher for 2xx status codes compared to 4xx status codes.
            """)

