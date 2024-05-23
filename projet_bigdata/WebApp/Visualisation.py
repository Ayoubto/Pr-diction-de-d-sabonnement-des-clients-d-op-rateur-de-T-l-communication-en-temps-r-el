import streamlit as st
from streamlit_option_menu import option_menu
from MongoDB import connect_to_mongodb, get_realtime_data
import time

import pandas as pd
import plotly.express as px
import pymongo
import plotly.graph_objects as go

# added
last_rerun_time = 0


database_url = "mongodb://mongodb:27017/"
database_name = "TwitterSentimentAnalysis"
collection_name = "TweetPrediction"

def visualization():


    st.markdown("""
        <style>
        /* Center align the title */
        .title {
            text-align: center;
        }
        /* Add padding to the left side of the content */
        .content {
            padding-left: 180px;
        }
        .stMarkdown p, .stMarkdown li {
            font-size: 20px !important;
        }
        /* Increase font size of titles */
        .content h2 {
            font-size: 40px;
        }
        .sentiment-counter {
            font-size: 24px;
            margin-right: 30px;
        }
        </style>
        """, unsafe_allow_html=True)


    # Connect to MongoDB
    collection = connect_to_mongodb(database_url, database_name, collection_name)
    if collection is None:
        print("Failed to connect to MongoDB.")
        return

    # Retrieve real-time data
    cursor = collection.find({}, {"_id": 0})  # Exclude the _id field
    df = pd.DataFrame(list(cursor))
    if df.empty:
        print("No data retrieved from MongoDB.")
        return

    st.markdown('<div class="title" "> <h1 style=" color: #F63366;"> Twitter Sentiment Analysis Dashboard</h1> </div>', unsafe_allow_html=True)

    co1, co2 = st.columns(2)
    with co1:
        # top-level sentiment filters
        sentiment_options = pd.unique(df["sentiment_prediction"])
        sentiment_options = ["All"] + list(sentiment_options)  # Add "All" option
        st.markdown("<h3 style='text-align:center;'>Filter Sentiment: </h3>", unsafe_allow_html=True)
        sentiment_filter = st.selectbox("", sentiment_options)

    with co2:
        # top-level games filters
        game_options = pd.unique(df["game"])
        game_options = ["All"] + list(game_options)  # Add "All" option
        st.markdown("<h3 style='text-align:center;'>Filter Game: </h3>", unsafe_allow_html=True)
        game_filter = st.selectbox("", game_options)
    while True:


        # Calculate counts for each unique sentiment
        sentiment_counts = df['sentiment_prediction'].value_counts()
        game_counts = df['game'].value_counts()

        # Calculate total number of tweets injected
        total_tweets_injected = len(df)

        filtered_df = df
        if sentiment_filter != "All":
            filtered_df = filtered_df[filtered_df["sentiment_prediction"] == sentiment_filter]
        if game_filter != "All":
            filtered_df = filtered_df[filtered_df["game"] == game_filter]
        st.dataframe(filtered_df, width=2000)

        # Define sentiment colors and shades
        sentiment_colors = {
            'positive': ['#D42D5E', '#FA5788'],  # Red shades
            'negative': ['#202541', '#41536F'],  # Blue shades
            'neutral': ['#CCCCCC', '#D42D5C'],  # Light gray shades
            'irrelevant': ['#41536F', '#9CB0EA']
        }

        st.markdown("<h2 style='text-align:center;'>Tweet Counts By Sentiment: </h2>", unsafe_allow_html=True)


        # Create a layout to display the sentiment gauges in a single row with 4 columns
        col1, col2, col3, col4 = st.columns(4)

        # Define spacing between gauges
        spacing = 20

        # Display sentiment gauges
        for sentiment, count in sentiment_counts.items():
            # Create gauge chart for each sentiment
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=count,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': sentiment.capitalize(), 'font': {'size': 28}},
                gauge={'axis': {'range': [0, total_tweets_injected]},
                       'bar': {'color': sentiment_colors[sentiment][0]},
                       'bgcolor': '#f0f0f0',
                       'borderwidth': 2,
                       'bordercolor': 'gray',
                       'steps': [
                           {'range': [0, total_tweets_injected], 'color': sentiment_colors[sentiment][1]},
                           # Darker shade
                       ]}))
            # Add spacing between gauges
            gauge_with_spacing = fig.update_layout(margin=dict(t=spacing, b=spacing, l=spacing, r=spacing))

            # Display the gauge chart in the corresponding column
            if sentiment == 'positive':
                col1.plotly_chart(fig, use_container_width=True)
            elif sentiment == 'negative':
                col2.plotly_chart(fig, use_container_width=True)
            elif sentiment == 'neutral':
                col3.plotly_chart(fig, use_container_width=True)
            else:
                col4.plotly_chart(fig, use_container_width=True)
        # Create a column layout to display the sentiment counts
        num_columns = len(sentiment_counts)
        columns = st.columns(num_columns)


        # Define custom colors
        colors = ['red', 'green', 'blue', 'gray']
        fig_col3, fig_col4 = st.columns(2)
        with fig_col3:
            # Pie Plot
            # Pie Plot
            st.markdown("<h2 style='text-align:center;'> Sentiment Distribution - Pie Chart</h2>",unsafe_allow_html=True)
            sentiment_counts = df['sentiment_prediction'].value_counts()
            fig_pie = px.pie(values=sentiment_counts, names=sentiment_counts.index, color_discrete_sequence=px.colors.sequential.Bluyl)
            st.plotly_chart(fig_pie, use_container_width=True)

        with fig_col4:
            # Bar Plot
            st.markdown("<h2 style='text-align:center;'> Sentiment Distribution - Bar Plot</h2>", unsafe_allow_html=True)
            fig_bar = px.bar(x=sentiment_counts.index, y=sentiment_counts.values, color=sentiment_counts.index,
                             color_discrete_sequence=px.colors.sequential.Plasma_r)
            fig_bar.update_layout(xaxis_title='Sentiment', yaxis_title='Count', title='Sentiment Distribution - Bar Plot')
            st.plotly_chart(fig_bar, use_container_width=True)


        # create two columns for charts
        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            # Create cross-tabulation table
            count_table = pd.crosstab(index=df['game'], columns=df['sentiment_prediction'])

            # Create heatmap plot using Plotly
            fig = go.Figure(data=go.Heatmap(
                z=count_table.values,
                x=count_table.columns,
                y=count_table.index,
                colorscale='YlOrRd',
                colorbar=dict(title='Count')
            ))

            fig.update_layout(
                title='Sentiment Distribution by Game',
                xaxis_title='Sentiment',
                yaxis_title='Game',
                width=850,
                height=600
            )

            # Display the Plotly figure in Streamlit
            st.plotly_chart(fig)
        with fig_col2:
            # Create density heatmap
            fig = px.density_heatmap(
                data_frame=df, x='game', y='sentiment_prediction',
                title='Density Heatmap: Sentiment vs Game',
                histfunc='count',
                color_continuous_scale='Blues'
            )
            fig.update_layout(
                xaxis_title='Game',
                yaxis_title='Sentiment',
                width=800,
                height=750
            )
            st.plotly_chart(fig)
            time.sleep(3)  # Wait for 5 seconds before rerunning the visualization
            st.rerun()

