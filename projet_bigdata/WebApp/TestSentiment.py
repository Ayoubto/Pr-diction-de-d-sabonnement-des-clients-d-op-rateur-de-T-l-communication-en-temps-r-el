import time
import streamlit as st
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pymongo import MongoClient
import pandas as pd
import altair as alt
import datetime


client = MongoClient("mongodb://localhost:27017")
database = client["TwitterSentimentAnalysis"]
collection = database["UserResults"]
model_dir = '../Pretrained_LogisticRegression.pkl'

positive_tweets = []
negative_tweets = []
neutral_tweets = []
irrelevant_tweets = []


def test_sentiment():
    st.markdown("<h1 style='text-align: center; color: #F63366;'>Analyze Tweet Sentiment</h1>", unsafe_allow_html=True)
    st.markdown("<h4 font-size: 22px;'>Enter the tweet you want to analyze:</h4>", unsafe_allow_html=True)
    tweet_input = st.text_area("", max_chars=1000)
    submit = st.button("Submit")
    if submit:
        if tweet_input.strip():
            prediction = analyse_sentiment(tweet_input, model_dir)
            store_data(prediction, tweet_input)

        else:
            st.error("Please enter a tweet before submitting.")
    display_tweets()
    if submit:
        st.header("")
        st.markdown("<h3 style='color: #F63366; text-align: center;'>Sentiment Analysis Progress: Visualizing Sentiment Trends</h3>", unsafe_allow_html=True)
        st.header("")
        plot_sentiment_progress()


def store_data(prediction, tweet):
    timestamp = datetime.datetime.utcnow()
    data = {
        "timestamp": timestamp,
        "tweet": tweet,
        "prediction": prediction[1]
    }
    collection.insert_one(data)
    data = list(collection.find({}, {"_id": 0}))
    for tweet_data in data:
        if tweet_data['prediction'] == 'positive':
            positive_tweets.append(tweet_data)
        elif tweet_data['prediction'] == 'negative':
            negative_tweets.append(tweet_data)
        elif tweet_data['prediction'] == 'neutral':
            neutral_tweets.append(tweet_data)
        else:
            irrelevant_tweets.append(tweet_data)

def display_tweets():
    col1, col2 = st.columns(2)

    if positive_tweets:
        with col1:
            st.subheader("")
            st.markdown("<h3 style='color: #F63366;text-align:center;'>Positive Sentiment Tweets</h3>", unsafe_allow_html=True)
            positive_df = pd.DataFrame(positive_tweets)
            positive_df.drop(columns='_id', inplace=True, errors='ignore')  # Drop the _id column
            st.dataframe(positive_df,width=1000)

    if negative_tweets:
        with col2:
            st.subheader("")
            st.markdown("<h3 style='color: #F63366;text-align:center;'>Negative Sentiment Tweets</h3>", unsafe_allow_html=True)
            negative_df = pd.DataFrame(negative_tweets)
            negative_df.drop(columns='_id', inplace=True, errors='ignore')  # Drop the _id column
            st.dataframe(negative_df,width=1000)
    col3, col4 = st.columns(2)

    if neutral_tweets:
        with col3:
            st.subheader("")
            st.markdown("<h3 style='color: #F63366;text-align:center;'>Neutral Sentiment Tweets</h3>", unsafe_allow_html=True)
            neutral_df = pd.DataFrame(neutral_tweets)
            neutral_df.drop(columns='_id', inplace=True, errors='ignore')  # Drop the _id column
            st.dataframe(neutral_df, width=1000)

    if irrelevant_tweets:
        with col4:
            st.subheader("")
            st.markdown("<h3 style='color: #F63366;text-align:center;'>Irrelevant Sentiment Tweets</h3>", unsafe_allow_html=True)
            irrelevant_df = pd.DataFrame(irrelevant_tweets)
            irrelevant_df.drop(columns='_id', inplace=True, errors='ignore')  # Drop the _id column
            st.dataframe(irrelevant_df, width=1000)

def convert_sentiment_column(df):
    # Map string values to numerical values for 'prediction' column
    sentiment_mapping = {'negative': 0, 'positive': 1, 'neutral': 2, 'irrelevant': -1}
    df['prediction'] = df['prediction'].map(sentiment_mapping)
def analyse_sentiment(text, pretrained_path):
    start = time.time()
    spark = SparkSession.builder.getOrCreate()
    tweet = spark.createDataFrame([(text,)], ["tweet"])
    model = PipelineModel.load(pretrained_path)
    transformed_data = model.stages[0].transform(tweet)
    transformed_data = model.stages[1].transform(transformed_data)
    transformed_data = model.stages[2].transform(transformed_data)
    predictions = model.stages[-1].transform(transformed_data)
    end = time.time()
    st.write(f"Time Taken: *{end - start:.2f}* seconds")

    prediction = predictions.selectExpr(
        "prediction",
        "CASE \
            WHEN prediction = 1.0 THEN 'positive' \
            WHEN prediction = 0.0 THEN 'negative' \
            WHEN prediction = 2.0 THEN 'neutral' \
            ELSE 'irrelevant' \
        END as sentiment"
    ).collect()[0]

    if prediction['sentiment'] == 'positive':
        st.success('Predicted Sentiment: positive')
    elif prediction['sentiment'] == 'negative':
        st.error('Predicted Sentiment: negative')
    elif prediction['sentiment'] == 'neutral':
        st.info('Predicted Sentiment: neutral')
    else:
        st.warning('Predicted Sentiment: irrelevant')
    return prediction

def plot_sentiment_progress():
    # Fetch data from MongoDB
    data = list(collection.find({}, {"_id": 0, "prediction": 1}))
    df = pd.DataFrame(data)

    # Count the occurrences of each sentiment category
    sentiment_counts = df['prediction'].value_counts()

    # Create a DataFrame from sentiment_counts
    sentiment_df = pd.DataFrame({
        'Sentiment': sentiment_counts.index,
        'Count': sentiment_counts.values
    })

    # Plotting the stacked bar chart
    chart = alt.Chart(sentiment_df).mark_bar().encode(
        x='Count',
        y=alt.Y('Sentiment', sort=None),
        color='Sentiment'
    ).properties(
        width=600,
        height=300
    )

    st.altair_chart(chart, use_container_width=True)