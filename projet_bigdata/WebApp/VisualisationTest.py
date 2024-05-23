import streamlit as st

def display_dfs(df_positive, df_negative, df_irrelevant, df_neutral):
    st.subheader('Positive Sentiment DataFrame')
    st.write(df_positive)

    st.subheader('Negative Sentiment DataFrame')
    st.write(df_negative)

    st.subheader('Neutral Sentiment DataFrame')
    st.write(df_irrelevant)

    st.subheader('Neutral Sentiment DataFrame')
    st.write(df_neutral)
