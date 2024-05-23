
"""
Created on Tuesday May 7 19:30:45 2024

@author1: <Hamza>

@author2: <Salma>
hello
"""
import time

import streamlit as st
from streamlit_option_menu import option_menu

from MongoDB import get_realtime_data, connect_to_mongodb
from TestSentiment import test_sentiment
from Visualisation import visualization
import pymongo


def custom_css():
    """
    Inject custom CSS styles into Streamlit app.
    """
    custom_styles = """
    <style>
    
        body {
            background-color: #f0f0f0; /* Set background color */
        }
        .container {
            max-width: 800px; /* Set maximum width for content */
            margin: auto; /* Center content horizontally */
            padding: 200px; /* Add padding to content */
            background-color: #ffffff; /* Set background color for content */
            border-radius: 10px; /* Add rounded corners to content */
            box-shadow: 0px 0px 10px 0px rgba(0,0,0,0.1); /* Add shadow to content */
        }
    </style>
    """
    # Inject custom CSS into Streamlit app
    st.markdown(custom_styles, unsafe_allow_html=True)

def home():
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
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="title"> <h1 style=" color: #F63366;"> Twitter Sentiment Analysis App </h1> </div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="content">
    
    ## Bienvenue!
    Cette application utilise l'apprentissage automatique pour prédire les sentiments en temps réel des flux de données du réseau social Twitter.

    <p style='text-align: center; color: #F63366; font-weight:900; font-size: 35px;'>---------------------------------------------------------------------------------------------------------</p>

    ## Caractéristiques principales
    - Prédiction en temps réel des sentiments des tweets sur Twitter.
    - Utilisation d'un modèle de régression logistique pour la prédiction.
    - Sauvegarde des résultats dans une base de données MongoDB.

    <p style='text-align: center; color: #F63366; font-weight:900; font-size: 35px;'>---------------------------------------------------------------------------------------------------------</p>

    ## Détails techniques
    - **Modèle utilisé:** Régression logistique.
    - **Précision de prédiction:** 90%.
    - **Temps de réponse:** Moins de 10 secondes en moyenne par sentiment.

    <p style='text-align: center; color: #F63366; font-weight:900; font-size: 35px;'>---------------------------------------------------------------------------------------------------------</p>

    ## Comment utiliser l'application
    1. Sélectionnez l'option 'Test Sentiment' pour entrer un tweet et obtenir son résultat de sentiment.
    2. Sélectionnez l'option 'Visualization' pour visualiser les données journalisées en temps réel.

    <p style='text-align: center; color: #F63366; font-weight:900; font-size: 35px;'>---------------------------------------------------------------------------------------------------------</p>

    ## À propos de l'application
    Cette application a été développée dans le cadre d'un mini-projet pour prédire les sentiments en temps réel des flux de données du réseau social Twitter.
    Elle utilise Apache Kafka Streams pour la lecture en temps réel des données, PySpark MLlib et NLTK pour les prétraitements et l'apprentissage automatique, et MongoDB pour la sauvegarde des résultats.
    Le modèle utilisé est une régression logistique, avec une précision de prédiction de 90%.

    <p style='text-align: center; color: #F63366; font-weight:900; font-size: 35px;'>---------------------------------------------------------------------------------------------------------</p>

    ## Comment utiliser l'application
    Pour commencer, sélectionnez l'une des options du menu en haut de la page.
    
    </div>
    """, unsafe_allow_html=True)


def about():
    st.markdown("""
    <style>
    /* Center align the title */
    .title {
        text-align: center;
    }
    /* Add padding to the left side of the content */
    .content {
        padding-left: 120px;
    }
    /* Increase font size for all text */
    .stMarkdown p, .stMarkdown li {
        font-size: 20px !important;
    }
    /* Increase font size of titles */
    .content h5, .content h3 {
        font-size: 24px;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<div class='title'> <h1> À propos de l'application </h1> </div>", unsafe_allow_html=True)

    st.markdown("""
    <div class="content">
    <br />
    <h5>Cette application a été développée dans le cadre d'un mini-projet pour prédire les sentiments en temps réel des flux de données du réseau social Twitter. </h5>

    <h3>Auteurs:</h3>
    
    - AMGAROU Salma <br/>
    - HAFDAOUI Hamza 

    <h3>Date de création:</h3>  
    
    - Mardi 7 mai 2024

    <h3>Technologies utilisées:</h3>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3, col4  = st.columns(4)
    with col1:
        st.header("")
        st.image("images/kafka.png",  width=360)
    with col2:
        st.image("images/mongod.png",  width=250)
    with col3:
        st.image("images/nltk.png",  width=270)
    with col4:
        st.header("")
        st.image("images/45.png",width=310)

    st.markdown("""
    <div class="content">
    
    <h3>Langages de programmation:</h3>
    <br />
    </div>
    """, unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        pass
    with col2:
        st.header("")
        st.image("images/python.svg",width=350)
        st.header("")
    with col3:
        pass
    with col4:
        pass
    with col5:
        pass

    st.markdown("""
    <div class="content">
    
    <h3>Framework:</h3>
    <br />
    </div>
    """, unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        pass
    with col2:
        st.image("images/streamlit.png",width=350)
        st.header("")
    with col3:
        pass
    with col4:
        pass
    with col5:
        pass


    st.markdown("""
    <div class="content">
    
    <h3>Systèmes d'exploitation:</h3>
    
    - Unix  <br />
    - MacOS <br />
    - Windows
    """, unsafe_allow_html=True)





def main():

    st.set_page_config(layout="wide", page_title="Twitter Sentiment Analysis App")
    choices = option_menu(
        menu_title=None,
        options=["Home", "Test Sentiment", "Visualization", "À propos"],
        icons=["house", "wechat", "graph-up-arrow", "info-circle"],
        menu_icon="cast",
        orientation="horizontal"
    )
    custom_css()
# added
    database_url = "mongodb://localhost:27017/"
    database_name = "TwitterSentimentAnalysis"
    collection_name = "TweetPrediction"
    collection = connect_to_mongodb(database_url, database_name, collection_name)
    # added

    if choices == "Home":
        home()
    elif choices == "À propos":
        about()
    elif choices == "Test Sentiment":
        test_sentiment()

    elif choices == "Visualization":
        visualization()


# added


if __name__ == '__main__':
    main()
