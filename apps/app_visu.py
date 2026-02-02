import streamlit as st
from pymongo import MongoClient
import pandas as pd
import time

# Configuration de la page
st.set_page_config(page_title="Dashboard Corridor Mali", layout="wide")

st.title("实时 | Dashboard Analyse de Sentiment (Lambda Architecture)")
st.markdown("Ce dashboard consomme les données traitées par Spark et stockées dans MongoDB.")

# Connexion à MongoDB (utilise le nom du service défini dans docker-compose)
@st.cache_resource
def get_database():
    client = MongoClient("mongodb://mongodb:27017/")
    return client.corridor_db

db = get_database()
collection = db.results

# Création d'un espace vide pour le rafraîchissement
placeholder = st.empty()

while True:
    # Récupération des données depuis MongoDB
    data = list(collection.find())
    
    with placeholder.container():
        if data:
            df = pd.DataFrame(data)

            # --- Section KPIs ---
            kpi1, kpi2, kpi3 = st.columns(3)
            
            with kpi1:
                st.metric("Total Avis Traités", len(df))
            
            with kpi2:
                sat_count = len(df[df['sentiment'] == 'Satisfait'])
                st.metric("Clients Satisfaits", sat_count, delta=f"{sat_count}")
                
            with kpi3:
                insat_count = len(df[df['sentiment'] == 'Insatisfait'])
                st.metric("Clients Insatisfaits", insat_count, delta=f"-{insat_count}", delta_color="inverse")

            st.divider()

            # --- Section Graphiques ---
            col_left, col_right = st.columns(2)

            with col_left:
                st.subheader("Répartition des Sentiments")
                sentiment_counts = df['sentiment'].value_counts()
                st.bar_chart(sentiment_counts)

            with col_right:
                st.subheader("Top 5 Stations par Note Moyenne")
                station_stats = df.groupby('station')['note'].mean().sort_values(ascending=False).head(5)
                st.line_chart(station_stats)

            # Affichage des derniers messages reçus
            st.subheader("Derniers avis reçus")
            st.dataframe(df[['station', 'service', 'note', 'sentiment']].tail(10), use_container_width=True)

        else:
            st.warning("Aucune donnée trouvée dans MongoDB. Vérifiez que votre job Spark tourne bien.")
            st.info("Astuce : Lancez votre producteur Kafka pour générer des avis.")

    # Pause de 2 secondes avant la prochaine mise à jour
    time.sleep(2)