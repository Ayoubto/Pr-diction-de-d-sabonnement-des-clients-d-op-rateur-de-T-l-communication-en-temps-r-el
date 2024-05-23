from io import BytesIO
from flask import Flask, render_template, request, Response, jsonify
from pymongo import MongoClient
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as VirtualCanvas
import datetime
import json

app = Flask(__name__)

# Database configuration
cluster_uri = "mongodb://localhost:27017"
database_name = "ChurnPrediction"
collection_name_prediction = "Predictions"
collection_name_reservations = "Reservations"

try:
    client = MongoClient(cluster_uri)
    db = client[database_name]
    collection_prediction = db[collection_name_prediction]
    collection_reservations = db[collection_name_reservations]
    print("Connected to MongoDB successfully!")
except Exception as e:
    print("Error connecting to MongoDB:", e)

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/churn_by_state_bar_chart')
def churn_by_state_bar_chart():
    data_from_mongo = collection_prediction.aggregate([
        {"$group": {
            "_id": "$State",
            "churn_count": {"$sum": {"$cond": [{"$eq": ["$Churn_prediction", "True"]}, 1, 0]}}
        }},
        {"$sort": {"churn_count": -1}},
        {"$limit": 10}
    ])

    states = []
    churn_counts = []

    for entry in data_from_mongo:
        states.append(entry["_id"])
        churn_counts.append(entry["churn_count"])

    fig = Figure()
    ax1 = fig.subplots(1, 1)

    ax1.bar(states, churn_counts, color='lightblue', edgecolor='black', linewidth=1, alpha=0.7)
    ax1.set_xlabel('State', fontsize=12)
    ax1.set_ylabel('Nombre de résiliations', fontsize=12)
    ax1.set_title('Nombre de résiliations par State (Top 10)', fontsize=14)
    ax1.set_xticklabels(states, rotation=90)

    output = BytesIO()
    fig.savefig(output, format='png')
    output.seek(0)

    return Response(output.getvalue(), mimetype='image/png')

@app.route('/churn_heatmap_by_state')
def churn_heatmap_by_state():
    data_from_mongo = collection_prediction.aggregate([
        {"$group": {
            "_id": "$State",
            "churn_count": {"$sum": {"$cond": [{"$eq": ["$Churn_prediction", "True"]}, 1, 0]}}
        }},
        {"$sort": {"churn_count": -1}},
        {"$limit": 10}
    ])

    states = []
    churn_counts = []

    for entry in data_from_mongo:
        states.append(entry["_id"])
        churn_counts.append(entry["churn_count"])

    fig = Figure()
    ax1 = fig.subplots(1, 1)

    heatmap = ax1.imshow([churn_counts], cmap='coolwarm', aspect='auto')
    ax1.set_xticks(range(len(states)))
    ax1.set_xticklabels(states, rotation=90)
    ax1.set_yticks([])

    ax1.set_title('Nombre de résiliations par état (Top 10)', fontsize=14)
    fig.colorbar(heatmap, ax=ax1, label='Nombre de résiliations')

    output = BytesIO()
    fig.savefig(output, format='png')
    output.seek(0)

    return Response(output.getvalue(), mimetype='image/png')

@app.route('/probability_heatmap')
def probability_heatmap():
    data_from_mongo = list(collection_prediction.aggregate([
        {"$group": {
            "_id": "$State",
            "average_probability": {"$avg": {"$arrayElemAt": ["$probability", 1]}}
        }}
    ]))

    states = [entry["_id"] for entry in data_from_mongo]
    avg_probabilities = [entry["average_probability"] for entry in data_from_mongo]

    fig = Figure()
    ax1 = fig.subplots(1, 1)

    heatmap = ax1.imshow([avg_probabilities], cmap='coolwarm', aspect='auto')
    ax1.set_xticks(range(len(states)))
    ax1.set_xticklabels(states, rotation=90)
    ax1.set_yticks([])

    ax1.set_title('Probabilité moyenne par état', fontsize=14)
    fig.colorbar(heatmap, ax=ax1, label='Probabilité moyenne')

    output = BytesIO()
    fig.savefig(output, format='png')
    output.seek(0)

    return Response(output.getvalue(), mimetype='image/png')



@app.route('/prediction_scatter_plot')
def prediction_scatter_plot():
    data_from_mongo = list(collection_prediction.find({}, {"_id": 0, "Churn_prediction": 1, "Total day minutes": 1}))

    predictions = [1 if entry["Churn_prediction"] == "True" else 0 for entry in data_from_mongo]
    total_day_minutes = [float(entry["Total day minutes"]) for entry in data_from_mongo]

    fig = Figure()
    ax1 = fig.subplots(1, 1)

    scatter = ax1.scatter(total_day_minutes, predictions, c=predictions, cmap='viridis', alpha=0.7)
    ax1.set_xlabel('Minutes totales par jour', fontsize=12)
    ax1.set_ylabel('Prédiction', fontsize=12)
    ax1.set_title('Prédiction vs. Minutes totales par jour', fontsize=14)
    fig.colorbar(scatter, ax=ax1, label='Prédiction')

    output = BytesIO()
    fig.savefig(output, format='png')
    output.seek(0)

    return Response(output.getvalue(), mimetype='image/png')



@app.route('/prediction_chart')
def prediction_chart():
    data_from_mongo = collection_prediction.aggregate([
        {"$group": {"_id": "$Churn_prediction", "count": {"$sum": 1}}}
    ])

    labels = []
    values = []

    for entry in data_from_mongo:
        labels.append(f'Prédiction {entry["_id"]}')
        values.append(entry["count"])

    fig = Figure()
    ax1 = fig.subplots(1, 1)

    ax1.pie(values, labels=labels, autopct='%1.1f%%', startangle=90, colors=['lightblue', 'lightgreen'], wedgeprops={'edgecolor': 'black'})
    ax1.set_title('Distribution des prédictions (0 et 1)', fontsize=14)

    output = BytesIO()
    fig.savefig(output, format='png')
    output.seek(0)

    return Response(output.getvalue(), mimetype='image/png')

@app.route('/avg_day_minutes_by_state')
def avg_day_minutes_by_state():
    data_from_mongo = list(collection_prediction.aggregate([
        {"$group": {
            "_id": "$State",
            "avg_day_minutes": {"$avg": "$Total day minutes"}
        }},
        {"$sort": {"avg_day_minutes": -1}},
        {"$limit": 10}
    ]))

    states = []
    avg_day_minutes = []

    for entry in data_from_mongo:
        states.append(entry["_id"])
        avg_day_minutes.append(entry["avg_day_minutes"])

    fig = Figure(figsize=(15, 5))
    ax1 = fig.subplots(1, 1)

    ax1.bar(states, avg_day_minutes, color='blue', edgecolor='black', linewidth=1, alpha=0.7)
    ax1.set_xlabel('State', fontsize=12)
    ax1.set_ylabel('Moyenne des minutes de jour', fontsize=12)
    ax1.set_title('Moyenne des minutes de jour par state (Top 10)', fontsize=14)
    ax1.set_xticklabels(states, rotation=90)

    output = BytesIO()
    fig.savefig(output, format='png')
    output.seek(0)

    return Response(output.getvalue(), mimetype='image/png')
@app.route('/count_documents')
def count_documents():
    try:
        collection_names = db.list_collection_names()
        total_documents = 0
        churn_true_documents = 0
        churn_false_documents = 0

        for collection_name in collection_names:
            collection = db[collection_name]
            total_documents += collection.count_documents({})
            churn_true_documents += collection.count_documents({"Churn_prediction": "True"})
            churn_false_documents += collection.count_documents({"Churn_prediction": "False"})

        response = {
            'total_count': total_documents,
            'churn_true_count': churn_true_documents,
            'churn_false_count': churn_false_documents
        }
        return jsonify(response)
    except Exception as e:
        print("Error fetching document count:", e)
        return jsonify({'error': 'Failed to fetch document count'})

@app.route('/churn_predictions_true')
def churn_predictions_true():
    try:
        predictions = []
        collection_names = db.list_collection_names()

        for collection_name in collection_names:
            collection = db[collection_name]
            churn_true_docs = list(collection.find({"Churn_prediction": "True"}))
            predictions.extend(churn_true_docs)

        return jsonify(predictions)
    except Exception as e:
        print("Error fetching churn predictions where Churn_prediction is True:", e)
        return jsonify({'error': 'Failed to fetch churn predictions'})


if __name__ == '__main__':
    app.run(debug=True)
