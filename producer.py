# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 21:13:13 2024

@author: 33672
"""

import pandas as pd
import time
from kafka import KafkaProducer

# Configurations kafka 
KafkaBroker="localhost:9092"
Topic="project_topic"
# Chargement du fichier CSV
df = pd.read_excel("/mnt/c/Users/Alex/Documents/Data_integration/Projet/Sources/LTM_Data_2022_8_1.xlsx")
numeric_columns = df.select_dtypes(include=['number']).columns
non_numeric_columns = df.select_dtypes(exclude=['number']).columns

df[numeric_columns] = df[numeric_columns].fillna(0) 
df[non_numeric_columns] = df[non_numeric_columns].fillna("NA")

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KafkaBroker,
    value_serializer=lambda v: v.encode("utf-8"),
    request_timeout_ms=120000,
    api_version=(2, 5, 0)
)


# Fonction pour envoyer des messages par lot
def send_to_kafka(data_batch):
    for record in data_batch:
        try:
            # Envoi du message avec callback pour confirmer l'envoi
            producer.send(Topic, value=record).add_callback(on_send_success).add_errback(on_send_error)
        except Exception as e:
            print(f"Erreur lors de l'envoi du message '{record}' : {e}")

# Callback de confirmation
def on_send_success(record_metadata):
    print(f"Message envoyé avec succès à {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

# Callback en cas d'erreur
def on_send_error(exception):
    print(f"Erreur lors de l'envoi du message : {exception}")

# Lecture par lots de 10 lignes
batch_size = 10
for start in range(0, len(df), batch_size):
    # Extraire un lot
    batch = df.iloc[start:start + batch_size]
    data = batch.to_csv(index=False, header=False).strip().split("\n")
    data_batch = [record for record in data if record.strip()]
    # Envoyer au topic Kafka
    send_to_kafka(data_batch)
    # Pause de 10 secondes
    time.sleep(10)

# Fermer le producteur
producer.close()
