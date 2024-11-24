# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 14:10:06 2024

@author: 33672
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import time

# Créer une session Spark
spark = SparkSession.builder \
    .appName("KafkaReaderApp") \
    .master("local[*]") \
    .getOrCreate()

# Se connecter à Kafka et lire le stream
kafkaDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "project_topic") \
    .load()

# Convertir la valeur du message Kafka en chaîne de caractères
dataDF = kafkaDF.selectExpr("CAST(value AS STRING)")

# Définir l'ID du fichier de sortie
file_counter = 1

# Fonction de traitement de batch et de sauvegarde des données dans un fichier CSV
def write_to_csv(batch_df, batch_id):
    global file_counter
    try:
        output_path = f"/mnt/c/Users/Alex/Documents/Data_integration/Projet/Sources/{file_counter}.csv"
        batch_df.write.csv(output_path, header=True, mode="overwrite")
        print(f"Batch {batch_id} écrit avec succès dans {output_path}")
        file_counter += 1
    except Exception as e:
        print(f"Erreur lors de l'écriture du fichier : {e}")

# Sauvegarder les messages traités par lot dans un fichier CSV
query = dataDF.writeStream \
    .foreachBatch(write_to_csv) \
    .start()

# Attendre la terminaison du stream
query.awaitTermination()