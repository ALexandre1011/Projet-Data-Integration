# Projet-Data-Integration

Ce jeu de données contient les informations sur la composition chimique en surface des eaux de 1980 à 2020. Il est mis à jour chaque année. Les données sont collectées dans quatre régions de l'Est des États-Unis (ruisseaux de Virginie, lacs et ruisseaux de New York, lacs du Maine et du New Hampshire, et lacs du Vermont). Ces données sont utilisées pour évaluer la réponse de l'écosystème aquatique aux changements de dépôt de soufre et d'azote. Les informations de ce jeu de données se trouvent dans la section Références de Data.gov : https://catalog.data.gov/dataset/epa-long-term-monitoring-of-acidified-surface-waters

L'objectif de ce projet est d'ingérer en temps réel les données de ce fichier à l'aide de Kafka. Pour cela, un script python (producer.py) envoie les données de ce fichier à Kafka par lot de 10. Ensuite un autre script python (consumer.py)  récupère ce flux et écrit pour chaque lot reçu un dataset.
