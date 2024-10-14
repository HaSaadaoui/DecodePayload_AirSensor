from azure.cosmos import CosmosClient
import subprocess
import json
from datetime import datetime
import uuid  
from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.


# Fonction pour décoder une trame brute
def decode_frame(raw_data):
    command = [
        "python",
        "C:/Users/Codec-Report-Batch-Python-main/br_uncompress.py",  # Chemin vers votre script de décodage
        "-a",
        "3",           # paramètre pour ajuster selon votre script de décodage
        "1,10,7,T",    # T sera renommé plus tard en Température
        "2,100,6,H",   # H sera renommé plus tard en Humidité
        "3,10,6,CO2",  # Décodage du CO2
        "4,10,6,COV",  # Décodage des COV
        "-if",
        raw_data       # la trame brute récupérée
    ]

    try:
        # Exécuter la commande de décodage
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return json.loads(result.stdout.strip())  # Retourner le résultat décodé en format JSON
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors du décodage : {e}")
        print("Sortie standard :", e.stdout)
        print("Sortie d'erreur :", e.stderr)
        return None

# Connexion à Cosmos DB
CONNECTION_STRING = os.getenv('CLE_COSMOS')
client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')

# Conteneur source (données brutes)
sensor_container = database.get_container_client('SensorData')

# Requête SQL pour récupérer les données brutes du capteur 'Air_05-01' pour une journée spécifique
query = 'SELECT c.raw, c.ReceivedTimeStamp FROM c WHERE c.device = "Air_05-01" AND STARTSWITH(c.ReceivedTimeStamp, "2024-10-10")'

# Exécution de la requête pour obtenir les éléments
items = list(sensor_container.query_items(query=query, enable_cross_partition_query=True))

# Affichage du nombre de trames trouvées
print(f"Nombre de payloads trouvés : {len(items)}")

# Conteneur de destination pour les données nettoyées et décodées
destination_container = database.get_container_client('DataCleanCosmos')

# Parcourir et traiter chaque trame brute
for item in items:
    raw_data = item['raw']
    print(f"Trame brute : {raw_data}")

    # Décoder la trame brute
    decoded_result = decode_frame(raw_data)

    if decoded_result:
        print("Résultat décodé avant modification des labels :")
        print(json.dumps(decoded_result, indent=4))

        # Préparer une nouvelle liste pour les données nettoyées
        cleaned_dataset = []

        # Parcourir le dataset 
        for data_point in decoded_result.get('dataset', []):
            # Suppression du champ data_relative_timestamp
            if 'data_relative_timestamp' in data_point:
                del data_point['data_relative_timestamp']  
            # Renommer les labels et ajuster les valeurs
            if data_point['data']['label_name'] == 'T':
                data_point['data']['label_name'] = 'Température'
                data_point['data']['value'] = round(data_point['data']['value'] * 0.01, 1)

            elif data_point['data']['label_name'] == 'H':
                data_point['data']['label_name'] = 'Humidité'
                data_point['data']['value'] = data_point['data']['value'] * 0.01

            # Renommer data_absolute_timestamp
            data_point['TimeStamp'] = data_point.pop('data_absolute_timestamp', '')  
            
            # Ajouter le data_point nettoyé à la nouvelle liste
            cleaned_dataset.append(data_point)

        # Mettre à jour le résultat décodé avec le dataset nettoyé
        decoded_result['dataset'] = cleaned_dataset

        # Ajouter les colonnes 'id' et 'Type'
        #decoded_result['id'] = item.get('ReceivedTimeStamp', datetime.now().isoformat())  # ou batch_relative_timestamp si disponible
        decoded_result['id'] = str(uuid.uuid4())  # Génère un UUID comme identifiant
        decoded_result['Type'] = "Air_05-01"

        # Afficher le document avant l'insertion
        print(f"Document à insérer : {json.dumps(decoded_result, indent=4)}")

        # Insérer le document dans le conteneur de destination
        destination_container.upsert_item(decoded_result)
        print(f"Données insérées dans le conteneur 'DataCleanCosmos': {decoded_result}")
    else:
        print("Échec du décodage pour cette trame.")
