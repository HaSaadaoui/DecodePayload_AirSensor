from azure.cosmos import CosmosClient

# Connexion Cosmos DB
CONNECTION_STRING = 'AccountEndpoint=https://cosmosdb-paris-chateaudun.documents.azure.com:443/;AccountKey=WQjxv3e4hNIOv2I91oLlTObR0PFVSSk3iB7goLJAMLwMEVCzgl98fmueRQEkwxBvqgmyBUmpXnfFACDbeUnUpg==;'
client = CosmosClient.from_connection_string(CONNECTION_STRING)
database = client.get_database_client('SmartBuildingDB-Paris-Chateaudun')
container = database.get_container_client('SensorData')

# Requête SQL pour récupérer les données du champ 'raw' pour le capteur 'Air_05-01'
#query = 'SELECT c.raw FROM c WHERE c.device = "Air_05-01"'

query = 'SELECT c.raw, c.ReceivedTimeStamp FROM c WHERE c.device = "Air_05-01" AND STARTSWITH(c.ReceivedTimeStamp, "2024-10-10")'



# Exécution de la requête
items = list(container.query_items(query=query, enable_cross_partition_query=True))  

 # Affichage du nombre de trames trouvées
print(f"Nombre de payloads trouvés : {len(items)}")

# Affichage des résultats
for item in items:
    print(item['raw'])
