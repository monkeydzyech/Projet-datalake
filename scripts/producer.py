from kafka import KafkaProducer
import json
import time

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Adresse du serveur Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation des données en JSON
)

# Chemin vers le fichier JSON (pub.json)
data_file = '/Users/monkeydziyech/Desktop/datalake_medaillon/data/pub.json'

# Chargement des données depuis le fichier JSON
try:
    with open(data_file, 'r') as file:
        data = json.load(file)
        print(f"Fichier {data_file} chargé avec succès. {len(data)} enregistrements trouvés.")
except FileNotFoundError:
    print(f"Erreur : Le fichier {data_file} est introuvable.")
    exit(1)
except json.JSONDecodeError as e:
    print(f"Erreur : Le fichier {data_file} contient un JSON invalide. {e}")
    exit(1)

# Nom du topic Kafka
topic_name = 'publicite_stream'  

# Envoi des données ligne par ligne avec une pause
try:
    for i, record in enumerate(data):
        # Envoi de chaque ligne au topic Kafka
        producer.send(topic_name, record)
        print(f"Message {i + 1} envoyé au topic {topic_name}: {record}")
        
        # Pause entre chaque message (1 seconde, ajustable)
        time.sleep(1)

except Exception as e:
    print(f"Erreur lors de l'envoi des messages au topic {topic_name} : {e}")
finally:
    # Fermer le producteur
    producer.close()
    print("Producteur Kafka fermé.")
