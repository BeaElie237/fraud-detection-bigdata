import json
import time
import random
from kafka import KafkaProducer

# Configuration du Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Liste étendue à 30 pays pour correspondre au dataset d'entraînement
pays_dict = {
    1: 'France', 2: 'Cameroun', 3: 'USA', 4: 'Belgique', 5: 'Canada',
    6: 'Sénégal', 7: 'Allemagne', 8: 'Espagne', 9: 'Italie', 10: 'Japon',
    11: 'Maroc', 12: 'Côte d''Ivoire', 13: 'Brésil', 14: 'Royaume-Uni', 15: 'Suisse',
    16: 'Nigeria', 17: 'Gabon', 18: 'Chine', 19: 'Inde', 20: 'Australie',
    21: 'Mali', 22: 'Togo', 23: 'Bénin', 24: 'Congo', 25: 'Tunisie',
    26: 'Russie', 27: 'Mexique', 28: 'Argentine', 29: 'Portugal', 30: 'Grèce'
}

print(f"📡 Flux actif : Simulation de transactions sur {len(pays_dict)} pays...")

try:
    while True:
        # Sélection aléatoire d'un code pays
        code_p = random.randint(1, 30)
        nom_p = pays_dict[code_p]
        
        data = {
            'id': random.randint(10000, 99999),
            'montant': round(random.uniform(10, 25000), 2), # Montants variés
            'pays': nom_p,
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Envoi vers le topic Kafka
        producer.send('transactions_brutes', data)
        
        # Feedback console pour ta démo
        print(f"📥 [{data['timestamp']}] ID: {data['id']} | {data['montant']}€ | Pays: {nom_p}")
        
        # Pause de 0.5s pour une démo dynamique mais lisible
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n🛑 Simulation interrompue.")