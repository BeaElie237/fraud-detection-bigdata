import pandas as pd
import numpy as np

def generate_fraud_data(n_rows=1000000):
    print(f"⏳ Génération de {n_rows} lignes avec 30 pays... Patientez.")
    
    # 1. Liste des 30 pays
    liste_pays = [
        'France', 'USA', 'Canada', 'Maroc', 'Allemagne', 'Japon', 'Royaume-Uni', 
        'Italie', 'Espagne', 'Bresil', 'Australie', 'Chine', 'Inde', 'Russie', 
        'Sénégal', 'Cameroun', 'Côte d\'Ivoire', 'Belgique', 'Suisse', 'Mexique',
        'Argentine', 'Egypte', 'Nigeria', 'Afrique du Sud', 'Portugal', 'Grèce',
        'Turquie', 'Corée du Sud', 'Thaïlande', 'Vietnam'
    ]
    
    # Création du mapping pour référence (utile pour ton detector plus tard)
    # France: 1, USA: 2, etc.
    mapping_pays = {pays: i+1 for i, pays in enumerate(liste_pays)}
    
    # 2. Génération aléatoire des noms de pays
    pays_noms = np.random.choice(liste_pays, n_rows)
    
    # 3. Conversion en codes numériques pour la logique de fraude
    code_pays = np.array([mapping_pays[p] for p in pays_noms])
    
    # 4. Montants et Heures
    montant = np.random.uniform(5, 50000, n_rows).round(2)
    heure = np.random.randint(0, 24, n_rows)
    
    # 5. Logique de fraude (Cible)
    # Fraude si : (Gros montant la nuit) OU (Pays index > 25 [Grèce à Vietnam] + Montant > 10k)
    is_fraud = np.where(
        ((montant > 15000) & (heure < 6)) | 
        ((code_pays > 25) & (montant > 10000)), 
        1, 0
    )
    
    # Bruit (11%)
    noise = np.random.choice([0, 1], size=n_rows, p=[0.89, 0.11])
    is_fraud = np.where(noise == 1, 1 - is_fraud, is_fraud)
    
    # 6. Création du DataFrame avec noms ET codes
    df = pd.DataFrame({
        'montant': montant,
        'heure': heure,
        'pays': pays_noms,        # Version texte pour Kafka/Processor
        'code_pays': code_pays,   # Version numérique pour l'entraînement
        'is_fraud': is_fraud
    })
    
    # Sauvegarde
    df.to_csv('donnees_fraude_1M.csv', index=False)
    
    # Affichage du mapping pour ton futur detector
    print("\n✅ Fichier généré. Voici ton dictionnaire à copier dans le Detector :")
    print(mapping_pays)

if __name__ == "__main__":
    generate_fraud_data(1000000)
