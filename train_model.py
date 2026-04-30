import xgboost as xgb
import pandas as pd
import mlflow.xgboost
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, classification_report
import time
import os

# Configuration MLflow
mlflow.set_experiment("Fraud_Detection_30_Pays")

def train_model():
    file_path = 'donnees_fraude_1M.csv'
    
    if not os.path.exists(file_path):
        print(f"❌ Erreur : Le fichier {file_path} n'existe pas. Génère-le d'abord !")
        return

    print(f"📂 Chargement de {file_path}...")
    
    # On charge tout, mais on ne garde que les colonnes numériques pour XGBoost
    df = pd.read_csv(file_path)

    # 1. Sélection des Features et de la Cible
    # IMPORTANT : On exclut 'pays' (texte) car XGBoost ne veut que des chiffres
    features = ['montant', 'heure', 'code_pays']
    X = df[features]
    y = df['is_fraud']
    
    # Split 80% train / 20% test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print(f"🧠 Entraînement sur {len(X_train)} lignes...")
    
    with mlflow.start_run(run_name="XGBoost_Final_30_Pays"):
        # Paramètres optimisés pour la vitesse (Histogramme)
        model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            tree_method='hist', 
            device="cpu", # Ou "cuda" si tu as une carte NVIDIA
            n_jobs=-1
        )

        start_time = time.time()
        model.fit(X_train, y_train)
        duration = time.time() - start_time
        
        # 2. Évaluation
        y_pred = model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        print(f"✅ Entraînement terminé en {duration:.2f}s")
        print(f"📊 Précision (Accuracy): {acc:.4f}")
        print(f"📊 F1-Score: {f1:.4f}")
        print("\n--- Rapport détaillé ---\n", classification_report(y_test, y_pred))

        # 3. Log dans MLflow
        mlflow.log_params(model.get_params())
        mlflow.log_metrics({"accuracy": acc, "f1_score": f1})
        
        # 4. Sauvegarde du modèle
        # Format JSON recommandé pour la portabilité vers Spark
        model.save_model("fraude_model.json")
        print("\n💾 Modèle sauvegardé avec succès : 'fraude_model.json'")

if __name__ == "__main__":
    train_model()
