# sensors.py
from dagster import sensor, RunRequest
from jobs.job import mon_job
import os
from datetime import datetime

@sensor(job=mon_job)
def bison_fute_file_sensor(context):
    """Détecte les nouveaux fichiers XML dans le répertoire de données."""
    # Récupérez l'état précédent du sensor (fichiers déjà traités)
    sensor_state = context.cursor or "[]"
    processed_files = set(eval(sensor_state))
    
    # Chemin vers le répertoire des données XML - ajustez selon votre configuration
    xml_dir = "/opt/dagster/app/data"  # Modifiez ce chemin si nécessaire
    
    if not os.path.exists(xml_dir):
        context.log.info(f"Le répertoire {xml_dir} n'existe pas encore.")
        return None
        
    # Liste des fichiers XML actuellement présents
    current_files = set()
    for file in os.listdir(xml_dir):
        if file.endswith('.xml'):
            filepath = os.path.join(xml_dir, file)
            # On utilise le chemin complet + date de modification pour détecter les changements
            file_info = f"{filepath}_{os.path.getmtime(filepath)}"
            current_files.add(file_info)
    
    # Détection des nouveaux fichiers
    new_files = current_files - processed_files
    
    if new_files:
        context.log.info(f"Nouveaux fichiers détectés: {len(new_files)} fichier(s).")
        
        # Mise à jour de l'état du sensor
        all_files = processed_files.union(new_files)
        context.update_cursor(str(list(all_files)))
        
        # Déclenchement d'un run pour traiter les nouveaux fichiers
        return RunRequest(
            run_key=f"new_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            run_config={}
        )
    
    context.log.info("Aucun nouveau fichier détecté.")
    return None
