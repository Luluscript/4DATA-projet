from dagster import asset, AssetExecutionContext, MetadataValue
import matplotlib.pyplot as plt
import pandas as pd
import os
import io
import base64
from typing import Dict, Any, Optional
import numpy as np
import constants

@asset(deps=["nettoyer_donnees_xml"])
def visualisation_trafic(context: AssetExecutionContext, nettoyer_donnees_xml) -> Dict[str, str]:
    """
    Crée des visualisations sur les données de trafic extraites des fichiers XML.
    """
    context.log.info("Création des visualisations des données de trafic")
    
    # Récupérer le DataFrame nettoyé
    df = nettoyer_donnees_xml
    
    # S'assurer que le répertoire existe
    os.makedirs(os.path.dirname(constants.TYPES_EVENEMENTS_PATH), exist_ok=True)
    
    # Vérifier que le DataFrame n'est pas vide
    if df is None or df.empty:
        context.log.warning("Aucune donnée disponible pour la visualisation")
        # Créer un graphique vide avec un message
        plt.figure(figsize=(10, 6))
        plt.text(0.5, 0.5, "Aucune donnée disponible", 
                horizontalalignment='center', verticalalignment='center',
                fontsize=14)
        plt.title("Visualisation du Trafic")
        
    else:
        context.log.info(f"Création de visualisations à partir de {len(df)} enregistrements")
        
        # 1. Compter par type d'événement/record (RecordType)
        plt.figure(figsize=(12, 6))
        
        # Vérifier la présence de la colonne RecordType
        if 'RecordType' in df.columns:
            # Compter les occurrences
            type_counts = df['RecordType'].value_counts().sort_values(ascending=False)
            
            # Créer le graphique à barres horizontales
            bars = plt.barh(type_counts.index, type_counts.values, color='skyblue')
            
            # Ajouter les valeurs à côté des barres
            for i, (index, value) in enumerate(zip(type_counts.index, type_counts.values)):
                plt.text(value + 0.5, i, str(value), va='center')
                
            plt.title('Répartition des Types d\'Événements', fontsize=14)
            plt.xlabel('Nombre d\'Occurrences', fontsize=12)
            plt.ylabel('Type d\'Événement', fontsize=12)
            plt.tight_layout()
        else:
            plt.text(0.5, 0.5, "Colonne 'RecordType' non disponible", 
                    horizontalalignment='center', verticalalignment='center',
                    fontsize=14)
            plt.title("Types d'Événements")

    # Sauvegarder le graphique
    plt.savefig(constants.TYPES_EVENEMENTS_PATH, bbox_inches='tight')
    plt.close()
    context.log.info(f"Graphique des types d'événements créé: {constants.TYPES_EVENEMENTS_PATH}")
    
    # Graphique 2 - Analyse temporelle si des dates sont disponibles
    graph2_path = None
    if not df.empty and 'DateDebut' in df.columns:
        try:
            # Convertir en datetime si ce n'est pas déjà fait
            if not pd.api.types.is_datetime64_dtype(df['DateDebut']):
                df['DateDebut'] = pd.to_datetime(df['DateDebut'], errors='coerce')
            
            # Si nous avons des dates valides
            if not df['DateDebut'].isna().all():
                # Extraire le jour de la semaine et compter
                df['JourSemaine'] = df['DateDebut'].dt.day_name()
                
                # Définir l'ordre des jours
                jours_ordre = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                
                # Créer un DataFrame pour le comptage avec l'ordre correct
                jour_counts = df['JourSemaine'].value_counts().reindex(jours_ordre, fill_value=0)
                
                plt.figure(figsize=(12, 6))
                colors = plt.cm.viridis(np.linspace(0, 0.8, len(jour_counts)))
                bars = plt.bar(jour_counts.index, jour_counts.values, color=colors)
                
                # Ajouter les valeurs au-dessus des barres
                for bar in bars:
                    height = bar.get_height()
                    plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                            f'{int(height)}', ha='center', va='bottom')
                
                plt.title('Répartition des Événements par Jour de la Semaine', fontsize=14)
                plt.xlabel('Jour de la Semaine', fontsize=12)
                plt.ylabel('Nombre d\'Événements', fontsize=12)
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                # Sauvegarder le graphique
                plt.savefig(constants.EVENEMENTS_PAR_JOUR_PATH, bbox_inches='tight')
                plt.close()
                context.log.info(f"Graphique des événements par jour créé: {constants.EVENEMENTS_PAR_JOUR_PATH}")
                graph2_path = constants.EVENEMENTS_PAR_JOUR_PATH
            else:
                context.log.warning("Aucune date valide dans la colonne 'DateDebut'")
        except Exception as e:
            context.log.error(f"Erreur lors de la création du graphique temporel: {str(e)}")
    else:
        context.log.info("Colonne 'DateDebut' non disponible pour l'analyse temporelle")
    
    # Graphique 3 - Carte de sévérité si disponible
    graph3_path = None
    if not df.empty and 'Severite' in df.columns and df['Severite'].notna().any():
        try:
            plt.figure(figsize=(10, 6))
            
            # Compter les occurrences par niveau de sévérité
            severity_counts = df['Severite'].value_counts().sort_index()
            
            # Définir une palette de couleurs qui va du jaune au rouge
            colors = plt.cm.YlOrRd(np.linspace(0.3, 0.9, len(severity_counts)))
            
            # Créer le graphique
            wedges, texts, autotexts = plt.pie(
                severity_counts.values, 
                labels=severity_counts.index,
                autopct='%1.1f%%',
                colors=colors,
                startangle=90,
                shadow=True,
            )
            
            # Embellir les textes
            for text in texts:
                text.set_fontsize(12)
            for autotext in autotexts:
                autotext.set_fontsize(10)
                autotext.set_color('white')
            
            plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
            plt.title('Répartition des Événements par Niveau de Sévérité', fontsize=14)
            
            # Sauvegarder le graphique
            plt.savefig(constants.SEVERITE_EVENEMENTS_PATH, bbox_inches='tight')
            plt.close()
            context.log.info(f"Graphique de sévérité créé: {constants.SEVERITE_EVENEMENTS_PATH}")
            graph3_path = constants.SEVERITE_EVENEMENTS_PATH
        except Exception as e:
            context.log.error(f"Erreur lors de la création du graphique de sévérité: {str(e)}")
    else:
        context.log.info("Données de sévérité non disponibles")
    
    # Créer un tableau HTML qui regroupe toutes les visualisations
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Tableau de Bord - Trafic et Événements</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
            }
            h1 {
                color: #333;
                text-align: center;
                padding-bottom: 10px;
                border-bottom: 2px solid #ddd;
            }
            .dashboard {
                display: flex;
                flex-wrap: wrap;
                justify-content: center;
                gap: 20px;
                margin-top: 20px;
            }
            .graph-container {
                background: white;
                border-radius: 8px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                padding: 15px;
                margin-bottom: 20px;
                width: 100%;
                max-width: 800px;
            }
            .graph-title {
                font-size: 18px;
                font-weight: bold;
                color: #444;
                margin-bottom: 15px;
                text-align: center;
            }
            .graph {
                text-align: center;
            }
            img {
                max-width: 100%;
                height: auto;
                border-radius: 4px;
            }
            .timestamp {
                text-align: center;
                margin-top: 30px;
                color: #888;
                font-size: 12px;
            }
        </style>
    </head>
    <body>
        <h1>Tableau de Bord - Analyse du Trafic et des Événements</h1>
        
        <div class="dashboard">
    """
    
    # Ajouter le premier graphique
    html_content += f"""
            <div class="graph-container">
                <div class="graph-title">Répartition des Types d'Événements</div>
                <div class="graph">
                    <img src="types_evenements.png" alt="Types d'événements">
                </div>
            </div>
    """
    
    # Ajouter le deuxième graphique s'il existe
    if graph2_path:
        html_content += f"""
            <div class="graph-container">
                <div class="graph-title">Événements par Jour de la Semaine</div>
                <div class="graph">
                    <img src="evenements_par_jour.png" alt="Événements par jour">
                </div>
            </div>
        """
    
    # Ajouter le troisième graphique s'il existe
    if graph3_path:
        html_content += f"""
            <div class="graph-container">
                <div class="graph-title">Répartition par Niveau de Sévérité</div>
                <div class="graph">
                    <img src="severite_evenements.png" alt="Niveaux de sévérité">
                </div>
            </div>
        """
    
    # Fermer les balises HTML
    html_content += f"""
        </div>
        
        <div class="timestamp">
            Généré le : {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </body>
    </html>
    """
    
    # Sauvegarder le HTML
    with open(constants.DASHBOARD_HTML_PATH, "w") as f:
        f.write(html_content)
    context.log.info(f"Dashboard HTML créé: {constants.DASHBOARD_HTML_PATH}")
    
    # Vérification de l'existence des fichiers (pour le débogage)
    for path in [constants.TYPES_EVENEMENTS_PATH, constants.EVENEMENTS_PAR_JOUR_PATH, 
                constants.SEVERITE_EVENEMENTS_PATH, constants.DASHBOARD_HTML_PATH]:
        if os.path.exists(path):
            context.log.info(f"Fichier vérifié et existe à: {path}")
            context.log.info(f"Taille du fichier: {os.path.getsize(path)} bytes")
        else:
            context.log.info(f"ATTENTION: Fichier introuvable à: {path}")
    
    # Encodage des images en base64 pour les métadonnées
    def image_to_base64(image_path):
        if image_path and os.path.exists(image_path):
            with open(image_path, "rb") as img_file:
                return base64.b64encode(img_file.read()).decode('utf-8')
        return None
    
    # Créer des métadonnées enrichies pour Dagit
    metadata = {
        "types_evenements_path": constants.TYPES_EVENEMENTS_PATH,
        "evenements_par_jour_path": constants.EVENEMENTS_PAR_JOUR_PATH if graph2_path else "",
        "severite_evenements_path": constants.SEVERITE_EVENEMENTS_PATH if graph3_path else "",
        "dashboard_html_path": constants.DASHBOARD_HTML_PATH
    }
    
    # Ajouter des images inline si possible
    type_events_b64 = image_to_base64(constants.TYPES_EVENEMENTS_PATH)
    if type_events_b64:
        metadata["types_evenements_img"] = MetadataValue.md(f"![Types d'événements](data:image/png;base64,{type_events_b64})")
    
    if graph2_path:
        events_day_b64 = image_to_base64(constants.EVENEMENTS_PAR_JOUR_PATH)
        if events_day_b64:
            metadata["evenements_jour_img"] = MetadataValue.md(f"![Événements par jour](data:image/png;base64,{events_day_b64})")
    
    if graph3_path:
        severity_b64 = image_to_base64(constants.SEVERITE_EVENEMENTS_PATH)
        if severity_b64:
            metadata["severite_img"] = MetadataValue.md(f"![Sévérité](data:image/png;base64,{severity_b64})")
    
    # Ajouter le chemin du dossier de visualisation
    metadata["viz_directory"] = MetadataValue.path(os.path.dirname(constants.TYPES_EVENEMENTS_PATH))
    
    # Ajouter les métadonnées à l'asset
    context.add_output_metadata(metadata)
    
    # Retourner les chemins des fichiers générés
    return {
        "types_evenements_path": constants.TYPES_EVENEMENTS_PATH,
        "evenements_par_jour_path": constants.EVENEMENTS_PAR_JOUR_PATH if graph2_path else "",
        "severite_evenements_path": constants.SEVERITE_EVENEMENTS_PATH if graph3_path else "",
        "dashboard_html_path": constants.DASHBOARD_HTML_PATH
    }
