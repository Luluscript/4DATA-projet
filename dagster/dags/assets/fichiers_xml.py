from dagster import asset, AssetExecutionContext
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from lxml import etree

@asset
def fichiers_xml_action_b(context):
    url_base = "http://tipi.bison-fute.gouv.fr/bison-fute-restreint/publications-restreintes/grt/ACTION-B/"
    login = "publication-grt"
    password = "CheiPe7T"

    response = requests.get(url_base, auth=(login, password))
    if response.status_code != 200:
        raise Exception(f"Erreur {response.status_code} lors de la récupération de la page")

    soup = BeautifulSoup(response.text, "html.parser")
    liens_xml = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith(".xml")]
    context.log.info(f"{len(liens_xml)} fichiers XML trouvés.")

    dossier_data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'xml_action_b'))
    os.makedirs(dossier_data, exist_ok=True)

    downloaded_files = []

    for lien in liens_xml:
        url_fichier = url_base + lien
        nom_fichier = os.path.join(dossier_data, lien)

        fichier = requests.get(url_fichier, auth=(login, password))
        if fichier.status_code == 200:
            with open(nom_fichier, 'wb') as f:
                f.write(fichier.content)
            downloaded_files.append(nom_fichier)  # Ajouter le chemin du fichier à la liste    
            context.log.info(f"Fichier téléchargé : {nom_fichier}")
        else:
            context.log.warning(f"Échec : {url_fichier} - Code {fichier.status_code}")

    return downloaded_files

@asset(deps=["fichiers_xml_action_b"])
def parse_fichiers_xml(context, fichiers_xml_action_b):
    """Parse les fichiers XML Action B et extrait les données structurées au format DATEX II."""
    from lxml import etree
    import os
    import pandas as pd
    
    # Vérifier si la liste de fichiers est vide
    if not fichiers_xml_action_b:
        context.log.error("Aucun fichier XML à traiter. La liste de fichiers est vide.")
        return pd.DataFrame()
        
    context.log.info(f"Début du parsing de {len(fichiers_xml_action_b)} fichiers XML")
    
    # Structure pour stocker les données extraites
    situations_data = []
    
    # Définir les namespaces pour la recherche XPath
    namespaces = {
        'soap': 'http://www.w3.org/2003/05/soap-envelope',
        'datex': 'http://datex2.eu/schema/2/2_0',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    }
    
    # Traitement de chaque fichier XML
    for fichier_xml in fichiers_xml_action_b:
        try:
            context.log.info(f"Traitement du fichier: {os.path.basename(fichier_xml)}")
            file_size = os.path.getsize(fichier_xml)
            context.log.info(f"Taille du fichier: {file_size} octets")
            
            # Chargement du fichier XML
            tree = etree.parse(fichier_xml)
            root = tree.getroot()
            context.log.info(f"Fichier XML chargé. Root tag: {root.tag}")
            
            # Extraction des situations - méthode corrigée avec namespaces explicites
            soap_body = root.find(".//soap:Body", namespaces=namespaces)
            if soap_body is None:
                context.log.warning(f"Pas de soap:Body trouvé dans {fichier_xml}")
                continue
                
            # Recherche du modèle DATEX II (sans utiliser le namespace directement)
            d2_model = None
            for child in soap_body:
                if child.tag.endswith('d2LogicalModel'):
                    d2_model = child
                    break
            
            if d2_model is None:
                context.log.warning(f"Pas de d2LogicalModel trouvé dans {fichier_xml}")
                continue
            
            # Recherche de la publication
            payload_pub = None
            for elem in d2_model.iter():
                if elem.tag.endswith('payloadPublication'):
                    payload_pub = elem
                    break
                    
            if payload_pub is None:
                context.log.warning(f"Pas de payloadPublication trouvé dans {fichier_xml}")
                continue
                
            # Recherche des situations
            situation_elements = []
            for elem in payload_pub.iter():
                if elem.tag.endswith('situation'):
                    situation_elements.append(elem)
            
            context.log.info(f"Nombre de situations trouvées: {len(situation_elements)}")
            
            for situation in situation_elements:
                # Extraction des métadonnées de la situation
                situation_id = situation.get('id', '')
                situation_version = situation.get('version', '')
                
                # Recherche de l'élément overallSeverity
                severity = ''
                for elem in situation.iter():
                    if elem.tag.endswith('overallSeverity'):
                        severity = elem.text
                        break
                
                # Recherche de l'élément situationVersionTime
                version_time = ''
                for elem in situation.iter():
                    if elem.tag.endswith('situationVersionTime'):
                        version_time = elem.text
                        break
                
                # Recherche des situationRecord
                for record in situation.iter():
                    if record.tag.endswith('situationRecord'):
                        record_type = record.get('{http://www.w3.org/2001/XMLSchema-instance}type', '')
                        record_id = record.get('id', '')
                        record_version = record.get('version', '')
                        
                        # Extraire le commentaire/description
                        comment = ''
                        for elem in record.iter():
                            if elem.tag.endswith('value') and elem.getparent() is not None and elem.getparent().tag.endswith('values'):
                                parent_parent = elem.getparent().getparent()
                                if parent_parent is not None and parent_parent.tag.endswith('comment'):
                                    comment = elem.text
                                    break
                        
                        # Extraire le statut (actif/terminé)
                        is_end = False
                        for elem in record.iter():
                            if elem.tag.endswith('end'):
                                is_end = elem.text.lower() == 'true'
                                break
                                
                        # Extraire les dates
                        start_time = ''
                        for elem in record.iter():
                            if elem.tag.endswith('overallStartTime'):
                                start_time = elem.text
                                break
                                
                        # Extraire les informations de localisation
                        latitude_start = longitude_start = latitude_end = longitude_end = None
                        for elem in record.iter():
                            if elem.tag.endswith('latitude'):
                                parent = elem.getparent()
                                if parent is not None:
                                    if parent.tag.endswith('pointCoordinates'):
                                        gp_parent = parent.getparent()
                                        if gp_parent is not None:
                                            if gp_parent.tag.endswith('fromPoint'):
                                                latitude_start = elem.text
                                            elif gp_parent.tag.endswith('toPoint'):
                                                latitude_end = elem.text
                            elif elem.tag.endswith('longitude'):
                                parent = elem.getparent()
                                if parent is not None:
                                    if parent.tag.endswith('pointCoordinates'):
                                        gp_parent = parent.getparent()
                                        if gp_parent is not None:
                                            if gp_parent.tag.endswith('fromPoint'):
                                                longitude_start = elem.text
                                            elif gp_parent.tag.endswith('toPoint'):
                                                longitude_end = elem.text
                        
                        # Extraire les informations sur le trafic (pour AbnormalTraffic)
                        traffic_type = queue_length = ''
                        if record_type == 'AbnormalTraffic':
                            for elem in record.iter():
                                if elem.tag.endswith('abnormalTrafficType'):
                                    traffic_type = elem.text
                                elif elem.tag.endswith('queueLength'):
                                    queue_length = elem.text
                        
                        # Extraire les informations sur la route
                        road_number = ''
                        for elem in record.iter():
                            if elem.tag.endswith('roadNumber'):
                                road_number = elem.text
                                break
                        
                        # Collecter toutes les informations extraites
                        situations_data.append({
                            'SituationID': situation_id,
                            'SituationVersion': situation_version,
                            'Severite': severity,
                            'DateVersion': version_time,
                            'RecordType': record_type,
                            'RecordID': record_id,
                            'RecordVersion': record_version,
                            'Commentaire': comment,
                            'EstTermine': is_end,
                            'DateDebut': start_time,
                            'LatitudeDepart': latitude_start,
                            'LongitudeDepart': longitude_start,
                            'LatitudeArrivee': latitude_end,
                            'LongitudeArrivee': longitude_end,
                            'TypeTrafic': traffic_type,
                            'LongueurFile': queue_length,
                            'NumeroRoute': road_number,
                            'FichierSource': os.path.basename(fichier_xml)
                        })
                    
            context.log.info(f"Extraction terminée pour le fichier: {os.path.basename(fichier_xml)}")
                    
        except Exception as e:
            context.log.error(f"Erreur lors du traitement du fichier {fichier_xml}: {str(e)}")
            import traceback
            context.log.error(traceback.format_exc())
    
    # Créer un DataFrame à partir des données extraites
    if situations_data:
        df = pd.DataFrame(situations_data)
        context.log.info(f"Données extraites avec succès: {len(df)} situations")
        
        # Conversion des types de données
        # Convertir les coordonnées en nombres flottants
        for col in ['LatitudeDepart', 'LongitudeDepart', 'LatitudeArrivee', 'LongitudeArrivee']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convertir les longueurs de file en nombres entiers
        df['LongueurFile'] = pd.to_numeric(df['LongueurFile'], errors='coerce')
        
        return df
    else:
        context.log.warning("Aucune situation trouvée dans les fichiers XML analysés.")
        return pd.DataFrame()

@asset(deps=["parse_fichiers_xml"])
def nettoyer_donnees_xml(context, parse_fichiers_xml):
    """
    Asset pour nettoyer et transformer les données extraites des fichiers XML.
    """
    df = parse_fichiers_xml.copy()
    
    if df.empty:
        context.log.warning("DataFrame vide, aucun nettoyage effectué.")
        return df
    
    # Journalisation avant nettoyage
    context.log.info(f"Nettoyage du DataFrame: {len(df)} lignes avant traitement.")
    context.log.info(f"Colonnes présentes dans le DataFrame: {df.columns.tolist()}")
    
    # Vérifier les valeurs manquantes dans les coordonnées
    coord_columns = ['LatitudeDepart', 'LongitudeDepart', 'LatitudeArrivee', 'LongitudeArrivee']
    for col in coord_columns:
        if col in df.columns:
            na_count = df[col].isna().sum()
            context.log.info(f"Nombre de valeurs NaN dans {col}: {na_count} sur {len(df)}")
    
    # 1. Supprimer les doublons
    df_sans_doublons = df.drop_duplicates(subset=['SituationID'], keep='first')
    context.log.info(f"{len(df) - len(df_sans_doublons)} doublons supprimés.")
    df = df_sans_doublons
    
    # 2. Gérer les valeurs manquantes de façon plus souple
    # Au lieu de supprimer les lignes sans coordonnées, on peut:
    # a) Conserver au moins une paire de coordonnées (départ OU arrivée)
    has_any_coords = df['LatitudeDepart'].notna() | df['LatitudeArrivee'].notna()
    df_avec_coords = df[has_any_coords]
    context.log.info(f"Après filtrage souple sur les coordonnées: {len(df_avec_coords)} lignes.")
    
    # Si après ce filtre souple on a encore 0 ligne, on reprend le DataFrame original
    if len(df_avec_coords) == 0:
        context.log.warning("Aucune ligne avec coordonnées valides. Conservation du DataFrame original.")
        df_avec_coords = df
    
    df = df_avec_coords
    
    # 3. Standardiser certaines valeurs
    if 'RecordType' in df.columns:
        # Standardiser les types d'événements
        type_mapping = {
            'AbnormalTraffic': 'Trafic Anormal',
            'Accident': 'Accident',
            'RoadOrCarriagewayOrLaneManagement': 'Gestion de Voie',
            'RoadWorks': 'Travaux',
            'MaintenanceWorks': 'Travaux',
            'PlanningWorks': 'Travaux',
        }
        
        df['TypeStandardise'] = df['RecordType'].apply(lambda x: type_mapping.get(x, x) if pd.notna(x) else x)
        context.log.info("Colonne TypeStandardise ajoutée.")
    
    # 4. Convertir les dates en format datetime
    date_columns = ['DateVersion', 'DateDebut']
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                context.log.info(f"Colonne {col} convertie en datetime.")
            except Exception as e:
                context.log.warning(f"Erreur lors de la conversion de la colonne {col}: {e}")
    
    # 5. Ajouter colonnes calculées
    if 'DateDebut' in df.columns:
        try:
            df['Jour_Semaine'] = df['DateDebut'].dt.day_name()
            df['Heure_Debut'] = df['DateDebut'].dt.hour
            context.log.info("Colonnes Jour_Semaine et Heure_Debut ajoutées.")
        except Exception as e:
            context.log.warning(f"Erreur lors de la création des colonnes dérivées: {e}")
    
    # 6. Normaliser valeurs textuelles
    text_columns = ['Commentaire', 'NumeroRoute']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]
    
    # 7. Valeur booléenne plus explicite
    if 'EstTermine' in df.columns:
        df['Statut'] = df['EstTermine'].apply(lambda x: 'Terminé' if x else 'En cours')
        context.log.info("Colonne Statut ajoutée.")
    
    # 8. Tri du DataFrame
    if 'DateDebut' in df.columns and not df['DateDebut'].isna().all():
        df = df.sort_values(by='DateDebut', ascending=False)
        context.log.info("DataFrame trié par DateDebut.")
    else:
        context.log.warning("Impossible de trier par DateDebut (valeurs manquantes ou colonne absente).")
    
    context.log.info(f"Nettoyage terminé: {len(df)} lignes après traitement.")
    
    return df


# Pour l'export CSV
@asset(deps=["nettoyer_donnees_xml"])
def xml_data_csv(context, nettoyer_donnees_xml) -> str:
    """Exporte les données nettoyées des fichiers XML en format CSV."""
    context.log.info("Début de l'export des données XML vers CSV")
    
    # Récupérer le DataFrame des données nettoyées
    df = nettoyer_donnees_xml
    
    # Vérifier si le DataFrame est vide
    if df is None or df.empty:
        context.log.warning("Le DataFrame d'entrée est vide ou None")
        context.log.info("Export CSV terminé - fichier vide créé")
    else:
        context.log.info(f"Préparation de l'export de {len(df)} lignes et {len(df.columns)} colonnes")
        context.log.debug(f"Colonnes du DataFrame: {list(df.columns)}")
    
    try:
        # Créer le dossier de sortie s'il n'existe pas
        output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'processed'))
        context.log.info(f"Dossier de destination: {output_dir}")
        
        os.makedirs(output_dir, exist_ok=True)
        context.log.info(f"Dossier de destination vérifié/créé: {output_dir}")
        
        # Générer le nom du fichier CSV avec timestamp
        current_timestamp = pd.Timestamp.now().strftime('%Y%m%d')
        csv_path = os.path.join(output_dir, f"action_b_data_{current_timestamp}.csv")
        context.log.info(f"Chemin du fichier CSV à générer: {csv_path}")
        
        # Exporter en CSV
        context.log.info("Exportation des données en cours...")
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        # Vérifier que le fichier a bien été créé
        if os.path.exists(csv_path):
            file_size = os.path.getsize(csv_path)
            context.log.info(f"Fichier CSV créé avec succès: {csv_path} (taille: {file_size} octets)")
        else:
            context.log.error(f"Le fichier CSV n'a pas été créé: {csv_path}")
            raise FileNotFoundError(f"Échec de création du fichier CSV: {csv_path}")
        
        context.log.info("Export CSV terminé avec succès")
        return csv_path
    
    except Exception as e:
        context.log.error(f"Erreur lors de l'export CSV: {str(e)}")
        raise

@asset(
    deps=["xml_data_csv"],
    required_resource_keys={"mongodb"}
)
def xml_data_mongodb(context: AssetExecutionContext, xml_data_csv) -> None:
    """Importe les données du CSV dans MongoDB."""
    context.log.info("Début de l'importation des données vers MongoDB")
    
    # Accéder à la ressource mongodb via le context
    mongodb = context.resources.mongodb
    
    # Récupérer le chemin du CSV généré par l'asset précédent
    csv_path = xml_data_csv
    
    # Vérifier que le fichier existe
    if not os.path.exists(csv_path):
        context.log.error(f"ERREUR: Le fichier CSV n'existe pas: {csv_path}")
        raise FileNotFoundError(f"Le fichier CSV n'existe pas: {csv_path}")
    
    context.log.info(f"Fichier CSV trouvé: {csv_path}")
    
    try:
        # Charger les données du CSV
        context.log.info(f"Chargement des données depuis: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Vérifier si le DataFrame est vide
        if df.empty:
            context.log.warning(f"Le fichier CSV est vide: {csv_path}")
            context.log.info("Import MongoDB terminé - aucune donnée à importer")
            return None
        
        # Log des informations sur le DataFrame
        context.log.info(f"Données chargées: {len(df)} lignes, {len(df.columns)} colonnes")
        context.log.debug(f"Colonnes: {list(df.columns)}")
        
        # Ajouter une date d'importation
        df['import_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Convertir en dictionnaires pour MongoDB
        records = df.to_dict('records')
        context.log.info(f"Préparation de {len(records)} documents pour l'insertion")
        
        # Obtenir la collection MongoDB
        db = mongodb.get_database('supmap')
        collection = db['users']
        
        # Supprimer les données existantes
        context.log.info("Suppression des anciennes données de la collection...")
        delete_result = collection.delete_many({})
        context.log.info(f"Données supprimées: {delete_result.deleted_count} documents")
        
        # Insérer les nouvelles données
        if records:
            try:
                result = collection.insert_many(records)
                context.log.info(f"Données insérées dans MongoDB: {len(result.inserted_ids)} documents")
            except Exception as e:
                context.log.error(f"Erreur lors de l'insertion des données: {str(e)}")
                raise
        else:
            context.log.warning("Aucune donnée à insérer dans MongoDB")
        
        context.log.info("Import MongoDB terminé avec succès")
        
    except pd.errors.EmptyDataError:
        context.log.warning(f"Le fichier CSV est vide ou mal formaté: {csv_path}")
        context.log.info("Import MongoDB terminé - aucune donnée à importer")
        return None
    except Exception as e:
        context.log.error(f"Erreur lors du traitement du CSV: {str(e)}")
        raise