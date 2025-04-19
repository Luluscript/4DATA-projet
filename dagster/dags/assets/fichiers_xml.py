from dagster import asset
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
    # Définir la structure des données
    data = {
        'ID': None,
        'Severite': None,
        'Type': None,
        'SousType': None,
        'Statut': None,
        'Direction': None,
        'Latitude': None,
        'Longitude': None,
        'Lieu': None,
        'Voie': None,
        'Debut': None,
        'Fin': None,
        'Duree_Heures': None
    }

    all_data = []  # Liste pour stocker tous les DataFrames

    for fichier_xml in fichiers_xml_action_b:
        try:
            # Charger le fichier XML
            tree = etree.parse(fichier_xml)
            root = tree.getroot()

            # Définir les namespaces pour les requêtes XPath
            namespaces = {
                'd2': 'http://www.w3.org/2001/XMLSchema-instance',  # Ajoute le bon namespace pour tes données
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }

            # Initialiser les données pour chaque enregistrement
            row_data = data.copy()

            situation = root.find('.//d2:situation', namespaces)
            if situation is not None:
                row_data['ID'] = situation.get('id')
                
                severity = situation.find('d2:overallSeverity', namespaces)
                if severity is not None:
                    row_data['Severite'] = severity.text
                
                # Extraire le premier situationRecord
                record = situation.find('.//d2:situationRecord', namespaces)
                if record is not None:
                    # Extraire le type général de l'événement
                    record_type = record.get('{' + namespaces['xsi'] + '}type')
                    row_data['Type'] = record_type
                    
                    # Extraire le sous-type spécifique
                    for element in record:
                        tag = element.tag.split('}')[-1]
                        if 'Type' in tag and tag != 'situationType':
                            row_data['SousType'] = element.text
                            break
                    
                    # Extraire le statut
                    mobility = record.find('.//d2:mobilityType', namespaces)
                    if mobility is not None:
                        row_data['Statut'] = f"Véhicule {mobility.text}"
                    else:
                        compliance = record.find('d2:complianceOption', namespaces)
                        if compliance is not None:
                            row_data['Statut'] = f"Mesure {compliance.text}"
                        else:
                            if 'works' in str(record_type).lower() or 'maintenance' in str(record_type).lower():
                                row_data['Statut'] = "Travaux"
                            else:
                                impact = record.find('.//d2:trafficConstrictionType', namespaces)
                                if impact is not None:
                                    row_data['Statut'] = f"Impact {impact.text}"
                                else:
                                    row_data['Statut'] = "En cours"
                    
                    # Extraire les informations de localisation
                    location = record.find('.//d2:tpegDirection', namespaces)
                    if location is not None:
                        direction_map = {'southBound': 'Sud', 'northBound': 'Nord', 
                                        'eastBound': 'Est', 'westBound': 'Ouest'}
                        direction_value = location.text
                        row_data['Direction'] = direction_map.get(direction_value, direction_value)
                    
                    # Extraire les coordonnées
                    from_point = record.find('.//d2:from/d2:pointCoordinates', namespaces)
                    if from_point is not None:
                        lat = from_point.find('d2:latitude', namespaces)
                        lon = from_point.find('d2:longitude', namespaces)
                        if lat is not None and lon is not None:
                            row_data['Latitude'] = float(lat.text)
                            row_data['Longitude'] = float(lon.text)
                    else:
                        coords = record.find('.//d2:pointCoordinates', namespaces)
                        if coords is not None:
                            lat = coords.find('d2:latitude', namespaces)
                            lon = coords.find('d2:longitude', namespaces)
                            if lat is not None and lon is not None:
                                row_data['Latitude'] = float(lat.text)
                                row_data['Longitude'] = float(lon.text)

                    # Extraire le lieu et la voie
                    from_names = record.findall('.//d2:from/d2:name', namespaces)
                    if from_names:
                        for name in from_names:
                            descriptor_type = name.find('d2:tpegOtherPointDescriptorType', namespaces)
                            if descriptor_type is not None:
                                if descriptor_type.text == 'townName':
                                    value = name.find('.//d2:value', namespaces)
                                    if value is not None:
                                        row_data['Lieu'] = value.text
                                elif descriptor_type.text == 'linkName':
                                    value = name.find('.//d2:value', namespaces)
                                    if value is not None:
                                        row_data['Voie'] = value.text
                    else:
                        names = record.findall('.//d2:name', namespaces)
                        for name in names:
                            descriptor_type = name.find('d2:tpegOtherPointDescriptorType', namespaces)
                            if descriptor_type is not None:
                                if descriptor_type.text == 'townName':
                                    value = name.find('.//d2:value', namespaces)
                                    if value is not None:
                                        row_data['Lieu'] = value.text
                                elif descriptor_type.text == 'linkName':
                                    value = name.find('.//d2:value', namespaces)
                                    if value is not None:
                                        row_data['Voie'] = value.text
                    
                    # Extraire la validité temporelle
                    validity = record.find('.//d2:validityTimeSpecification', namespaces)
                    if validity is not None:
                        start_time = validity.find('d2:overallStartTime', namespaces)
                        end_time = validity.find('d2:overallEndTime', namespaces)
                        
                        if start_time is not None:
                            dt = datetime.fromisoformat(start_time.text.replace('Z', '+00:00'))
                            row_data['Debut'] = dt.strftime('%d/%m/%Y %H:%M:%S')
                        
                        if end_time is not None:
                            dt = datetime.fromisoformat(end_time.text.replace('Z', '+00:00'))
                            row_data['Fin'] = dt.strftime('%d/%m/%Y %H:%M:%S')
                            
                            # Calculer la durée en heures
                            if row_data['Debut'] is not None:
                                start_dt = datetime.fromisoformat(start_time.text.replace('Z', '+00:00'))
                                end_dt = datetime.fromisoformat(end_time.text.replace('Z', '+00:00'))
                                duration = (end_dt - start_dt).total_seconds() / 3600
                                row_data['Duree_Heures'] = round(duration, 2)

                    # Ajouter les données extraites à la liste
                    all_data.append(row_data)

        except Exception as e:
            context.log.warning(f"Erreur de parsing pour {fichier_xml} : {str(e)}")

    # Convertir la liste de données en DataFrame
    if all_data:
        df = pd.DataFrame(all_data)
        context.log.info(f"Total de {len(df)} lignes après traitement.")
    else:
        df = pd.DataFrame()

    return df

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
    
    # 1. Supprimer les doublons
    df_sans_doublons = df.drop_duplicates(subset=['ID'], keep='first')
    context.log.info(f"{len(df) - len(df_sans_doublons)} doublons supprimés.")
    df = df_sans_doublons
    
    # 2. Gérer les valeurs manquantes
    # Par exemple, remplir des valeurs manquantes spécifiques ou supprimer des lignes
    # Pour les coordonnées géographiques, nous pouvons exiger qu'elles soient présentes
    df = df.dropna(subset=['Latitude', 'Longitude'])
    context.log.info(f"Après suppression des lignes sans coordonnées: {len(df)} lignes.")
    
    # 3. Standardiser certaines valeurs
    if 'Type' in df.columns and not df['Type'].empty:
        # Standardiser les types d'événements (exemple)
        type_mapping = {
            'Accident': 'Accident',
            'Roadworks': 'Travaux',
            'MaintenanceWorks': 'Travaux',
            'PlanningWorks': 'Travaux',
            # Ajoutez d'autres mappings selon vos besoins
        }
        
        df['Type'] = df['Type'].apply(lambda x: type_mapping.get(x, x) if pd.notna(x) else x)
    
    # 4. Convertir les dates en format datetime pour faciliter les manipulations
    for col in ['Debut', 'Fin']:
        if col in df.columns and not df[col].empty:
            try:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y %H:%M:%S', errors='coerce')
            except Exception as e:
                context.log.warning(f"Erreur lors de la conversion de la colonne {col}: {e}")
    
    # 5. Ajouter de nouvelles colonnes calculées qui pourraient être utiles
    # Par exemple, extraction du jour de la semaine, de l'heure, etc.
    if 'Debut' in df.columns and not df['Debut'].empty:
        try:
            df['Jour_Semaine'] = df['Debut'].dt.day_name()
            df['Heure_Debut'] = df['Debut'].dt.hour
        except Exception as e:
            context.log.warning(f"Erreur lors de la création des colonnes dérivées: {e}")
    
    # 6. Normaliser certaines valeurs textuelles
    text_columns = ['Lieu', 'Voie', 'Direction']
    for col in text_columns:
        if col in df.columns and not df[col].empty:
            # Convertir en majuscules et supprimer les espaces superflus
            df[col] = df[col].str.strip().str.upper() if df[col].dtype == 'object' else df[col]
    
    # 7. Filtrer les événements périmés si nécessaire
    if 'Fin' in df.columns and not df['Fin'].empty:
        now = pd.Timestamp.now()
        df_actifs = df[df['Fin'] >= now]
        context.log.info(f"{len(df) - len(df_actifs)} événements périmés supprimés.")
        df = df_actifs
    
    # 8. Trier le DataFrame
    if 'Debut' in df.columns and not df['Debut'].empty:
        df = df.sort_values(by='Debut', ascending=False)
    
    context.log.info(f"Nettoyage terminé: {len(df)} lignes après traitement.")
    
    return df