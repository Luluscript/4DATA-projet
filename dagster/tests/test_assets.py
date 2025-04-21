# tests/test_assets.py
import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock, mock_open
import sys

# Ajouter le répertoire parent au PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Le reste de vos imports
from unittest.mock import patch, MagicMock

# Import après avoir ajusté le PYTHONPATH
from dags.assets import fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb

# Test pour l'asset fichiers_xml_action_b
@patch('dags.jobs.sensors.requests.get')
def test_fichiers_xml_action_b(mock_get, tmp_path):
    # Créer un mock pour la réponse de requests.get
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = """
    <html>
        <body>
            <a href="file1.xml">file1.xml</a>
            <a href="file2.xml">file2.xml</a>
            <a href="document.pdf">document.pdf</a>
        </body>
    </html>
    """
    mock_get.return_value = mock_response
    
    # Créer un mock pour le context Dagster
    mock_context = MagicMock()
    mock_context.log = MagicMock()
    
    # Rediriger le dossier de sortie vers un dossier temporaire
    with patch('os.path.dirname', return_value=str(tmp_path)), \
         patch('os.makedirs', return_value=None), \
         patch('builtins.open', mock_open()), \
         patch('os.path.join', return_value=str(tmp_path / "file.xml")):
        
        result = fichiers_xml_action_b(mock_context)
        
        # Vérifier que la fonction a bien appelé requests.get
        mock_get.assert_called()
        
        # Vérifier que la fonction a filtré uniquement les fichiers XML
        assert len(result) == 2
        assert "file1.xml" in [os.path.basename(f) for f in result]
        assert "file2.xml" in [os.path.basename(f) for f in result]
        
        # Vérifier que les logs ont été créés
        mock_context.log.info.assert_called()

# Test pour l'asset parse_fichiers_xml
@patch('dags.jobs.sensors.etree.parse')
def test_parse_fichiers_xml(mock_parse, tmp_path):
    # Créer des fichiers XML factices pour le test
    xml_files = [str(tmp_path / "file1.xml"), str(tmp_path / "file2.xml")]
    
    # Créer un mock pour le context Dagster
    mock_context = MagicMock()
    mock_context.log = MagicMock()
    
    # Créer un mock pour le résultat de etree.parse
    mock_tree = MagicMock()
    mock_root = MagicMock()
    
    # Simuler un document XML avec une structure minimale
    def find_with_ns(path, namespaces):
        if path == ".//soap:Body":
            return MagicMock()
        return None
    
    mock_root.find = find_with_ns
    mock_root.tag = "root"
    mock_root.iter.return_value = []
    
    mock_tree.getroot.return_value = mock_root
    mock_parse.return_value = mock_tree
    
    # Appeler la fonction à tester
    result = parse_fichiers_xml(mock_context, xml_files)
    
    # Vérifier que la fonction a essayé de parser les fichiers XML
    assert mock_parse.call_count == len(xml_files)
    
    # Vérifier qu'un DataFrame a été retourné (même vide)
    assert isinstance(result, pd.DataFrame)
    
    # Vérifier que les logs ont été créés
    mock_context.log.info.assert_called()

# Test pour l'asset nettoyer_donnees_xml
def test_nettoyer_donnees_xml():
    # Créer un mock pour le context Dagster
    mock_context = MagicMock()
    mock_context.log = MagicMock()
    
    # Créer un DataFrame de test
    test_data = pd.DataFrame({
        'SituationID': ['1', '2', '3'],
        'RecordType': ['ns2:Accident', 'AbnormalTraffic', 'Other'],
        'DateVersion': ['2023-01-01T12:00:00', '2023-01-02T14:30:00', '2023-01-03T09:15:00'],
        'DateDebut': ['2023-01-01T10:00:00', '2023-01-02T13:00:00', '2023-01-03T08:00:00'],
        'LatitudeDepart': [45.5, 200.0, -91.0],  # Une valeur invalide
        'LongitudeDepart': [3.5, 4.0, -190.0],   # Une valeur invalide
        'Commentaire': ['Accident', 'Trafic dense', 'Travaux']
    })
    
    # Appeler la fonction à tester
    result = nettoyer_donnees_xml(mock_context, test_data)
    
    # Vérifier que le DataFrame résultant a la bonne structure
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(test_data)
    
    # Vérifier que les dates ont été converties en datetime
    assert pd.api.types.is_datetime64_dtype(result['DateVersion'])
    assert pd.api.types.is_datetime64_dtype(result['DateDebut'])
    
    # Vérifier que les valeurs hors limites pour les coordonnées sont devenues NaN
    assert pd.isna(result.loc[1, 'LatitudeDepart'])
    assert pd.isna(result.loc[2, 'LongitudeDepart'])
    
    # Vérifier que les logs ont été créés
    mock_context.log.info.assert_called()

# Test pour l'asset xml_data_csv
def test_xml_data_csv(tmp_path):
    # Créer un mock pour le context Dagster
    mock_context = MagicMock()
    mock_context.log = MagicMock()
    
    # Créer un DataFrame de test
    test_data = pd.DataFrame({
        'SituationID': ['1', '2', '3'],
        'Commentaire': ['Accident', 'Trafic dense', 'Travaux']
    })
    
    # Rediriger le dossier de sortie vers un dossier temporaire
    with patch('os.path.dirname', return_value=str(tmp_path)), \
         patch('os.makedirs', return_value=None), \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
         patch('os.path.exists', return_value=True), \
         patch('os.path.getsize', return_value=1000):
        
        # Appeler la fonction à tester
        result = xml_data_csv(mock_context, test_data)
        
        # Vérifier que to_csv a été appelé
        mock_to_csv.assert_called_once()
        
        # Vérifier que la fonction a retourné un chemin de fichier
        assert isinstance(result, str)
        
        # Vérifier que les logs ont été créés
        mock_context.log.info.assert_called()

# Test pour l'asset xml_data_mongodb
def test_xml_data_mongodb():
    # Créer un mock pour le context Dagster
    mock_context = MagicMock()
    mock_context.log = MagicMock()
    mock_context.resources.mongodb = MagicMock()
    
    # Créer un mock pour la collection MongoDB
    mock_collection = MagicMock()
    mock_db = MagicMock()
    mock_db.__getitem__.return_value = mock_collection
    mock_context.resources.mongodb.get_database.return_value = mock_db
    
    # Créer un patch pour pd.read_csv
    test_data = pd.DataFrame({
        'SituationID': ['1', '2', '3'],
        'Commentaire': ['Accident', 'Trafic dense', 'Travaux']
    })
    
    with patch('pandas.read_csv', return_value=test_data), \
         patch('os.path.exists', return_value=True):
        
        # Appeler la fonction à tester
        xml_data_mongodb(mock_context, "fake_path.csv")
        
        # Vérifier que la fonction a appelé les méthodes MongoDB
        mock_collection.delete_many.assert_called_once()
        mock_collection.insert_many.assert_called_once()
        
        # Vérifier que les logs ont été créés
        mock_context.log.info.assert_called()
