# tests/conftest.py
import pytest
import pandas as pd
import os
from unittest.mock import MagicMock
import sys

# Ajouter le répertoire du projet au PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture
def sample_xml_data():
    """Fixture pour les données XML d'exemple"""
    return [
        "chemin/vers/fichier1.xml",
        "chemin/vers/fichier2.xml"
    ]

@pytest.fixture
def sample_df():
    """Fixture pour un DataFrame d'exemple"""
    return pd.DataFrame({
        'SituationID': ['1', '2', '3'],
        'SituationVersion': ['1', '1', '1'],
        'Severite': ['high', 'medium', 'low'],
        'DateVersion': ['2023-01-01T12:00:00', '2023-01-02T14:30:00', '2023-01-03T09:15:00'],
        'RecordType': ['Accident', 'AbnormalTraffic', 'MaintenanceWorks'],
        'RecordID': ['R1', 'R2', 'R3'],
        'RecordVersion': ['1', '1', '1'],
        'Commentaire': ['Accident sur l\'autoroute', 'Trafic dense', 'Travaux en cours'],
        'EstTermine': [False, False, True],
        'DateDebut': ['2023-01-01T10:00:00', '2023-01-02T13:00:00', '2023-01-03T08:00:00'],
        'LatitudeDepart': [45.5, 46.0, 44.8],
        'LongitudeDepart': [3.5, 4.0, 3.8],
        'LatitudeArrivee': [45.6, 46.1, 44.9],
        'LongitudeArrivee': [3.6, 4.1, 3.9],
        'TypeTrafic': ['congestion', 'abnormalTraffic', ''],
        'LongueurFile': ['500', '1000', ''],
        'NumeroRoute': ['A1', 'N7', 'D123'],
        'FichierSource': ['file1.xml', 'file2.xml', 'file3.xml']
    })

@pytest.fixture
def mock_dagster_context():
    """Fixture pour simuler un contexte Dagster"""
    context = MagicMock()
    context.log = MagicMock()
    
    # Simuler les méthodes de logging
    for method in ['info', 'warning', 'error', 'debug']:
        setattr(context.log, method, MagicMock())
    
    # Simuler les ressources
    context.resources = MagicMock()
    mongodb_mock = MagicMock()
    db_mock = MagicMock()
    collection_mock = MagicMock()
    
    # Configurer les mocks pour simuler la structure MongoDB
    db_mock.__getitem__.return_value = collection_mock
    mongodb_mock.get_database.return_value = db_mock
    context.resources.mongodb = mongodb_mock
    
    return context
