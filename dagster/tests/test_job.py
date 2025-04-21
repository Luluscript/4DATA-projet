# tests/test_jobs.py
import pytest
from dagster import execute_job
import sys
import os
from dags.assets import fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb
from dags.assets import visualisation_trafic

# Ajouter le dossier parent au chemin de recherche Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer vos jobs
from dags.jobs.job import my_job  # Remplacez par le nom réel de votre job

def test_job_execution(mock_mongodb_resource):
    """Test pour vérifier que le job s'exécute sans erreur."""
    # Remplacez les ressources réelles par des mocks
    resource_defs = {
        "mongodb": mock_mongodb_resource,
    }
    
    # Exécuter le job avec des ressources mockées
    result = execute_job(
        my_job,
        resources=resource_defs
    )
    
    # Vérifier que le job s'est exécuté avec succès
    assert result.success
    
    # Vérifier que tous les assets ont été matérialisés
    materialized_assets = [event.asset_key for event in result.get_asset_materialization_events()]
    expected_assets = ["parse_xml_asset", "transform_data_asset"]  
    
    for asset in expected_assets:
        assert any(asset in str(materialized_asset) for materialized_asset in materialized_assets)
