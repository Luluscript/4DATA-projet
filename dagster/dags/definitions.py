from dagster import Definitions

# APRÈS si renommé en fichiers_xml.py
from assets.fichiers_xml import fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb
from assets.visualisations import visualisation_trafic
from jobs import mon_job
from resources.mongodb import mongodb_resource  
from jobs.schedules import bison_fute_daily_schedule  
from jobs.sensors import bison_fute_file_sensor  

defs = Definitions(
    assets=[fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb, visualisation_trafic],
    jobs=[mon_job],
     schedules=[bison_fute_daily_schedule],  
    sensors=[bison_fute_file_sensor],  
    resources={
        "mongodb": mongodb_resource.configured({
            "connection_string": "mongodb://supmap:supmap@mongodb:27017/supmap?authSource=admin",
            "database_name": "supmap"
        })
    }
)