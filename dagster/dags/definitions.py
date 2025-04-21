from dagster import Definitions

# APRÈS si renommé en fichiers_xml.py
from assets.fichiers_xml import fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb
from assets.visualisations import visualisation_trafic
from jobs import mon_job
from resources.mongodb import mongodb_resource  # Import ajouté ici
from jobs.schedules import bison_fute_daily_schedule  # Importez la schedule
from jobs.sensors import bison_fute_file_sensor  # Importez le sensor



# # Define all assets
# asset_definitions = [
#     fichiers_xml_action_b,
#     parse_fichiers_xml,
#     nettoyer_donnees_xml,
#     xml_data_csv, 
#     xml_data_mongodb, 
   
# ]

# # Define all jobs
# job_definitions = [
#     mon_job,
# ]

# # Define all schedules
# schedule_definitions = [
#     my_job_schedule,
# ]

defs = Definitions(
    assets=[fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb, visualisation_trafic],
    jobs=[mon_job],
     schedules=[bison_fute_daily_schedule],  # Ajoutez la schedule
    sensors=[bison_fute_file_sensor],  # Ajoutez le sensor
    resources={
        "mongodb": mongodb_resource.configured({
            "connection_string": "mongodb://supmap:supmap@mongodb:27017/supmap?authSource=admin",
            "database_name": "supmap"
        })
    }
)