from dagster import Definitions

# APRÈS si renommé en fichiers_xml.py
from assets.fichiers_xml import fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb
# parse_fichiers_xml
from jobs import mon_job
from resources.mongodb import mongodb_resource  # Import ajouté ici

# from schedules import my_job_schedule 


# Define all assets
asset_definitions = [
    fichiers_xml_action_b,
    parse_fichiers_xml,
    nettoyer_donnees_xml,
    xml_data_csv, 
    xml_data_mongodb
]

# Define all jobs
job_definitions = [
    mon_job,
]

# # Define all schedules
# schedule_definitions = [
#     my_job_schedule,
# ]

defs = Definitions(
    assets=[fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb],
    jobs=[mon_job],
    resources={
        "mongodb": mongodb_resource.configured({
            "connection_string": "mongodb://supmap:supmap@mongodb:27017/supmap?authSource=admin",
            "database_name": "supmap"
        })
    }
)