from dagster import Definitions

# APRÈS si renommé en fichiers_xml.py
from assets.fichiers_xml import fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv
# parse_fichiers_xml

from jobs import mon_job

# from schedules import my_job_schedule 


# Define all assets
asset_definitions = [
    fichiers_xml_action_b,
    parse_fichiers_xml,
    nettoyer_donnees_xml,
    xml_data_csv
]

# Define all jobs
job_definitions = [
    mon_job,
]

# # Define all schedules
# schedule_definitions = [
#     my_job_schedule,
# ]

# Combine all definitions
defs = Definitions(
    assets=asset_definitions,
    sjobs=job_definitions,
    # schedules=schedule_definitions,
)
