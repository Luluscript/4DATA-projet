from dagster import Definitions

# APRÈS si renommé en fichiers_xml.py
from assets.fichiers_xml import fichiers_xml_action_b 
# parse_fichiers_xml

# from jobs import MyJob

# from schedules import my_job_schedule 


# Define all assets
asset_definitions = [
    fichiers_xml_action_b,
    # parse_fichiers_xml,
]

# # Define all jobs
# job_definitions = [
#     MyJob,
# ]

# # Define all schedules
# schedule_definitions = [
#     my_job_schedule,
# ]

# Combine all definitions
defs = Definitions(
    assets=asset_definitions,
    # jobs=job_definitions,
    # schedules=schedule_definitions,
)
