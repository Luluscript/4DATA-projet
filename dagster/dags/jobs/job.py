from dagster import job
from assets.fichiers_xml import fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv, xml_data_mongodb
from resources.mongodb import mongodb_resource


@job
def mon_job():
    # L'output de chaque asset sera automatiquement utilisé comme entrée pour le suivant
    fichiers = fichiers_xml_action_b()
    df_parsed = parse_fichiers_xml(fichiers)
    df_cleaned = nettoyer_donnees_xml(df_parsed)
    csv_path = xml_data_csv(df_cleaned)
    xml_data_mongodb(csv_path)