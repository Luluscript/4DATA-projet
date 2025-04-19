from dagster import job
from assets.fichiers_xml import fichiers_xml_action_b, parse_fichiers_xml, nettoyer_donnees_xml, xml_data_csv

@job
def mon_job():
    # L'output de chaque asset sera automatiquement utilisé comme entrée pour le suivant
    fichiers = fichiers_xml_action_b()  # Premier asset
    df_parsed = parse_fichiers_xml(fichiers)  # Deuxième asset avec l'output du premier
    df_cleaned = nettoyer_donnees_xml(df_parsed)  # Troisième asset qui nettoie les données
    xml_data_csv(df_cleaned)  # Quatrième asset qui exporte en CSV