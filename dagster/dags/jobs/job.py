from dagster import job
from assets.fichiers_xml import fichiers_xml_action_b, parse_fichiers_xml

@job
def mon_job():
    # L'output de `fichiers_xml_action_b` sera automatiquement utilisé comme entrée pour `parse_fichiers_xml`
    fichiers = fichiers_xml_action_b()  # Tu appelles le premier asset
    parse_fichiers_xml(fichiers)  # Tu passes l'output de `fichiers_xml_action_b` à `parse_fichiers_xml`
