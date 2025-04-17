from dagster import asset
import os
import requests
from bs4 import BeautifulSoup

@asset
def fichiers_xml_action_b(context):
    url_base = "http://tipi.bison-fute.gouv.fr/bison-fute-restreint/publications-restreintes/grt/ACTION-B/"
    login = "mpublication-grt"
    password = "CheiPe7T"

    response = requests.get(url_base, auth=(login, password))
    if response.status_code != 200:
        raise Exception(f"Erreur {response.status_code} lors de la récupération de la page")

    soup = BeautifulSoup(response.text, "html.parser")
    liens_xml = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith(".xml")]
    context.log.info(f"{len(liens_xml)} fichiers XML trouvés.")

    dossier_data = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'xml_action_b'))
    os.makedirs(dossier_data, exist_ok=True)

    for lien in liens_xml:
        url_fichier = url_base + lien
        nom_fichier = os.path.join(dossier_data, lien)

        fichier = requests.get(url_fichier, auth=(login, password))
        if fichier.status_code == 200:
            with open(nom_fichier, 'wb') as f:
                f.write(fichier.content)
            context.log.info(f"Fichier téléchargé : {nom_fichier}")
        else:
            context.log.warning(f"Échec : {url_fichier} - Code {fichier.status_code}")
