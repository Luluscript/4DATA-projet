import os

# Utiliser des chemins relatifs par rapport au répertoire courant
# Cela fonctionne mieux dans différents environnements
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VISUALISATIONS_DIR = os.path.join(BASE_DIR, "data", "visualisations")

# S'assurer que le répertoire existe
os.makedirs(VISUALISATIONS_DIR, exist_ok=True)

# Chemins des fichiers de visualisation
TYPES_EVENEMENTS_PATH = os.path.join(VISUALISATIONS_DIR, "types_evenements.png")
EVENEMENTS_PAR_JOUR_PATH = os.path.join(VISUALISATIONS_DIR, "evenements_par_jour.png")
SEVERITE_EVENEMENTS_PATH = os.path.join(VISUALISATIONS_DIR, "severite_evenements.png")
DASHBOARD_HTML_PATH = os.path.join(VISUALISATIONS_DIR, "dashboard.html")
