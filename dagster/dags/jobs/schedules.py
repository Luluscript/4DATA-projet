# schedules.py
from dagster import ScheduleDefinition
from jobs.job import mon_job

# Schedule qui exécute le job tous les jours à 6h du matin
bison_fute_daily_schedule = ScheduleDefinition(
    name="bison_fute_daily",
    cron_schedule="0 6 * * *",  # format cron: minute heure jour_du_mois mois jour_de_semaine
    job=mon_job,
    execution_timezone="Europe/Paris"  # Ajustez selon votre fuseau horaire
)
