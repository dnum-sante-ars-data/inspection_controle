# INSPECTION CONTROLE - README
- La base de données regroupe : 
* les tables métier de staging préfixées sa_ : copie sans altération des fichiers sources
* les tables temporaires métier préfixées missions_ sur lesquelles sont effectuées certaines transformations, lesdites transformations sont effectuées dans des tâches de base de données
* les tables de référentiel géo préfixées ref_
* les tables finess : complet (t_finess) et filtré sur les EHPAD (t_finess_500)
* les tables finales destinées à OpendataSoft préfixées DWH_ au sein de vues constituées après transformations, lesdites transformations sont effectuées dans les requêtes des vues
* les tables finales destinées aux TdB excel prévixées TDB_ au sein de vues constituées après transformations, lesdites transformations sont effectuées dans les requêtes des vues
- Mode opératoire pour charger les données : 
* NB : pour afficherles tâches de BDD, dans DBeaver, aller dans le menu base de données puis cliquer sur tâches
* supprimer les tables sa_ et impoter les nouvelles données en créant de nouvelles tables sa_
* supprimer la table t_finess et importer les nouvelles données en créant de nouveau la table t_finess
* supprimer les tables missions_ et exécuter la tâche siicea_sa_to_missions_real puis siicea_sa_to_missions_prev
* supprimer la table t_finess_500 et exécuter la tâche t_finess_to_500
* regénérer l'ensemble des vues
