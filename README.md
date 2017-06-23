# Projet cassandra NF26

Le but de ce projet est de traiter un grand jeu de données, d'environ 1 700 000 lignes. Nous cherchons ici à utiliser Cassandra dans un but éducatif, pour se familiariser avec les grandes problématiques du traitement.

Le projet se divise en 3 parties :
- Traitement du fichier en entrée
- Connexion avec la base de données Cassandra
- Analyse des données insérées

Le projet est réalisé entièrement en Python 3, et utilise les libraries Arrow et cassandra-driver.

## Traitement du fichier en entrée

Le fichier est composé de différents attributs :
| Variable  | Description |
| ------------- | ------------- |
| TRIP_ID  | Descripteur unique du trajet  |
| CALL_TYPE  | Type d'appel, A B C  |
| ORIGIN_CALL | ID du centre d'appel, rempli si CALL_TYPE=A |
| ORIGIN_STAND | ID du spot, rempli si CALL_TYPE=B |
| TAXI_ID | Descripteur unique du trajet |
| TIMESTAMP | |
| DAY_TYPE | Férié, vacance ou scolaire |
| MISSING_DATA | Données manquantes sur la Polyline |
| POLYLINE | Coordonnées du trajet |

Les données sont entrée de façon à pouvoir répondre aux questions suivantes :
-Quels sont les taxis les plus pris ?
-Quels sont les types d'appels les plus courants ?
-A quel endroit sollicitons-nous le plus de taxis ? 
-A quelle date/type de jour les taxis sont-ils le plus sollicités ? 

La manière choisie de répondre à ces questions viendra avec la description des fichiers.

## Structure de la base de données

Le fichier database contient une classe servant de surcouche sur l'interface de base Cassandra. Il contient des fonctions permettant de créer des tables, nettoyer la base de données ou encore de faire des sélections.
Le fichier tools contient quant à lui des fonctions outils, notamment celle servant à créer les différentes tables. En voici la liste exhaustive :
- Une table contenant les jours, mois et années des différents trajets
- Un table contenant le type du jour au cours duquel le trajet a été réalisé
- Une table contenant les informations sur le trajet. Il contient la position de départ, d'arrivée et la distance parcourue par les taxis
- Une table contenant le type d'appel 
- Une table contenant l'identifiant du taxi.

Ces tables permettent en effet de répondre à toutes les questions présentées ci-dessus, mais également d'autres, comme par exemple la distance moyenne, ou encore le nombre de trajet en un mois.
Les différentes tables contiennent toutes l'identifiant du trajet, et utilisent une clé de partitionnement étudiée pour répondre aux questions ci-dessus.
Il est ainsi possible de les croiser afin de répondre à des requêtes du type : "Quelle est la distance moyenne parcourue par taxi ?"

## Analyse

L'analyse se déroule en plusieurs parties. Tout d'abord, l'interrogation directe des tables fourni, du fait de leur structure la réponse à la plupart des questions.
Ensuite, on peut réaliser une analyse plus poussée. Ici a été réimplémenté une version de l'algorithme des kmeans. Nous l'utilisons ici pour retrouver les différents "spots" du jeu de données. 
Cela correspond par exemple à des endroits intéressants pour implémenter un dépôt de taxi. L'implémentation fournie ici permet également de le faire fonctionner sur les distances, pour retrouver une classificationd des k premières distances moyennes.


