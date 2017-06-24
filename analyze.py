def select_date_trips(bdd, year, month=None, day=None):
    if month == None:
        month = list(range(1, 13))
    if day == None:
        day = list(range(1, 32))
    query = "SELECT TRIP_ID FROM DATE_COMPLETE WHERE YEAR = %s AND MONTH IN (%s) AND DAY IN (%s)" % (
        year, ", ".join(month), ", ".join(day))
    return bdd.execute(query)


def answer(bdd):
    # This function goal is to answer the question asked during the modelisation
    print("Top 10 taxis")
    list_taxi = sorted(list(bdd.selection_grouped("TAXI_TRIP", "TAXI_ID", "COUNT")), key=lambda x: x.count,
                       reverse=True)
    [print(i) for i in list_taxi[:10]]
    print("Distance moyenne d'un trajet ")
    dist = list(bdd.execute("SELECT SUM(DIST), COUNT(DIST) FROM FACTS;"))
    print(dist[0].system_sum_dist / dist[0].system_count_dist)
    print("Quel sont les types de jours les plus sollicités ?")
    list_type_day = bdd.selection_grouped("DATE_TYPE_DAY", "DAY_TYPE", "COUNT")
    print(list(list_type_day)[0].day_type)
    print("Quelle est la journée la plus sollicitée de l'année ?")
    day = sorted(list(bdd.selection_grouped("DATE_COMPLETE", ["YEAR","MONTH","DAY"], "COUNT")), key=lambda x: x.count, reverse=True )
    print(day[0])
