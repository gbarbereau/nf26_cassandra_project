def select_date_trips(bdd, year, month=None, day=None):
    if month == None:
        month = list(range(1, 13))
    if day == None:
        day = list(range(1, 32))
    query = "SELECT TRIP_ID FROM DATE_COMPLETE WHERE YEAR = %s AND MONTH IN (%s) AND DAY IN (%s)" % (
        year, ", ".join(month), ", ".join(day))
    return bdd.execute(query)

def answer(bdd):
    #This function goal is to answer the question asked during the modelisation
    #Quels sont les taxis les plus pris ?
    print("Top 10 taxis")
    list_taxi = bdd.selection_grouped("TAXI_TRIP", "TAXI_ID", "COUNT")
    [print(i) for i in list_taxi]
    print("Distance moyenne d'un trajet ")
    #Quelle est la distance moyenne d'un trajet ?
    dist = bdd.execute("SELECT AVG(DIST) FROM FACTS;")
    print(dist)
    #Sur quels types de jour les taxis sont-ils les plus sollicités ?
    print("Quel sont les types de jours les plus sollicités ?")
    list_type_day = bdd.selection_grouped("TYPE_DAY", "DAY_TYPE", "COUNT")
    [print(i) for i in list_type_day]