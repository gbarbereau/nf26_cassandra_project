#!/usr/bin/env python
from cassandra.cluster import Cluster
import math


class Bdd:
    def __init__(self):
        self.bdd = None

    def __del__(self):
        if self.bdd:
            self.bdd.shutdown()
            self.bdd = None

    def change_default_timeout(self, timeout):
        self.bdd.default_timeout = timeout

    def connect(self, username="e13"):
        self.bdd = Cluster().connect(username)

    def get_size(self, table):
        query = "SELECT COUNT(1) FROM %s;" % table
        return self.execute(query)

    def disconnect(self):
        self.bdd.shutdown()
        self.bdd = None

    def execute(self, query):
        return self.bdd.execute(query)

    def select_all(self, table):
        query = "SELECT * FROM %s;" % table
        return self.execute(query)

    def select(self, table, attributes):
        query = "SELECT %s FROM %s;" % (", ".join(attributes), table)
        return self.execute(query)

    def clean_table(self, table):
        query = "DROP TABLE IF EXISTS e13.%s;" % table
        self.execute(query)
        return True

    def clean_database(self):
        list_table = self.get_table_list()
        for table in list_table:
            self.clean_table(table)

    def group_execute(self, items, limit=100):
        splitted_list = [items[i:i + limit] for i in range(0, len(items), limit)]
        for l in splitted_list:
            query = "BEGIN BATCH " + ' '.join(l) + " APPLY BATCH;"
            self.execute(query)

    def get_table_list(self):
        query = "DESCRIBE TABLES"
        return self.execute(query)

    def get_insert_query(self, fields, ordered_ids, table):
        rm_index = []
        arguments = ""
        for index, elem in enumerate(ordered_ids):
            if elem == '':
                rm_index.append(index)
            elif isinstance(elem, str) and elem != "False" and elem != "True":
                arguments += "'%s', " % elem
            else:
                arguments += "%s, " % elem
        arguments = arguments[:-2]
        for ind in sorted(rm_index, reverse=True):
            fields.pop(ind)
        query = "INSERT INTO %s (%s) VALUES (%s);" % (table, ", ".join(fields), arguments)
        return query

    def create_table(self, attributes, pk, name):
        query = "CREATE TABLE IF NOT EXISTS %s (" % name
        query += [x + y + ", " for x, y in enumerate(attributes)]
        query += "PRIMARY KEY ( %s ));" % ", ".join(pk)
        self.execute(query)

    def selection_grouped(self, table, attribute, operation, limit=None):
        if isinstance(attribute,list):
            attribute = ", ".join(attribute)
        query = "SELECT %s, %s(*) FROM %s GROUP BY %s" % (
        attribute, operation, table, attribute)
        if limit:
            query += "LIMIT %s" % limit
        return self.execute(query)
