from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
import json

driver = GraphDatabase.driver('neo4j+s://cb6521e8.databases.neo4j.io:7687', auth=('neo4j', 'jvI0Qw1-M8uRNzI42LrWaQI36hUUOoKKJPs09bKUpYU'))
session = driver.session()
graph = driver.session(database="neo4j")

# query = "MATCH (n:User {username: '%s'}) return n"%('Danielgonc14')

# result = graph.run(query)
# print(result.data())