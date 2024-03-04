# # Flight Connections and Airports Graph Analysis

'''
In this analysis, I am going to use graph analysis applied to a Big Data framework.
I will use GraphFrames Spark library with the API for DataFrames (GraphFrames Spark library) 
to analyze a dataset containing information about flight connections and airports in the whole world.

I am going to use datasets containing informations about airports, airlines and flights worldwide.
Consider these three `csv` files:
- airports.csv: contains one line for each airport in the world. Among the others, it provides the columns: 
id, name, city, country, iata, latitude and longitude.
- airlines.csv: provides some information for each airline. Among the others, it provides the columns: 
airline_id, name, country, icao.
- routes.csv enumerates the flights provided by each airline between two airports. it provides the columns:
airline_id, airport_source_id, airport_destination_id.

For this Analysis, I am going to use GraphFrames Spark library with the API for DataFrames 
(GraphFrames Spark library) 
'''

# Input Datasets from the big data cluster
airports = '/data/students/bigdata_internet/lab5/airports.csv'
airlines = '/data/students/bigdata_internet/lab5/airlines.csv'
flights = '/data/students/bigdata_internet/lab5/routes.csv'

'''In this analysis, PySpark was utilized for its robust distributed computing capabilities, 
ideal for handling large datasets efficiently.
If you're using the PySpark shell, no additional setup is necessary. 
However, for those working in a Python environment, setting up PySpark involves the following steps:
1. Install PySpark: Begin by installing PySpark using pip:
pip install pyspark
2. Configure PySpark.sql: In your Python script or interactive session, include the following configuration 
to initialize PySpark.sql:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```
Ensure to execute this configuration before performing any PySpark operations.
For comprehensive installation and configuration instructions, refer to the official PySpark documentation: 
PySpark Installation Guide
'''

# Reading datasets
airportsDF = spark.read.load(airports, format="csv", header=True, inferSchema=True, sep=',')
airlinesDF = spark.read.load(airlines, format="csv", header=True, inferSchema=True, sep=',')
flightsDF = spark.read.load(flights, format="csv", header=True, inferSchema=True, sep=',')

# Find top airports and airlines
# Find countries with more than 200 airports
numberOfAirports = airportsDF.groupBy("country").agg({"id": "count"})
airportsDF.createOrReplaceTempView("airportsDF")
numberOfAirportsSorted = spark.sql('''SELECT country, count(iata) as number_of_airports FROM airportsDF
                                        GROUP BY country Having count(iata) > 200 ORDER BY number_of_airports DESC''')
numberOfAirportsSorted.show()

# Find the top-10 airlines by total number of flights
numberOfFlights = flightsDF.groupBy("airline_id").agg({"airline_id": "count"})
numberOfFlights = numberOfFlights.withColumnRenamed("count(airline_id)", "num_flights")
joinedAirlineFlights = airlinesDF.join(numberOfFlights, airlinesDF.airline_id == numberOfFlights.airline_id)
icaoAirline = joinedAirlineFlights.selectExpr("name", "icao", "num_flights")
icaoAirlineOrdered = icaoAirline.sort("num_flights", ascending=False)
icaoAirlineOrderedFinal = icaoAirlineOrdered.limit(10).sort("num_flights", ascending=True)

icaoAirlineOrderedFinal.show(truncate=False)

# Find the top-5 routes in the world
groupedFlights = flightsDF.groupBy("airport_source_id", "airport_destination_id").agg({"airport_source_id": "count"})
groupedFlightsOrdere = groupedFlights.sort("count(airport_source_id)", ascending=False)
groupedFlightsOrdere.count()

from pyspark.sql.functions import col
joined1 = groupedFlightsOrdere.join(airportsDF, groupedFlightsOrdere.airport_source_id == airportsDF.id).select(col("airport_source_id"), 
        col("airport_destination_id"), col("count(airport_source_id)").alias("route_count"), col("name").alias("source_airport"), 
        col("city").alias("source_city"))

joined2 = joined1.join(airportsDF, joined1.airport_destination_id == airportsDF.id).select(
    col("source_airport"), col("source_city"), col("name").alias("destination_airport"), col("city").alias("destination_city"), col("route_count"))

routes = joined1.join(airportsDF, joined1.airport_destination_id == airportsDF.id).select(col("airport_source_id"), col("airport_destination_id"))

routesName = joined2.sort("route_count", ascending=False)

routesName.show(5, truncate=False)

# -------------------------------------------------------------------------------------------

# Create the graph of flight connections
# Build a graph using GraphFrames where vertices are the airports in airports.csv,
# and edges are the flights from one airport to another contained in routes.csv 

# Cleaning the flights (routes) by removing missing values
flightsDF.count()
flightsClean = flightsDF.filter((flightsDF["airport_source_id"] != "\\N") & (flightsDF["airport_destination_id"] != "\\N"))
flightsClean.count()

from graphframes import GraphFrame
v = airportsDF.select("id", "name", "icao", "city", "country")
v = v.withColumn("id", v.id.cast("string"))
e = flightsClean.selectExpr("airport_source_id as src", "airport_destination_id as dst", "airline_id")
e = e.withColumn("src", e.src.cast("string"))
e = e.withColumn("dst", e.dst.cast("string"))
g = GraphFrame(v, e)

# -------------------------------------------------------------------------------------------

# Find the 5 airports with the highest ratio of outgoing edges over incoming edges (edgeOut/edgeIn)
outgoing = g.outDegrees
incoming = g.inDegrees

outgoing_incoming = outgoing.join(incoming, outgoing.id == incoming.id).select(
    outgoing["id"], incoming["inDegree"], outgoing["outDegree"], (outgoing["outDegree"] / incoming["inDegree"]).alias("ratio"))

outgoing_incoming_final = outgoing_incoming.join(airportsDF, outgoing_incoming.id == airportsDF.id).select(
    airportsDF["name"], outgoing_incoming["id"], outgoing_incoming["inDegree"], outgoing_incoming["outDegree"], outgoing_incoming["ratio"])

outgoing_incoming_final.sort("ratio", ascending=False).show(5, truncate=False)

# -------------------------------------------------------------------------------------------

# Finding the number of airports that from there we can reach the city of "Torino" taking exactly 1 flight

# Finding the id of 'Turin Airport'
idTurin = outgoing_incoming_final.filter("name = 'Turin Airport'")
idTurin.show(1)

# Create the motif
motifs = g.find("(a)-[]->(b)")

# filter all destination with 'Turin Airport' id
airportsToTurin = motifs.filter("b.id = 1526")

airportsToTurin.select("a").distinct().count()

# -------------------------------------------------------------------------------------------

# Find the number of airports that can be reached from the city of "Torino" taking exactly 1 flight
airportsFromTurin = motifs.filter("a.id = 1526")
airportsFromTurin.select("b").distinct().count()

'''
In the analysis of Turin Airport's network connectivity, both inDegrees and outDegrees are observed to be 29. 
This symmetry implies a balanced exchange of flights – for each departing flight, there is a corresponding 
incoming flight. The equal values of inDegrees and outDegrees indicate a reciprocity in the airport's connectivity,
highlighting a comprehensive and evenly distributed network.
'''

# -------------------------------------------------------------------------------------------

# Find the number of airports that can be reached from the city of "Torino" taking exactly 2 flights

# Create the motif
stopFlightMotifs = g.find("(v1)-[]->(v2);(v2)-[]->(v3)")

# Filter the origin airport 'Turin Airport' id
stopFlightFromTurin = stopFlightMotifs.filter("v1.id = 1526")
stopFlightFromTurin.select("v3").distinct().count()

# -------------------------------------------------------------------------------------------

# Find the number of airports that from there we can reach "Los Angeles International Airport" using less hop
# than to reach the city of "Torino"

# Find the id of "Los Angeles International Airport"
idLA = outgoing_incoming_final.filter("name = 'Los Angeles International Airport'")
idLA.show(1)

# Calculate shortestPaths to "Los Angeles International Airport"
toLA = g.shortestPaths(landmarks=["3484"])

# Calculate shortestPaths to "Turin Airport"
toTurin = g.shortestPaths(landmarks=["1526"])

# Join two previous dataframes
LA_TO = toLA.join(toTurin, toLA.id == toTurin.id).select(toLA["id"], toLA["name"].alias("toLA_name"), 
        toLA["distances"].alias("toLA_distance"), toTurin["name"].alias("toTurin_name"), toTurin["distances"].alias("toTurin_distances"))

LA_TO = LA_TO.withColumn("toLA_distance_numeric", col("toLA_distance").getItem("3484")).withColumn(
    "toTurin_distance_numeric", col("toTurin_distances").getItem("1526"))

# Filter those airports with less hops than Torino
LA_TO_filtered = LA_TO.filter("toLA_distance_numeric < toTurin_distance_numeric")
LA_TO_filtered.count()

# -------------------------------------------------------------------------------------------

# Find the number of airports that from there we can reach the city of Torino using less hops than to reach
# "Los Angeles International Airport"
TO_LA_filtered = LA_TO.filter("toLA_distance_numeric > toTurin_distance_numeric")
TO_LA_filtered.count()

# -------------------------------------------------------------------------------------------

# Find the number of airports that from there we can reach with the same number of hops Torino 
# and "Los Angeles International Airport"
TOLA_equal_filtered = LA_TO.filter("toLA_distance_numeric == toTurin_distance_numeric")
TOLA_equal_filtered.count()

# -------------------------------------------------------------------------------------------

# Find the number of connected components of at least two airports are there in the graph
# and the size of those connected components.
sc.setCheckpointDir("tmp_ckpts")
gClean = g.dropIsolatedVertices()
connComp = gClean.connectedComponents()
connComp.createOrReplaceTempView("connComp")
connCompClean = spark.sql('''SELECT component, count(*) FROM connComp GROUP BY component Having count(*) >= 2''')
connCompClean.distinct().count()
connCompClean.show()

# -------------------------------------------------------------------------------------------

# By considering only the subgraph of the flights that are performed by two different airlines (identified by the
# icao), each involving at least 5 citie, I am going to select two airlines to show their graphs.

# Select two Airlines with previous characteristics
# XLA (Excel Airways), GLG (Aerolineas Galapagos)
test_city = icao_joined.join(airportsDF, icao_joined.src == airportsDF.id).select(icao_joined['icao'], airportsDF['city'])
test_city.createOrReplaceTempView("test_city")
select_city = spark.sql('''
                            SELECT icao, COUNT(city)
                            FROM test_city
                            GROUP BY icao
                            HAVING COUNT(city) >= 5''')

# Create a new dataframe (graphframe) with desired columns
icao_joined = flightsClean.join(airlinesDF, flightsClean.airline_id == airlinesDF.airline_id).select(
            flightsClean["airport_source_id"].alias("src"), flightsClean["airport_destination_id"].alias("dst"),
            airlinesDF["airline_id"], airlinesDF["icao"].alias("icao"), airlinesDF["name"].alias("airline_name"))

# Create the subgraph of selected airlines
icao_joined.createOrReplaceTempView("icao_joined")
e_icao = spark.sql('''SELECT src, dst, icao, airline_name FROM icao_joined WHERE icao = "XLA" OR icao = "GLG"''')
v_cities = airportsDF.select("id", "city")
g_icao = GraphFrame(v_cities, e_icao)
g_final = g_icao.dropIsolatedVertices()

# Plot the subgraph of these flights
from graphviz import Digraph
def vizGraph(edge_list,node_list):
    Gplot=Digraph()
    edges=edge_list.collect()
    nodes=node_list.collect()
    for row in edges:
        Gplot.edge(str(row['src']),str(row['dst']),label=str(row['icao']))
    for row in nodes:
        Gplot.node(str(row['id']),label=str(row['city']))
    return Gplot

Gplot=vizGraph(g_final.edges, g_final.vertices)
Gplot
# the picture of final graph will be added at the end of the report.

# -------------------------------------------------------------------------------------------

'''Find the destination airport at minimum distance from "Tancredo Neves International Airport"
that we can reach by taking exactly 2 flights.I am going to return the destination airport and its distance
from "Tancredo Neves International Airport" by considering that we cannot come back to the 
Tancredo Neves International Airport'''

# Find the id of "Tancredo Neves International Airport"
idTN = outgoing_incoming_final.filter("name = 'Tancredo Neves International Airport'")
idTN.show(1)

# Create the motif that we cannot comeback
motifTN = g.find("(a)-[]->(b);(b)-[]->(c); !(c)-[]->(a);!(b)-[]->(a)")
fromTN = motifTN.filter("a.id = 2537")
fromTN.count()

destinationList = fromTN.select("c.id")
lat_lon_TN = airportsDF.filter("name = 'Tancredo Neves International Airport'").show(5)

lat_lon = destinationList.join(airportsDF, destinationList.id == airportsDF.id).select(
                    destinationList['id'].alias('id'), airportsDF['name'], airportsDF['latitude'], airportsDF['longitude'])

# Define a function (haversine) to calculate the distance between airports
import math
def haversine(lat1, lon1, lat, lon):
    R = 6371.0
    lat1, lon1, lat, lon = map(math.radians, [lat1, lon1, lat, lon])
    dlat = lat - lat1
    dlon = lon - lon1
    hav = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat) * math.sin(dlon / 2) ** 2
    distance = 2 * R * math.asin(math.sqrt(hav))
    return distance

# Register the function
spark.udf.register('hav', haversine)

dinstanceAirportsDF = lat_lon.selectExpr("id", "name", "hav(-19.62444305419922, -43.97194290161133, latitude, longitude) as distance").sort("distance")
dinstanceAirportsDF.show(1, truncate = False)

'''There are 39 airports  that can be reach by taking exactly two flights from 
"Tancredo Neves International Airport" that there is no return flight from those destination. 
The minimum distance from this airport with these conditions is 1008.7177113996354 which the destination 
airport is "Hercílio Luz International Airport".'''

# -------------------------------------------------------------------------------------------

# Compute the total flown distance in kilometers, considering the distance from 
# "Tancredo Neves International Airport" to the first airport summed to the distance from the first airport to
# second one (total flown distance)

bothAirportsList = fromTN.select("b.id", "c.id").filter("c.id = 2555")
bothAirportsList.show()

airportB_List = fromTN.select("b.id")

lat_lon_B = airportB_List.join(airportsDF, airportB_List.id == airportsDF.id).select(
                    airportB_List['id'].alias('id'), airportsDF['name'], airportsDF['latitude'], airportsDF['longitude'])

lat_lon_B.filter("id = 2442").show(1, truncate=False)

airport_B_C = lat_lon.selectExpr("id", "name", "hav(-34.5592, -58.4156, latitude, longitude) as distance").filter("id = 2555")
airport_B_C.show(truncate=False)

airport_A_B = lat_lon_B.selectExpr("id", "name", "hav(-19.62444305419922, -43.97194290161133, latitude, longitude) as distance")
airport_A_B.show(1, truncate=False)

'''The distance between "Tancredo Neves International Airport" and "Jorge Newbery Airpark" is 2186.1514575333313 
and the distance between "Jorge Newbery Airpark" and "Hercílio Luz International Airport" is 1210.6138281258875,
so the sum of distances is 3396.76529.'''

