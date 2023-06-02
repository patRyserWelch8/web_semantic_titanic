import pipeline as pl
from rdflib import Graph

print("hi")
RAW_DATA = "data/titanic.csv"
CLEAN_DATA = "data/clean_titanic.csv"

ingested_data  = pl.ingest(RAW_DATA)

print(ingested_data.shape)
print(ingested_data.dtypes)

cleaned_data = pl.clean(ingested_data)

print(cleaned_data.shape)
print(cleaned_data.dtypes)

outcome = pl.serialize(cleaned_data,CLEAN_DATA)
print(outcome)

passengers = pl.extract_passengers(cleaned_data)
print(passengers.shape)
print(passengers.dtypes)
print(passengers[:10])

passenger_graph : Graph = pl.transform_passengers_to_rdfs(passengers[:10])
print(passenger_graph.serialize(format='turtle').decode('UTF-8'))