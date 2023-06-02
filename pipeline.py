import pandas as pd
import os
from rdflib import Graph, Literal, RDF, URIRef, Namespace #basic RDF handling
from rdflib.namespace import FOAF , XSD #most common namespaces
import urllib.parse #for parsing strings to URI's


def ingest(filename: str) -> pd.DataFrame:
    if os.path.exists(filename):
        data = pd.read_csv(filename)
    else:
        data = pd.DataFrame()
    return data

def clean(some_data: pd.DataFrame) -> pd.DataFrame:
    list_cols = ["name", "cabin", "boat","sex",
                 "embarked", "ticket","pclass", "survived","fare",
                 "home.dest", "body", "parch", "sibsp","age"]
    data = some_data.copy(deep=True)

    if list(data.columns) in list_cols:
        data.loc[data["name"].isna(), "name"] = "unknown"
        data.loc[data["cabin"].isna(), "cabin"] = "unknown"
        data.loc[data["boat"].isna(), "boat"] = "unknown"
        data.loc[data["sex"].isna(), "sex"] = "unknown"
        data.loc[data["embarked"].isna(), "embarked"] = "unknown"
        data.loc[data["ticket"].isna(), "ticket"] = "unknown"
        data.loc[data["pclass"].isna(), "pclass"] = 0
        data.loc[data["survived"].isna(), "survived"] = 0
        data.loc[data["fare"].isna(), "fare"] = -1
        data.loc[data["home.dest"].isna(), "home.dest"] = "unknown"
        data.loc[data["body"].isna(), "body"] = -1
        data.loc[data["parch"].isna(), "parch"] = 0
        data.loc[data["sibsp"].isna(), "sibsp"] = 0
        data.loc[data["age"].isna(), "age"] = 0
        data.loc[:, "name"] = data["name"].str.replace('(', '\(')
        data.loc[:, "name"] = data["name"].str.replace(')', '\)')

    return data

def serialize(some_data: pd.DataFrame, filename: str) -> bool:
    some_data.to_csv(filename)
    return os.path.exists(filename)

def extract_passengers(some_data: pd.DataFrame) -> pd.DataFrame:
    list_cols = ["name", "sex", "age"]
    return some_data.loc[:,list_cols].copy(deep=True)

def transform_passengers_to_rdfs(some_passengers: pd.DataFrame) -> Graph :
    ps_data = some_passengers.copy(deep=True)
    g = Graph()
    ppl = Namespace('http://example.org/people/')
    schema = Namespace('http://schema.org/')
    for index, row in ps_data.iterrows():
        g.add((URIRef(ppl + str(index)), RDF.type, XSD.string))
       # g.add((URIRef(ppl + row['Name']), URIRef(schema + 'name'), Literal(row['Name'], datatype=XSD.string)))
       # g.add((URIRef(ppl + row['Name']), FOAF.age, Literal(row['Age'], datatype=XSD.integer)))
       # g.add((URIRef(ppl + row['Name']), URIRef(schema + 'address'), Literal(row['Address'], datatype=XSD.string)))
       # g.add((URIRef(loc + urllib.parse.quote(row['Address'])), URIRef(schema + 'name'),
       #        Literal(row['Address'], datatype=XSD.string)))
    return g