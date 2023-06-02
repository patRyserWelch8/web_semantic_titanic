import pandas as pd
import numpy as np
import os
from rdflib import Graph, Literal, RDF, URIRef, Namespace #basic RDF handling
from rdflib.namespace import FOAF , XSD #most common namespaces
import urllib.parse #for parsing strings to URI's

GENDER: str = Namespace('http://example.org/gender/')
SCHEMA: str = Namespace('http://schema.org/')
PEOPLE: str = Namespace('http://example.org/titanic/passenger/')
MAN:    str = "https://en.wikipedia.org/wiki/Man"
WOMAN:  str = "https://en.wikipedia.org/wiki/Woman"

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
    genders = get_gender_url(MAN, "male")  + get_gender_url(WOMAN,"female")
    for tuple in genders:
        g.add(tuple)

    for index, row in ps_data.iterrows():
        uri = get_uri(index)
        g.add(get_passenger(uri))
        g.add(get_passenger_name(uri, str(row['name'])))
        g.add(get_passenger_age(uri, row['age']))
        g.add(get_passenger_gender(uri, row['sex']))
  #      #g.add((uri, URIRef(schema + 'sex'), Literal(row['sex'], datatype=XSD.string)))

    return g

def get_uri(pass_id: int) -> str:
    return PEOPLE + str(pass_id)

def get_name(raw_name:str) -> str:
    name : str = ""
    if raw_name == 'nan':
        name = "unknown"
    else:
        if raw_name.__contains__('('):
            name = raw_name.split("(")
            name = name[1]
            name = name[:-1]
        else:
            if raw_name.__contains__("unknown"):
                name = raw_name
            else:
                name = raw_name.split(", ")
                surname = name[0]
                if len(name) <= 0:
                    name = name[0]
                else:
                    first_name = str(name[1])
                    first_name = first_name.split(". ")
                    first_name = first_name[1]
                    name = first_name + " " + surname
    return name

def get_gender_url(an_url:str, a_gender: str) -> []:
    return [(URIRef(an_url), RDF.type, FOAF.page),
            (URIRef(GENDER + a_gender), RDF.type, FOAF.gender),
            (URIRef(GENDER + a_gender), URIRef(SCHEMA + "is_described_by"), URIRef(an_url))]

def get_passenger(uri:str) -> ():
    return (URIRef(uri), RDF.type, FOAF.Person)

def get_passenger_name(uri:str, pass_name:str) -> ():
    predicate: str = SCHEMA + "is_named"
    name : str      = get_name(pass_name)
    return (URIRef(uri), URIRef(predicate), Literal(name, datatype=XSD.string))

def get_passenger_age(uri:str, pass_age:float) -> ():
    predicate: str = SCHEMA + "age_recorded_is"
    if np.isnan(pass_age):
        return (URIRef(uri), URIRef(predicate), Literal(pass_age, datatype=XSD.integer))
    else:
        return (URIRef(uri), URIRef(predicate), Literal(pass_age, datatype=XSD.integer))

def get_passenger_gender(uri:str, gender:str) -> ():
    predicate = SCHEMA + "is"
    if gender == "female":
        return (URIRef(uri), URIRef(predicate), URIRef(GENDER + "female"))
    elif gender == "male":
        return (URIRef(uri), URIRef(predicate), URIRef(GENDER + "male"))
    else:
        return (URIRef(uri), URIRef(predicate), URIRef(GENDER + "unknown"))