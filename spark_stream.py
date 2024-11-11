import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def create_keyspace(session):
    # create keyspace here

def create_table(session):
    #create table here


def insert_data(session, **kwargs):
    #insertion here

def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
        .appName()

def create_cassandra_connection():
    # creating cassandra connection


if __name__ == "__main__":
    spark_conn = create_spark_connection()
