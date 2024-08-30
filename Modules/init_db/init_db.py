import sqlite3
import os
import json
import pandas as pd


def connDb(dbname):
    conn = sqlite3.connect(dbname)
    return conn

