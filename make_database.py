#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 14:21:11 2019

@author: danielyaeger

This is code to create the database used in the sql_workshop Jupyter notebook.
To create the database, make sure the file 'NCHS_-_Leading_Causes_of_Death__United_States.csv'
is in the same directory as this script, navigae to the directory,
and then open a terminal and type:

python make_database
"""
import numpy as np
import re
import sqlite3
import pandas as pd

def clean_names(df):
    L = []
    for col in df.columns:
        L.append(re.sub(r"\s+|-", '_', col))
    L[1] = 'Cause_Description'
    df.columns = L

def make_db(data):
    conn = sqlite3.connect("leading_cases_of_death.sqlite")
    cur = conn.cursor()
    
    # Get unique years
    yr_unique = np.sort(data.Year.unique()).tolist()
    
    # Make year table
    sql_statement1 = ("DROP TABLE IF EXISTS Year")
    sql_statement2 = '''CREATE TABLE Year(
                    YearID INTEGER PRIMARY KEY,
                    Year INTEGER NOT NULL
                    )'''
    cur.execute(sql_statement1)
    cur.execute(sql_statement2)
    
    # Insert data into year table
    for yr in yr_unique:
        cur.execute("INSERT INTO Year (Year) VALUES (?)", (yr,))
    conn.commit()
    
    # Create list of unique causes
    cause_unique = data.Cause_Name.unique().tolist()
    maxLengthCause = max([len(item) for item in cause_unique])
    cause_desc_unique = data.Cause_Description.unique().tolist()
    maxLengthDesc = max([len(item) for item in cause_desc_unique])
    
    # Create Cause table
    sql_statement1 = ("DROP TABLE IF EXISTS Cause")
    sql_statement2 = '''CREATE TABLE Cause(
                    CauseID INTEGER PRIMARY KEY,
                    Cause_Name VARCHAR({0}),
                    Cause_Description VARCHAR({1})
                    )'''.format(maxLengthCause,maxLengthDesc)
    cur.execute(sql_statement1)
    cur.execute(sql_statement2)
    
    # Insert data into Cause table
    for i in range(len(cause_unique)):
        cur.execute("INSERT INTO Cause (Cause_Name,Cause_Description) VALUES (?,?)", 
            (cause_unique[i], cause_desc_unique[i]))
    conn.commit()
    
    # Sort list of states
    state_unique = np.sort(data.State.unique()).tolist()
    maxLength = max([len(item) for item in state_unique])
    
    # Make State table
    sql_statement1 = ("DROP TABLE IF EXISTS State")
    sql_statement2 = '''CREATE TABLE State(
                    StateID INTEGER PRIMARY KEY,
                    State VARCHAR({0})
                    )'''.format(maxLength,)
    cur.execute(sql_statement1)
    cur.execute(sql_statement2)
    
    # Insert data into State
    for state in state_unique:
        cur.execute("INSERT INTO State (State) VALUES (?)", (state,))
    conn.commit()
    
    # Get dataframes from each table to merge
    df_state = pd.read_sql_query("SELECT * FROM State", conn)
    df_cause = pd.read_sql_query("SELECT * FROM Cause", conn)
    df_year = pd.read_sql_query("SELECT * FROM Year", conn)
    
    # Merge ID values to dataframe
    merged = pd.merge(data,df_state,how='outer',on=['State'])
    merged = pd.merge(merged,df_cause,how='outer',on=['Cause_Name'])
    merged = pd.merge(merged, df_year, how ='outer',on=['Year'])
    
    # Create Deaths table
    sql_statement1 = ("DROP TABLE IF EXISTS Deaths")
    sql_statement2 = '''CREATE TABLE Deaths (
                    ID INTEGER PRIMARY KEY,
                    YearID INTEGER,
                    CauseID INTEGER,
                    StateID INTEGER,
                    Deaths INTEGER,
                    Age_adjusted_Death_Rate FLOAT,
                    FOREIGN KEY (CauseID) REFERENCES Cause(CauseID),
                    FOREIGN KEY (YearID) REFERENCES Year(YearID),
                    FOREIGN KEY (StateID) REFERENCES State(StateID)
                    );'''
    cur.execute(sql_statement1)
    cur.execute(sql_statement2)
    
    # Get merged data into right format
    deaths = merged['Deaths'].values.tolist()
    age_adj_rate = merged['Age_adjusted_Death_Rate'].values.tolist()
    causeID = merged['CauseID'].values.tolist()
    yearID = merged['YearID'].values.tolist()
    stateID = merged['StateID'].values.tolist()
    
    # Insert data into Deaths table
    for i in range(len(merged)):
        cur.execute('''INSERT INTO Deaths 
                    (Deaths,Age_adjusted_Death_Rate,CauseID,YearID,StateID) 
                    VALUES (?,?,?,?,?)''',
                    (deaths[i],age_adj_rate[i],causeID[i],yearID[i],stateID[i]))
        conn.commit()
        
    # Close database
    conn.close()
    
if __name__ == "__main__":
    # This chunk of code says that if you call this script from the command line
    # it will execute the code below
    print("Importing data...")
    data = pd.read_csv('NCHS_-_Leading_Causes_of_Death__United_States.csv',',')
    print("Cleaning variable names...")
    clean_names(data)
    print("Making SQLite database...")
    make_db(data)
    try:
        conn = sqlite3.connect("leading_cases_of_death.sqlite")
        df_state = pd.read_sql_query("SELECT * FROM State", conn)
        if len(df_state) > 0:
            print("Database named leading_cases_of_death.sqlite successfully created")
        else:
            print("Problem creating database")
    except:
        print("Error creating database!")
    
    