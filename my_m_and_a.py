

import pandas as pd
import numpy as np
import csv
import sqlite3 as sq
from my_ds_babel import csv_to_sql
from io import StringIO

def first_input(csv):
    df = pd.read_csv(csv)
    
    # Making first input doc looking alike as the final doc
    
    #1: Deleting the row 
    df = df.drop(['UserName'], axis = 1)
    # Renaming the columns names
    df.rename(columns = {'Gender':'gender', 'FirstName':'firstname',
                              'LastName':'lastname', 'Email':'email', 'Age':'age', 'City':'city', 'Country':'country'}, inplace = True)
    
    #2: Cleaning every column
    
    # Gender
    # Making everything look the same
    df = df.replace({'gender' : {'0':"Male",'1':"Female", "F":"Female", "M":"Male"}})
    
    # Firstname
    # Formatting the writing
    df["firstname"] = df["firstname"].str.lower()
    df["firstname"] = df["firstname"].str.title()
    # Deleting unnecessary characters
    df['firstname'] = df['firstname'].str.replace('\\', '', regex = False).str.replace('""', '', regex = False)
    #print(df1)
    
    # LastName
    df["lastname"] = df["lastname"].str.lower()
    df["lastname"] = df["lastname"].str.title()
    # Deleting unnecessary characters / replacement
    df['lastname'] = df['lastname'].str.replace('\\', '', regex = False).str.replace('""', '', regex = False)
        
    # Email
    df["email"] = df["email"].str.lower()
    
    # Age is okay

    # City
    df["city"] = df["city"].str.lower()
    df["city"] = df["city"].str.title()
    # Deleting unnecessary characters / replacement
    df['city'] = df['city'].str.replace('_', ' ', regex = False).str.replace('-', ' ', regex = False)
    
    # Country
    df["country"] = df["country"].str.lower()
    df["country"] = df["country"].str.title()
    df["country"] = df["country"].str.replace('.', '', regex = False)
    # Deleting unnecessary characters / replacement
    df = df.replace({'country' : {"United State Of America":"USA", 'US':"USA",'12':"USA", "USAA":"USA", "Usa":"USA", "Us":"USA", "Us":"USA"}})
    df['country'] = df['country'].replace(np.nan, 'USA', regex = False)

    # 3: Creating additional columns (created_at, referral)
    df['created_at'] = "Plastic Free Boutique"
    df['referral'] = "Only Wood Box. Database 1"
    
    csv_string_1 = df.to_csv(index = None)
    #object1 = StringIO(csv_string_1)
    
    return csv_string_1

csv1 = 'https://storage.googleapis.com/qwasar-public/only_wood_customer_us_1.csv'
content_database_1 = first_input(csv1)


def second_input(csv):
    
    #1: Creating new header in accordance with the final doc
    header_0 = ["age", "city", "gender", "name_surname", "email", "country"]
    
    # Right order of columns
    header = ["gender", "firstname", "lastname", "email", "age", "city", "country"]
    
    # Opening the file and creating a header (header_0)
    df = pd.read_csv(csv2, sep=';', names = header_0)
    
    # Splitting one column into two separate columns
    df[['firstname','lastname']] = df.name_surname.str.split(expand = True)
    df = df.drop(['name_surname'], axis = 1)
    
    # Changing the order of the columns
    df = df.loc[:,header]
    
    #2: Cleaning every column
    
    # Gender
    df = df.replace({'gender' : {'0':"Male",'1':"Female", "F":"Female", "M":"Male"}})
    
    # Firstname
    # Formatting the writing
    df["firstname"] = df["firstname"].str.lower()
    df["firstname"] = df["firstname"].str.title()
    # Deleting unnecessary characters
    df['firstname'] = df['firstname'].str.replace('\\', '', regex = False).str.replace('""', '', regex = False)

    # LastName
    df["lastname"] = df["lastname"].str.lower()
    df["lastname"] = df["lastname"].str.title()
    # Deleting unnecessary characters / replacement
    df['lastname'] = df['lastname'].str.replace('\\', '', regex = False).str.replace('""', '', regex = False).str.replace('"', '', regex = False)
    
    # Email
    df["email"] = df["email"].str.lower()
    
    # Age is okay
    df['age'] = df['age'].str.replace('years', '', regex = False).str.replace('year', '', regex = False).str.replace('yo', '', regex = False)
    
    # City
    df["city"] = df["city"].str.lower()
    df["city"] = df["city"].str.title()
    # Deleting unnecessary characters / replacement
    df['city'] = df['city'].str.replace('_', ' ', regex = False).str.replace('-', ' ', regex = False)
    
    # Country 
    df['country'] = df['country'].replace(np.nan, 'USA', regex = False)
    
    # 3: Creating additional columns (created_at, referral)
    df['created_at'] = "Plastic Free Boutique"
    df['referral'] = "Only Wood Box. Database 2"
    
    csv_string_2 = df.to_csv(index = None)
    #object2 = StringIO(csv_string_2)
    
    return csv_string_2
    

csv2 = 'https://storage.googleapis.com/qwasar-public/only_wood_customer_us_2.csv'
content_database_2 = second_input(csv2)

def third_input(csv):
    
    # Opening file
    df = pd.read_csv(csv)
    
    #1: Preparing dataset
    
    # Deleting unnecesary words string, integer
    df['Gender'] = df['Gender'].str.replace('string_', '').str.replace('integer_', '')
    
    # Splitting everything
    df[["gender", "name", "email", "age", "city", "country"]] = df.Gender.str.split('\t', expand = True)
    
    # Dropping old columns
    df = df.drop(['Gender', 'Name', 'Email', 'Age', 'City', 'Country'], axis = 1)
    
    # Splitting Name column into name & surname columns
    df[['firstname','lastname']] = df.name.str.split(' ', expand = True)
    
    # Dropping Name column
    df = df.drop(['name'], axis = 1)
    
    # Changing the order of the columns in the dataset
    header = ["gender", "firstname", "lastname", "email", "age", "city", "country"]
    df = df.loc[:,header]
    
    #2: Cleaning every column
    
    # Gender
    df = df.replace({'gender' : {'boolean_0':"Male",'boolean_1':"Female", "character_M":"Male"}})
    
    # Firstname
    df["firstname"] = df["firstname"].str.lower()
    df["firstname"] = df["firstname"].str.title()
    
    # LastName
    df["lastname"] = df["lastname"].str.lower()
    df["lastname"] = df["lastname"].str.title()
    # Deleting unnecessary characters / replacement
    df['lastname'] = df['lastname'].str.replace('\ ', '', regex = False).str.replace('"', '', regex = False)
    
    # Email
    df["email"] = df["email"].str.lower()
    
    # Age
    df['age'] = df['age'].str.replace('years', '', regex = False).str.replace('year', '', regex = False).str.replace('yo', '', regex = False).str.replace('"', '', regex = False)
    
    # City
    df["city"] = df["city"].str.lower()
    df["city"] = df["city"].str.title()
    # Deleting unnecessary characters / replacement
    df['city'] = df['city'].str.replace('_', ' ', regex = False).str.replace('-', ' ', regex = False)
    
    # Country
    df = df.replace({'country' : {"United State Of America":"USA", 'US':"USA", "USAA":"USA", "Usa":"USA", "Us":"USA", "Us":"USA", "United_State_Of_America":"USA", "1":"USA", "u.s.a.":"USA", "united states of america": "USA", "United states of america":"USA", "us":"USA", "U.S.A.":"USA", "usa":"USA", "U.s.a.":"USA", "U.s.":"USA", "u.s.":"USA", "U.S.":"USA", "United state of america":"USA", "United-State-Of-America":"USA", "UNITED STATE OF AMERICA":"USA", "united state of america":"USA" }})
    
    # 3: Creating additional columns (created_at, referral)
    df['created_at'] = "Plastic Free Boutique"
    df['referral'] = "Only Wood Box. Database 3"
    
    csv_string_3 = df.to_csv(index = None)
    #object3 = StringIO(csv_string_3)
    
    return csv_string_3

csv3 = 'https://storage.googleapis.com/qwasar-public/only_wood_customer_us_3.csv'
content_database_3 = third_input(csv3)
#print(type(content_database_3))

def my_m_and_a(csv1, csv2, csv3):
    
    
    df1 = pd.DataFrame([row.split(',') for row in csv1.split('\n')])
    df2 = pd.DataFrame([row.split(',') for row in csv2.split('\n')])
    df3 = pd.DataFrame([row.split(',') for row in csv3.split('\n')])
    
    
    frames = [df1, df2, df3]
    #frames = [pandas1, pandas2, pandas3]
    res1 = pd.concat(frames)
    
    # Dropping NAN values
    res_f = res1.dropna()
    
    # Headers check - changing it to the first row values
    header = ["gender", "firstname", "lastname", "email", "age", "city", "country", "created_at", "referral"]
    res_f.columns = header
    
    # Deleting the first row values
    res_f = res_f.drop(index=0)
    
    # Changing index to start from 0 not 1
    res_f.index = res_f.index - 1
    #print(res_f.head(5))
    
    # Converting Pandas Df to csv
    merged_csv_one = res_f.to_csv(index = None)
    
    return merged_csv_one

merged_csv = my_m_and_a(content_database_1, content_database_2, content_database_3)


def main():
    merged_csv = my_m_and_a(content_database_1, content_database_2, content_database_3)
    csv_to_sql(merged_csv, 'plastic_free_boutique.sql','customers')

if __name__=="__main__":
    main()






# to SEE the RESULTS delete # sign in front of "csv_to_sql()" 