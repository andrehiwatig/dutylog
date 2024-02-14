import streamlit as st
import os
import pandas as pd
import string
import re

def rem_dup(y):
    return list(set(y))

def conv_to_string(x):
    if len(x) == 0:
        return ''
    else:
        return ' '.join([str(element) for element in x])

def incident_categorizer(logs, keywords, file_name):
    # make incident descriptions lowercase
    logs.Description = [str(x).lower() for x in list(logs.Description)]

    # remove punctuation
    translator = str.maketrans(string.punctuation, " " * len(string.punctuation))
    logs.Description = [x.translate(translator) for x in list(logs.Description)]

    # remove extra spaces
    logs.Description = logs.Description.apply(lambda x: re.sub(r'\s+', ' ', x).strip())

    # Create a dictionary of keys/values based on keywords
    dict = {} 
    for i in range(len(keywords)): 
        key = keywords.iat[i, 0] 
        value = keywords.iat[i, 1]
        if key in dict: 
            dict[key] += [value] # if the keyword is already in the dictionary, add the value to the key
        else:
            dict[key] = [value] # create a new keyword
            
    # Create a column to store incident types
    logs['Incident Type'] = pd.Series(dtype = 'str')
    logs['Incident Type'] = logs['Incident Type'].astype('object')

    # Iterate through the rows in the DataFrame
    for index, row in logs.iterrows():
        phrase = row['Description']
        split_phrases = phrase.split()  # Split the phrase into separate words
        matching_keys = []

        # Check each split phrase against the values in the dictionary
        for word in split_phrases:
            for key, values in dict.items():
                if word in values:
                    matching_keys.append(key)

        # Update Incident Type Column with the matching keys (if any)
        logs.at[index, 'Incident Type'] = matching_keys

    # Apply the remove duplicates function to the 'phrases' column
    logs['Incident Type'] = logs['Incident Type'].apply(rem_dup)

    # Column for the number of tags each row has been assigned
    logs['Number of Labels'] = logs['Incident Type'].apply(lambda x: len(x))
    
    # Create a column for each "key" in keywords
    for item in keywords['Keys']:
        logs[item] = pd.Series(dtype = 'str')
        
    # Insert a 1 in each column that matches a keyword, else 0
    for ikey, rkey in keywords.iterrows():
        colName = rkey['Keys']
        for idata, rdata in logs.iterrows():
            if rkey['Keys'] in rdata['Incident Type']:
                logs.loc[idata, colName] = 1
            else: 
                logs.loc[idata, colName] = 0

    # Create a new column to show which rows need to be reviewed.
    logs['other'] = logs.apply(lambda row: 1 if row['Number of Labels'] == 0 else 0, axis=1)
    logs.loc[logs['other'] == 1, 'Incident Type'] = 'other'

    # apply convert to string function
    logs['Incident Type'] = logs['Incident Type'].apply(conv_to_string)

    # Output File
    return logs

def location_categorizer(logs, locations, courts, file_name):
    # Make summary descriptions lowercase
    logs['Location details where incident occurred'] = [str(x).lower() for x in list(logs['Location details where incident occurred'])]
    locations['Values'] = [str(x).lower() for x in list(locations['Values'])]

    # Remove punctuation
    translator = str.maketrans(string.punctuation, " " * len(string.punctuation))
    logs['Location details where incident occurred'] = [x.translate(translator) for x in list(logs['Location details where incident occurred'])]

    # Remove extra spaces
    logs['Location details where incident occurred'] = logs['Location details where incident occurred'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())

    # iterate through each entry in the logs
    for i, log_location in enumerate(logs['Location details where incident occurred']):

        # if it already has a location tag (implemented beginning Fall 2023) skip it
        if pd.isna(logs['Building where incident occurred'][i]):
            match_found = False

            # iterate through all the location values
            for j, location_value in enumerate(locations['Values']):

                # if the entry in the logs matches an entry in the recognized location values tag it with corresponding location tag
                if location_value in log_location:
                    logs['Building where incident occurred'][i] = locations['Keys'][j]
                    match_found = True
                    break
            # if there isn't a location that matches assign it to 'Other'
            if not match_found:
                logs['Building where incident occurred'][i] = 'Other'

    # lowercase all the entries in the Building column
    logs['Building where incident occurred'] = [str(x).lower() for x in list(logs['Building where incident occurred'])]

    # perform similar alogorithm as before to standardize all location values
    for i, log_building in enumerate(logs['Building where incident occurred']):
        for j, location_value in enumerate(locations['Values']):
            if location_value == log_building:
                logs['Building where incident occurred'][i] = locations['Keys'][j]
                break
    
    # perform another similar algorithm but this time assigning a value to the location's court
    for i, building in enumerate(logs['Building where incident occurred']):
        for j, location in enumerate(courts['Value']):
            if building == location:
                logs.at[i, 'Zone'] = courts.at[j, 'Keys']
                break 
    
    # returned labeled csv file
    return logs

def duration_cleaner(file):
    file['Duration of Incident'] = file['Duration of Incident'].str.replace('1 hour 30 minutes', '90')
    file['Duration of Incident'] = file['Duration of Incident'].str.replace(' minutes', '')
    file['Duration of Incident'] = file['Duration of Incident'].str.replace('1 hour', '60')
    file['Duration of Incident'] = pd.to_numeric(file['Duration of Incident'], errors='coerce')
    return file

st.title("Duty Log Cleaner")

st.subheader("File Upload")
uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:
    file = location_categorizer(
    incident_categorizer(pd.read_csv(uploaded_file),
        pd.read_csv('reslifekeywords.csv'),
        'dutylog_labeled.csv'
        ),
    pd.read_csv('uclalocations.csv'),
    pd.read_csv('uclazones.csv'),
    'dutylog_labeled.csv'
    )
    dropped = ['Token', 'Token Used']
    file = file.drop(dropped, axis=1)
    file = duration_cleaner(file)
    btn = st.download_button(
        label="Download csv",
        data=file.to_csv(),
        file_name='Duty Log Cleaned.csv',
        mime="text/csv"
    )
