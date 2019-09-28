import csv
import re
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import datetime
import os
import shutil

Base = declarative_base()

######---------------------------------#####
# Prepare database
######---------------------------------#####

######---------------------------------#####
# Import from full data
######---------------------------------#####

statelist = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', # 1
    'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', # 2
    'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', # 3
    'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', # 4
    'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', # 5
    'WY'                                                        # 6
]

stateChunk1 = statelist[:10]
stateChunk2 = statelist[10:20]
stateChunk3 = statelist[20:30]
stateChunk4 = statelist[30:40]
stateChunk5 = statelist[40:50]
stateChunk6 = statelist[50:]

# Return numeric year from date string
def parseDate(date):
    dateFormat = re.compile(r'^\d{8}$')
    if not re.match(dateFormat, date): return None
    else: return int(date[-4:])

# Get a dictionary mapping column names to row index
def getRowIndex():
    total = 0
    with open('data/arcos_all_washpost.tsv') as tsv_file:
        for row in csv.reader(tsv_file,
                            delimiter='\t'):
            total += 1
            rowIndex = {}
            for index, item in enumerate(row):
                rowIndex[item] = index
            if total > 0: break
        return rowIndex

# Too slow when I read and write on external HD.  Write locally instead.
def getStateTSV_local(states):
    total = 0
    dt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
    print(f"Processing states: {states}")
    print(f"{dt}: Start")
    rowIndex = getRowIndex()

    # Iterate over all rows
    with open('data/arcos_all_washpost.tsv') as read_tsv:
        for row in csv.reader(read_tsv,
                            delimiter='\t'):

            # Output row count and time to monitor progress
            dt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
            total += 1
            if total % 1000000 == 0:
                mils = total / 1000000;
                print(f"{dt}: Reading data/arcos_all_washpost.tsv, line {mils}M")

            # First row contains header
            if total==1:
                # Initialize state TSVs
                for state in states:
                    # Delete old TSV, if one exists
                    try:
                        os.remove(f'state/{state}.tsv')
                    except:
                        pass
                    # Write the header to a new TSV file
                    with open(f'state/{state}.tsv', 'w') as write_tsv:
                        write_tsv.write('\t'.join(row))
                continue

            # Output to state-level TSV
            else:
                us_state = row[rowIndex['BUYER_STATE']]

                # Only write results from statelist to process in chunks
                if us_state in states:
                    with open(f'state/{us_state}.tsv', 'a') as write_tsv:
                        write_tsv.write('\t'.join(row) + '\n')

def moveStateTSV_remote(states):
    for state in states:
        # Print a progress message
        dt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
        try:
            size = os.path.getsize(f'state/{state}.tsv')
            sizeMB = round(size * 1024**-2, 1)
            print(f'{dt}: Preparing to move {state} ({sizeMB} M)')
        except:
            print(f'{dt}: Preparing to move {state} (size unknown)')
        shutil.move(f'state/{state}.tsv', f'data/statelevel/{state}.tsv')

def buyerCat(buyStr):
    if 'PRACTITIONER' in buyStr: return 'PRACTITIONER'
    elif 'PHARMACY' in buyStr: return 'PHARMACY'

# Loop over all the TSVs, build dataframes, merge, and output
def buildSQL(states=statelist,
             dbname='transactions'):
    try:
        os.remove(f'db/{dbname}.sqlite')
    except OSError:
        pass
    localengine = create_engine(f'sqlite:///db/{dbname}.sqlite')
    Base.metadata.create_all(localengine)

    rowIndex = getRowIndex()
    for index, state in enumerate(states):
        dt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
        print(f'{dt}: {state} Start')
        df = pd.read_csv(f'data/statelevel/{state}.tsv',
                         delimiter='\t',
                         usecols=[
                             rowIndex['Combined_Labeler_Name'],
                             rowIndex['Reporter_family'],
                             rowIndex['BUYER_NAME'],
                             rowIndex['BUYER_STATE'],
                             rowIndex['BUYER_COUNTY'],
                             rowIndex['TRANSACTION_DATE'],
                             rowIndex['DOSAGE_UNIT'],
                             rowIndex['REPORTER_BUS_ACT'],
                             rowIndex['BUYER_BUS_ACT']
                         ],
                         engine='c',
                         skip_blank_lines=False
                         )

        # Rename practitioners to reduce data
        df['buy_bus'] = df['BUYER_BUS_ACT'].apply(buyerCat)

        # Derived variables
        df['YEAR'] = df['TRANSACTION_DATE'].astype('str').apply(parseDate)
        df['pills'] = df['DOSAGE_UNIT'].fillna(0)

        # Rename practiioners so you don't end up with too much data
        df.loc[df['buy_bus'] == 'PRACTITIONER', 'BUYER_NAME'] =\
            'JOE PRACTIONER, M.D.'

        df_gb = df\
            .groupby(['Combined_Labeler_Name',
                      'Reporter_family',
                      'BUYER_NAME',
                      'BUYER_STATE',
                      'BUYER_COUNTY',
                      'YEAR',
                      'REPORTER_BUS_ACT',
                      'BUYER_BUS_ACT'])\
            ['pills']\
            .sum()\
            .reset_index()

        reduced = df_gb\
            .rename(columns={'Combined_Labeler_Name': 'manufacturer_name',
                             'Reporter_family':       'distributor_name',
                             'BUYER_NAME':            'buyer_name',
                             'BUYER_STATE':           'us_state',
                             'BUYER_COUNTY':          'us_county',
                             'YEAR':                  'year',
                             'REPORTER_BUS_ACT':      'reporter_bus',
                             'buy_bus':               'buyer_bus',
                             'pills':                 'tot_pills'})

        dt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
        print(f'{dt}: {state} ready to merge')
        print(reduced.head())

        if index == 0:
            df_main = reduced
        else:
            print('Merging')
            df_main = df_main.merge(reduced,
                                    # Different states, no rows should match
                                    on=df_main.columns.tolist(),
                                    how='outer')

    df_main.to_sql(f'total',
                 con=localengine)

# Can't actually run this beacuse there is not sufficient disk space
# Instead, run getState and moveState in small batches of 10 or so
# Finally, run buildSQL on the full state list
# Could set up this function to iterate in chunks over the state list but...
# The DB is already built.
def runAllStates(states):
    getStateTSV_local(states)
    moveStateTSV_remote(states)
    buildSQL(states)

buildSQL(states=['KY', 'WV', 'VA', 'SC'],
         dbname='elChapo')
