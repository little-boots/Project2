import csv
import re
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import datetime
import random
import os
import shutil

# import queue
# import threading
# import time

Base = declarative_base()

######---------------------------------#####
# Prepare database
######---------------------------------#####

class Transaction(Base):
    __tablename__ = 'transaction'
    t_id = Column(Integer, primary_key=True)
    manufacturer_name = Column(String)
    distributor_name = Column(String)
    year = Column(Integer)
    us_state = Column(String)
    us_county = Column(String)
    tot_pills = Column(Integer)
    tot_dose = Column(Integer)
    pct_null = Column(Integer)

engine = create_engine('sqlite:///db/transactions.sqlite')
Base.metadata.create_all(engine)
session = Session(engine)

######---------------------------------#####
# Import from full data
######---------------------------------#####

statelist = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA',
    'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY',
    'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
    'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]

stateChunk1 = statelist[:10]
stateChunk2 = statelist[10:20]
stateChunk3 = statelist[20:30]
stateChunk4 = statelist[30:40]
stateChunk5 = statelist[40:50]
stateChunk6 = statelist[50:]

# al = pd.read_csv('data/statelevel/AL.tsv', delimiter='\t')
# print(al.columns)

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
    print(f"States: {states}")
    print(f"{dt} (Start)")
    rowIndex = getRowIndex()

    # Iterate over all rows
    with open('data/arcos_all_washpost.tsv') as read_tsv:
        for row in csv.reader(read_tsv,
                            delimiter='\t'):


            # Output row count and time to monitor progress
            rdt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
            total += 1
            if total % 1000000 == 0:
                print(f"{rdt}: {total}")

            # First row contains header
            if total==1:
                # Initialize state TSVs
                for state in states:
                    # Delete old TSV, if one exists
                    try:
                        #os.remove(f'data/statelevel/{state}.tsv')
                        os.remove(f'state/{state}.tsv')
                    except:
                        pass
                    # Write the header to a new TSV file
                    #with open(f'data/statelevel/{state}.tsv', 'w') as write_tsv:
                    with open(f'state/{state}.tsv', 'w') as write_tsv:
                        write_tsv.write('\t'.join(row))
                continue

            # Output to state-level TSV
            else:
                us_state = row[rowIndex['BUYER_STATE']]

                # Only write results from statelist to process in chunks
                if us_state in states:
                    #with open(f'data/statelevel/{us_state}.tsv', 'a') as write_tsv:
                    with open(f'state/{us_state}.tsv', 'a') as write_tsv:
                        write_tsv.write('\t'.join(row) + '\n')

# TODO: This step could probably benefit from multithreading
def moveStateTSV_remote(states):
    for state in states:
        shutil.move(f'state/{state}.tsv', f'data/statelevel/{state}.tsv')

# TODO: This step could probably benefit from multithreading
def SQLizeStateTSV(states):
    for state in states:
        localengine = create_engine(f'sqlite:///db/statelevel/{state}.sqlite')
        #Base.metadata.create_all(localengine)
        #localsession = Session(localengine)

        print(f'Trying {state}')
        rowIndex = getRowIndex()

        df = pd.read_csv(f'data/statelevel/{state}.tsv',
                         delimiter='\t',
                         usecols=[
                             rowIndex['Combined_Labeler_Name'],
                             rowIndex['Reporter_family'],
                             rowIndex['TRANSACTION_DATE'],
                             rowIndex['BUYER_STATE'],
                             rowIndex['BUYER_COUNTY'],
                             rowIndex['QUANTITY'],
                             rowIndex['DOSAGE_UNIT'],
                             rowIndex['dos_str']
                         ],
                         engine='c',
                         skip_blank_lines=False
                         )

        # I misnamed a column...
        myDosStr = [column for column in df.columns if column.startswith('dos_str')][0]

        df['pills'] = df['QUANTITY'].fillna(0) * df['DOSAGE_UNIT'].fillna(0)
        df['mgs'] = df['pills'].fillna(0) * df[f'{myDosStr}'].fillna(0)

        df['YEAR'] = df['TRANSACTION_DATE'].astype('str').apply(parseDate)

        df_gb = df\
            .groupby(['Combined_Labeler_Name',
                            'Reporter_family',
                            'YEAR',
                            'BUYER_STATE',
                            'BUYER_COUNTY'])\
            [['pills', 'mgs']]\
            .sum()

        print(df_gb.head())

        df_gb.to_sql(f'{state}',
                     con=localengine)

def getStateRowData(state, localsession, row, rowIndex_):
    # Demographics
    manufacturer_name = row[rowIndex_['Combined_Labeler_Name']]
    distributor_name = row[rowIndex_['Reporter_family']]
    year = parseDate(row[rowIndex_['TRANSACTION_DATE']])
    us_state = row[rowIndex_['BUYER_STATE']]
    us_county = row[rowIndex_['BUYER_COUNTY']]

    # TODO: There must be a better way to do this

    # Drug
    try:
        row_quantity = float(row[rowIndex_['QUANTITY']])
    except:
        row_quantity = None

    try:
        row_unit = float(row[rowIndex_['DOSAGE_UNIT']])
    except:
        row_unit = None

    # This value is sometimes null
    try:
        row_dose = float(row[rowIndex_['dos_str']])
    except:
        row_dose = None
    try:
        row_tot_pills = row_quantity * row_unit
    except:
        row_tot_pills = None
    try:
        row_tot_dose = row_tot_pills * row_dose
    except:
        row_tot_dose = None

    # If there's a record to update, update it
    try:
        # Try writing this as pure SQL?
        thisrow = localsession\
            .query(Transaction)\
            .filter_by(manufacturer_name = manufacturer_name,
                       distributor_name = distributor_name,
                       us_state = us_state,
                       us_county = us_county,
                       year = year)\
            .first()

        if row_tot_pills is not None:
            thisrow.tot_pills += row_tot_pills
        if row_tot_dose is not None:
            thisrow.tot_dose += row_tot_dose
        # I can't actually derive this without also deriving row count
        thisrow.pct_null = 0

    # Otherwise, make a new record
    except:
        localsession.add(Transaction(manufacturer_name=manufacturer_name,
                                     distributor_name=distributor_name,
                                     year=year,
                                     us_state=us_state,
                                     us_county=us_county,
                                     tot_pills=row_tot_pills,
                                     tot_dose=row_tot_dose,
                                     pct_null=0))

def getStateTSV():
    total = 0
    dt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
    print(f"{dt} (Start)")
    rowIndex = getRowIndex()

    # Iterate over all rows
    with open('data/arcos_all_washpost.tsv') as read_tsv:
        for row in csv.reader(read_tsv,
                            delimiter='\t'):


            # Output row count and time to monitor progress
            rdt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
            total += 1
            if total % 100000 == 0:
                print(f"{rdt}: {total}")

            # First row contains header
            if total==1:
                # Initialize state TSVs
                for state in statelist:
                    # Delete old TSV, if one exists
                    try:
                        os.remove(f'data/statelevel/{state}.tsv')
                    except:
                        pass
                    # Write the header to a new TSV file
                    with open(f'data/statelevel/{state}.tsv', 'w') as write_tsv:
                        write_tsv.write('\t'.join(row))
                continue

            # Output to state-level TSV
            else:
                us_state = row[rowIndex['BUYER_STATE']]
                with open(f'data/statelevel/{us_state}.tsv', 'a') as write_tsv:
                    write_tsv.write('\t'.join(row))

def buildDB2():
    data = pd.read_csv('data/arcos_all_washpost.tsv',
                       delimiter='\t')
    return data

# Run this if you want to crash your computer
# buildDB2()

# TODO: Get random import working


def buildDB(num=-1, pct=5):
    total = 0
    if num==0: return
    rowIndex = getRowIndex()
    with open('data/arcos_all_washpost.tsv') as tsv_file:
        session.new
        dt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
        print(f"{dt} (Start)")
        for row in csv.reader(tsv_file,
                            delimiter='\t'):
            rdt = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
            total += 1
            if total==1: continue
            # Commit once every 5000 lines
            if total % 50000 == 0: session.commit()
            # Give a sense of progress
            if total % 10000 == 0:
                print(f"{rdt}: {total}")

            randnum = random.randint(1,101)
            if randnum <= pct:
                getRowData(row, rowIndex)

            # Setting num negative will loop through all
            if total > num and num > 0: break
        #session.commit()

def getRowData(row, rowIndex_):
    # Demographics
    manufacturer_name = row[rowIndex_['Combined_Labeler_Name']]
    distributor_name = row[rowIndex_['Reporter_family']]
    year = parseDate(row[rowIndex_['TRANSACTION_DATE']])
    us_state = row[rowIndex_['BUYER_STATE']]
    us_county = row[rowIndex_['BUYER_COUNTY']]

    # TODO: There must be a better way to do this

    # Drug
    try:
        row_quantity = float(row[rowIndex_['QUANTITY']])
    except:
        row_quantity = None

    try:
        row_unit = float(row[rowIndex_['DOSAGE_UNIT']])
    except:
        row_unit = None

    # This value is sometimes null
    try:
        row_dose = float(row[rowIndex_['dos_str']])
    except:
        row_dose = None
    try:
        row_tot_pills = row_quantity * row_unit
    except:
        row_tot_pills = None
    try:
        row_tot_dose = row_tot_pills * row_dose
    except:
        row_tot_dose = None

    # If there's a record to update, update it
    try:
        # Try writing this as pure SQL?
        thisrow = session\
            .query(Transaction)\
            .filter_by(manufacturer_name = manufacturer_name,
                       distributor_name = distributor_name,
                       us_state = us_state,
                       us_county = us_county,
                       year = year)\
            .first()

        if row_tot_pills is not None:
            thisrow.tot_pills += row_tot_pills
        if row_tot_dose is not None:
            thisrow.tot_dose += row_tot_dose
        # I can't actually derive this without also deriving row count
        thisrow.pct_null = 0

    # Otherwise, make a new record
    except:
        session.add(Transaction(manufacturer_name=manufacturer_name,
                                distributor_name=distributor_name,
                                year=year,
                                us_state=us_state,
                                us_county=us_county,
                                tot_pills=row_tot_pills,
                                tot_dose=row_tot_dose,
                                pct_null=0))

# Return numeric year from date string
def parseDate(date):
    dateFormat = re.compile(r'^\d+$')
    if not re.match(dateFormat, date): return None
    else: return int(date[-4:])


#getStateTSV_local(stateChunk1)
#moveStateTSV_remote(stateChunk1)
SQLizeStateTSV(stateChunk1)
