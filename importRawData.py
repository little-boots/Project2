from credentials import postgres_user, postgres_passwd
import csv
import re
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, Column, Integer, String, inspect
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import datetime
import random

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

# Return numeric year from date string
def parseDate(date):
    dateFormat = re.compile(r'^\d{8}$')
    if not re.match(dateFormat, date): return None
    else: return int(date[-4:])

buildDB()

"""
print(session.query(Transaction.distributor_name,
                    Transaction.manufacturer_name,
                    Transaction.us_county,
                    Transaction.us_state,
                    Transaction.year,
                    Transaction.tot_dose,
                    Transaction.tot_pills).all())
                    """
