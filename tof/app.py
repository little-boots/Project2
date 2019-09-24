import os

import pandas as pd
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from flask import Flask, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

# to avoid browser to cache static assets served by Flask
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0


#################################################
# Database Setup
#################################################

# app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', '') or 'sqlite:///db/tof.sqlite'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db/tof.sqlite'
db = SQLAlchemy(app)


# Routes for website navigation purpose
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Route to Homepage
@app.route('/')
def index():
    """Go to the homepage"""
    return render_template("index.html")

# Route to Manufacturers Choropleth Visualization
@app.route('/manufacturer')
def manufacturer():
    """Go to the manufacturers analysis"""
    return render_template("manufacturer.html")


# Next routes are API endpoints used to query 
# the backend needed for all visualizations
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Used by: Manufacturers Choropleth Visualization
#------------------------------------------------
# This is an API endpoint that returns top 10 
# manufacturers ordered in descending order by 
# number of pills distributed.
@app.route('/top10manufacturers')
def top10manufacturers():
    """Return a list of top 10 manufacturers"""

    sql  = '   SELECT combined_labeler_name, SUM(total_dosage_unit) AS pills'
    sql += '     FROM total_manufacturers_county'
    sql += ' GROUP BY combined_labeler_name'
    sql += ' ORDER BY pills DESC'
    sql += ' LIMIT 10;'
    df = pd.read_sql_query(sql, db.engine)

    return jsonify(list(df['combined_labeler_name']))

# Used by: Manufacturers Choropleth Visualization
#------------------------------------------------
# This is an API endpoint that returns average number
# of pills per person at county level across all years
# (2006 to 2012).
@app.route('/pillsByManufacturer/<manufacturerName>')
def pillsByManufacturer(manufacturerName):
    """Return total number of pills per manufacturer"""

    sql  = '   SELECT fips, avg_pills_per_person'
    sql += '     FROM total_manufacturers_county'
    sql += '    WHERE combined_labeler_name = "' + manufacturerName + '";'
    df = pd.read_sql_query(sql, db.engine)
    df = df.round(4)
    df.set_index('fips', inplace=True)

    # return a list of the column names
    return df.to_json(orient='columns')


if __name__ == "__main__":
    app.run(debug=True)
