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

#################################################
# Database Setup
#################################################

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db/tof.sqlite'
db = SQLAlchemy(app)

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(db.engine, reflect=True)

# save references to each table
HeatTable = Base.classes.heatTable


# Routes for website navigation purpose
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Route to Homepage
@app.route("/")
def index():
    """Return the homepage."""
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

# Route to Heatmap
@app.route("/heat")
def heat():
    """Return the homepage."""
    return render_template("heat.html")

# Route to Markermap
@app.route("/marker")
def marker():
    """Return the homepage."""
    return render_template("marker.html")

# Route to Clustermap
@app.route("/cluster")
def cluster():
    """Return the homepage."""
    return render_template("cluster.html")

# Route to get data for heat, marker and cluster map
@app.route("/heatlist")
def heatlist():
    """Return data to build the heat template """

    heatQuery = db.session.query(HeatTable).statement
    my_df = pd.read_sql_query(heatQuery, db.session.bind)
    my_df = my_df.round({'per_capita_usage': 0, 'addiction_index': 2})

    my_list = {
        "len": len(my_df),
        "lat": my_df.lat.values.tolist(),
        "lon": my_df.lon.values.tolist(),
        "state_county": my_df.state_county.values.tolist(),
        "per_capita_usage": my_df.per_capita_usage.values.tolist(),
        "addiction_index": my_df.addiction_index.values.tolist()
    }

    # Format the data to send as json   
    return jsonify(my_list)

# the route to render the sankey diagram
@app.route("/sankey")
def sankey():
    return render_template("sankey.html")

# the route to render the chart.js line chart
@app.route("/chartjs")
def chartjs():
    return render_template("chartjs.html")

# this route provides the data for the state dropdown menus
@app.route("/states")
def states():
    engine = create_engine("sqlite:///db/arcos_data.sqlite")
    connection = engine.connect()

    result = connection.execute("SELECT DISTINCT us_state FROM tblByCountyAndYear ORDER BY us_state").fetchall()

    connection.close()

    states = []
    for row in result:
        states.append(row[0])

    return jsonify(states)

# this route provides the data for the county dropdown for a given state
@app.route("/counties/<state>")
def counties(state):
    engine = create_engine("sqlite:///db/arcos_data.sqlite")
    connection = engine.connect()

    result = connection.execute(f"SELECT DISTINCT us_county FROM tblByCountyAndYear WHERE us_state = '{state}' ORDER BY us_county").fetchall()

    connection.close()

    counties = []
    for row in result:
        counties.append(row[0])

    return jsonify(counties)

# this route provides the line chart data for a given state and county
@app.route("/data/<state>/<county>")
def chartData(state,county):

    # get data from the db
    engine = create_engine("sqlite:///db/arcos_data.sqlite")
    connection = engine.connect()

    # add state and nation views to the db and replace the state and nation queries below with queries to those views. faster/more efficient?
    county_result = connection.execute(f"SELECT us_state, us_county, year, sum(tot_pills) FROM tblByCountyAndYear WHERE us_state = '{state}' AND us_county = '{county}' GROUP BY us_state, us_county, year ORDER BY year").fetchall()
    state_result = connection.execute(f"SELECT us_state, year, sum(tot_pills) FROM tblByCountyAndYear WHERE us_state = '{state}' GROUP BY us_state, year ORDER BY year").fetchall()
    nation_result = connection.execute(f"SELECT year, sum(tot_pills) FROM tblByCountyAndYear GROUP BY year ORDER BY year").fetchall()
    county_pop = connection.execute(f"SELECT us_county, year, population FROM tblPopByCountyAndYear WHERE us_state = '{state}' AND us_county = '{county}' ORDER BY year").fetchall()
    state_pop = connection.execute(f"SELECT us_state, year, sum(population) FROM tblPopByCountyAndYear WHERE us_state = '{state}' GROUP BY us_state, year ORDER BY year").fetchall()
    nation_pop = connection.execute(f"SELECT year, sum(population) FROM tblPopByCountyAndYear GROUP BY year ORDER BY year").fetchall()

    # close the db connection
    connection.close()

    # the list of year should always be these seven years:  [2006,2007,2008,2009,2010,2011,2012]
    years = []

    # these should be 7 elements long, one for each year
    tot_pills_county = []
    tot_pills_state = []
    tot_pills_nation = []

    # these should be 7 elements long, one for each year
    pop_county = []
    pop_state = []
    pop_nation = []

    # these will hold the per capita calculations
    ppc_county = []
    ppc_state = []
    ppc_nation = []

    for row in county_result:
        years.append(row[2])
        tot_pills_county.append(row[3])
    
    for row in state_result:
        tot_pills_state.append(row[2])
    
    for row in nation_result:
        tot_pills_nation.append(row[1])

    for row in county_pop:
        pop_county.append(row[2])

    for row in state_pop:
        pop_state.append(row[2])
    
    for row in nation_pop:
        pop_nation.append(row[1])
    
    for i in range(0,7):
        ppc_county.append(tot_pills_county[i]/pop_county[i])
        ppc_state.append(tot_pills_state[i]/pop_state[i])
        ppc_nation.append(tot_pills_nation[i]/pop_nation[i])

    #create the dictionary to return
    data = {"years":years,"ppc_county":ppc_county,"ppc_state":ppc_state,"ppc_nation":ppc_nation}

    return jsonify(data)

# this route provide the sankey chart data for a given state and county
@app.route("/sankeyData/<state>/<county>")
def sankeyData(state,county):

    # connect to the db
    engine = create_engine("sqlite:///db/arcos_data.sqlite")
    connection = engine.connect()

    sql = f"SELECT * FROM tblByCountyAndYear WHERE us_state = '{state}' AND us_county = '{county}'"
    county_data = pd.read_sql_query(sql,connection)

    # close the db
    connection.close()

    # get the part of the data frame that we care about for the sankey
    smaller_df = county_data[['manufacturer_name','distributor_name','tot_pills']].copy()

    # group by manufactorer and distributor
    smaller_grouped = smaller_df.groupby(['manufacturer_name','distributor_name'])

    # sum total_pills and then use reset_index() to convert back to a dataframe
    df = smaller_grouped['tot_pills'].sum().reset_index()

    # sort from largest to smallest total_pills
    df.sort_values('tot_pills',ascending=False,inplace=True)

    # set the index back to 0, 1, 2, etc.
    df.index = range(len(df.index))

    # grab only the 20 largest manufacturer/distributor combinations 
    df = df.iloc[:20]

    # build a list of dictionaries of nodes {"node":<integer>,"name":<string>}
    nodes = []
    node_counter = 0

    # loop through the manufacturers
    for i,row in df.iterrows():
        if not any(node.get('name', None) == row[0] for node in nodes):
            nodes.append({"node":node_counter,"name":row[0]})
            node_counter = node_counter + 1

    # loop through the buyers
    for i,row in df.iterrows():
        if not any(node.get('name', None) == row[1] for node in nodes):
            nodes.append({"node":node_counter,"name":row[1]})
            node_counter = node_counter + 1

    # create a dataframe from the dictionary of nodes
    nodes2 = pd.DataFrame.from_dict(nodes)

    # join the nodes dataframe to the grouped df to associate the source and target info with the proper nodes
    temp1 = pd.merge(nodes2,df, how = 'inner', left_on = 'name',right_on ='manufacturer_name')
    temp2 = pd.merge(nodes2,temp1, how = 'inner', left_on = 'name', right_on = 'distributor_name')
    temp3 = temp2.drop(columns = ['name_x', 'name_y'])

    # rename some columns just for the sake of clarity
    temp4 = temp3.rename(columns={'node_y':'source','node_x':'target','tot_pills':'value'})

    # create the dataframe of links
    links_df = temp4[['source','target','value']].sort_values('value',ascending=False)

    # build dictionary of links, e.g. {"source":0,"target":2,"value":1}
    # convert Pandas integer datatype to Python integer datatypes before jsonify
    links = []
    for i,row in links_df.iterrows():
        links.append({"source":int(row[0]),"target":int(row[1]),"value":int(row[2])})  
   
    sankey_dict = {"nodes":nodes,"links":links}

    return jsonify(sankey_dict)


if __name__ == "__main__":
    app.run()
