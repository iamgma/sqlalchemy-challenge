import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from dateutil.relativedelta import relativedelta

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

measurement = Base.classes.measurement
station = Base.classes.station

app = Flask(__name__)

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<end>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    # Design a query to retrieve the last 12 months of precipitation data and plot the results. 
    # Starting from the most recent data point in the database. 
    most_recent_date_string = session.query(measurement.date).order_by(measurement.date.desc()).first()[0]
    most_recent_date_date = dt.datetime.strptime(most_recent_date_string,'%Y-%m-%d').date()

    # Calculate the date one year from the last date in data set.
    one_year_ago = most_recent_date_date - relativedelta(years=1)

    # Perform a query to retrieve the data and precipitation scores
    precip_data = session.query(measurement.date,func.sum(measurement.prcp).label("prcp")).\
        filter(measurement.date <= most_recent_date_date).\
        filter(measurement.date >= one_year_ago).\
        group_by(measurement.date).all()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    precip_df = pd.DataFrame(precip_data)

    # Sort the dataframe by date
    precip_df.sort_values(by=['date'], ascending=False)
    precip_df['prcp']=precip_df['prcp'].round(2)
    precip_df.set_index('date',inplace=True)
    session.close()
    return jsonify(precip_df.to_dict())

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    station_counts = session.query(measurement.station,func.count(measurement.station).label('counts')).\
        group_by(measurement.station).\
        order_by(func.count(measurement.station).desc()).all()
    session.close()
    station_df = pd.DataFrame(station_counts)
    station_df.sort_values(by=['counts'],ascending=False)
    station_df.set_index('station',inplace=True)
    return (station_df.to_dict())


@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    most_active_station = session.query(measurement.station,func.count(measurement.station)).group_by(measurement.station).\
        order_by(func.count(measurement.station).desc()).all()[0][0]
    mrd_active = session.query(measurement.date).filter(measurement.station == most_active_station).order_by(measurement.date.desc()).first()[0]
    mrd_active_date = dt.datetime.strptime(mrd_active,'%Y-%m-%d').date()
    mrd_one_year_ago = mrd_active_date - relativedelta(years=1)
    most_active_oneyear_data = session.query(measurement.id,measurement.station,measurement.date,measurement.prcp,measurement.tobs).\
    filter(measurement.station == most_active_station).\
    filter(measurement.date <= mrd_active_date).\
    filter(measurement.date >= mrd_one_year_ago).all()

    active_station_df = pd.DataFrame(most_active_oneyear_data)
    session.close()
    return (active_station_df.to_json(orient="records"))

@app.route("/api/v1.0/<start>")
def start(start):
    session = Session(engine)
    input_date = dt.datetime.strptime(start,'%Y-%m-%d').date()
    precip_data = session.query(measurement.date,func.sum(measurement.prcp).label("prcp")).\
        filter(measurement.date >= input_date).\
        group_by(measurement.date).all()
    precip_df = pd.DataFrame(precip_data)
    session.close()
    tmin = precip_df['prcp'].describe()['min']
    tmax = precip_df['prcp'].describe()['max']
    tavg = precip_df['prcp'].describe()['mean']
    temp_dict = {'tmin' : tmin,
                 'tmax' : tmax,
                 'tavg' : tavg
                 }
    return jsonify(temp_dict)
    

@app.route("/api/v1.0/<start>/<end>")
def startend(start,end):
    session = Session(engine)
    input_start = dt.datetime.strptime(start,'%Y-%m-%d').date()
    input_end = dt.datetime.strptime(end,'%Y-%m-%d').date()
    precip_data = session.query(measurement.date,func.sum(measurement.prcp).label("prcp")).\
        filter(measurement.date <= input_end).\
        filter(measurement.date >= input_start).\
        group_by(measurement.date).all()
    
    session.close()
    precip_df = pd.DataFrame(precip_data)
    tmin = precip_df['prcp'].describe()['min']
    tmax = precip_df['prcp'].describe()['max']
    tavg = precip_df['prcp'].describe()['mean']
    temp_dict = {'tmin' : tmin,
                 'tmax' : tmax,
                 'tavg' : tavg
                 }
    return jsonify(temp_dict)


if __name__ == "__main__":
    app.run(debug=True)