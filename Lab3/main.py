import os
from flask import Flask, jsonify
import psycopg2
import geopandas as gpd
import pandas as pd
from shapely import wkt
import json

app = Flask(__name__)


@app.route("/")
def hello_world():
    """Example Hello World route."""
    name = os.environ.get("NAME", "World")
    return f"Hello {name}!"

@app.route('/elevation_accuracy', methods=['GET'])
def elevation_accuracy():
    try:
        conn = psycopg2.connect(
            dbname="lab2",
            user=<user>,
            password=<password>,
            host="34.133.43.30",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM elevation_accuracy")
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame.from_records(rows, columns=columns)

        json_data = json.loads(df.to_json(orient="records"))

        cursor.close()
        conn.close()

        return jsonify(json_data)

    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/temperature_accuracy', methods=['GET'])
def temperature_accuracy():
    try:
        conn = psycopg2.connect(
            dbname="lab2",
            user=<user>,
            password=<password>,
            host="34.133.43.30",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM temperature_accuracy")
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame.from_records(rows, columns=columns)

        json_data = json.loads(df.to_json(orient="records"))

        cursor.close()
        conn.close()

        return jsonify(json_data)

    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/elevation_testing', methods=['GET'])
def elevation_testing():
    # Connect to the PostGIS database
    conn = psycopg2.connect(
        dbname="lab2",
        user=<user>,
        password=<password>,
        host="34.133.43.30",
        port="5432"
    )
    cursor = conn.cursor()

    # Execute SQL query
    sql = "SELECT *, ST_AsText(geom) as wkt_geometry FROM elevation_testing"
    cursor.execute(sql)

    # Fetch data
    rows = cursor.fetchall()

    # Convert to GeoDataFrame
    columns = [desc[0] for desc in cursor.description]
    gdf = gpd.GeoDataFrame.from_records(rows, columns=columns)

    # Convert WKT to geometry
    gdf['geometry'] = gdf['wkt_geometry'].apply(wkt.loads)
    gdf = gdf.drop(columns=['wkt_geometry'])

    # Convert GeoDataFrame to GeoJSON
    geojson_data = json.loads(gdf.to_json())

    # Close the database connection
    cursor.close()
    conn.close()

    # Return GeoJSON data
    return jsonify(geojson_data)

@app.route('/temperature_testing', methods=['GET'])
def temperature_testing():
    # Connect to the PostGIS database
    conn = psycopg2.connect(
        dbname="lab2",
        user=<user>,
        password=<password>,
        host="34.133.43.30",
        port="5432"
    )
    cursor = conn.cursor()

    # Execute SQL query
    sql = "SELECT *, ST_AsText(geom) as wkt_geometry FROM temperature_testing"
    cursor.execute(sql)

    # Fetch data
    rows = cursor.fetchall()

    # Convert to GeoDataFrame
    columns = [desc[0] for desc in cursor.description]
    gdf = gpd.GeoDataFrame.from_records(rows, columns=columns)

    # Convert WKT to geometry
    gdf['geometry'] = gdf['wkt_geometry'].apply(wkt.loads)
    gdf = gdf.drop(columns=['wkt_geometry'])

    # Convert GeoDataFrame to GeoJSON
    geojson_data = json.loads(gdf.to_json())

    # Close the database connection
    cursor.close()
    conn.close()

    # Return GeoJSON data
    return jsonify(geojson_data)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
