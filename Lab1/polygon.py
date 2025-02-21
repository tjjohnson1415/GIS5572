from flask import Flask, jsonify
import psycopg2
import geopandas as gpd
from shapely import wkt
import json

app = Flask(__name__)

@app.route('/get_geojson', methods=['GET'])
def get_geojson():
    # Connect to the PostGIS database
    conn = psycopg2.connect(
        dbname="lab1",
        user="postgres",
        password="postgres",
        host="34.133.43.30",
        port="5432"
    )
    cursor = conn.cursor()

    # Execute SQL query
    sql = "SELECT *, ST_AsText(geometry_column) as wkt_geometry FROM lab1_polygon"
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

if __name__ == '__main__':
    app.run(debug=True)
