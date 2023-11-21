
import pandas as pd
import sqlite3
import json
from flask import Flask, jsonify, request

df = pd.read_excel("production.xls")
print(df.columns)




df["OIL"] = df.groupby(["API WELL  NUMBER", "QUARTER 1,2,3,4"])["OIL"].transform("sum")
df["GAS"] = df.groupby(["API WELL  NUMBER", "QUARTER 1,2,3,4"])["GAS"].transform("sum")
df["BRINE"] = df.groupby(["API WELL  NUMBER", "QUARTER 1,2,3,4"])["BRINE"].transform("sum")
df.rename(columns={'API WELL  NUMBER':'API_WELL_NUMBER'},inplace=True)
df.rename(columns={'QUARTER 1,2,3,4':'QUARTER'},inplace=True)
print(df.columns)




conn = sqlite3.connect("production_database.db")

df.to_sql("production_data", conn, index=False, if_exists="replace")

conn.close()




app = Flask(__name__)

@app.route('/data', methods=['GET'])
def get_data():
    well_id = request.args.get('well')
    
   
    conn = sqlite3.connect("production_database.db")
    
    
    query = f"SELECT SUM(OIL), SUM(GAS), SUM(BRINE) FROM production_data WHERE API_WELL_NUMBER = {well_id}"
    result = conn.execute(query).fetchone()
    
   
    conn.close()

    if result:
        data = {

            "oil": result[0],
            "gas": result[1],
            "brine": result[2]
        }
        return(json.dumps(data,sort_keys=False,indent=2))
       
    else:
        return jsonify({"error": "Well not found"}), 404
    





@app.route('/all', methods=['GET'])
def get_all():
    
    quarter = request.args.get('quarter')
 
    conn = sqlite3.connect("production_database.db")
    
    
    query2 = f"select * from Production_data where QUARTER={quarter} group by API_WELL_NUMBER"
    result2 = conn.execute(query2).fetchall()
    
   
    conn.close()
    data=[]
    if result2:
        for item in result2:
              data.append( {
            
              "API WELL ID": item[0],
              "PRODUCTION YEAR": item[1],
              "QUARTER":item[2],
              "OIL" : item[8],
              "GAS" : item[9],
              "BRINE": item[10]
          } )
        return(jsonify(data))
    else:
        return jsonify({"error": "Well not found"}), 404


if __name__ == '__main__':
    app.run(port=8080,debug=True)
    
