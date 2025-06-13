from fastapi import FastAPI, HTTPException
import sqlite3
import pandas as pd

app = FastAPI(title="Cube Analytics API")

DB_PATH = "/app/population_survey_project/spark_streaming/cube_analytics.db"

def read_cube_table(table_name: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {"message": "Bienvenue dans l'API des cubes analytiques 🎲"}

@app.get("/cubes")
def list_cubes():
    return [
        "cube_health_18_34",
        "cube_health_35_64",
        "cube_health_65_plus",
        "cube_self_employment",
        "cube_income"
    ]

@app.get("/cubes/{cube_name}")
def get_cube_data(cube_name: str):
    return read_cube_table(cube_name)
