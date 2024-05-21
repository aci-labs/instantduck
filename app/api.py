from fastapi import FastAPI, HTTPException, Request, Query
import redis
import json
import duckdb

app = FastAPI()
redis_conn = redis.Redis(host="localhost", port=6379)
# Adding support for query through api 
# duckdb_conn = duckdb.connect(database='/data/duckdb.db')
# duckdb_conn.execute("INSTALL json;")
# duckdb_conn.execute("LOAD json;")

@app.post("/ingest")
async def ingest_data(request: Request, tags: str = Query(None, description="Comma-separated list of dbt tags")):
    data = await request.json()
    if tags:
        data['tags'] = tags
    # Add data to Redis Stream
    redis_conn.xadd('ingeststream', {'data': json.dumps(data)})
    return {"message": "Data ingested successfully"}


# @app.get("/query")
# async def query_data(query: str = "SELECT * FROM db1.ingest.messages"):
#     result = duckdb_conn.execute(query).fetchall()
#     return result

# @app.post("/transform")
# async def transform_data(query: str):
#     try:
#         duckdb_conn.execute(query)
#         duckdb_conn.commit()
#         return {"message": "Transformation applied successfully"}
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
