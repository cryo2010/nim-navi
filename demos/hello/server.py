"""Minimal FastAPI server exposing the /hello route the navi/js demo calls."""

from fastapi import FastAPI

app = FastAPI()


@app.get("/hello")
def hello() -> dict[str, str]:
    return {"message": "hello from FastAPI"}
