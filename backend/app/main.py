from fastapi import FastAPI

app = FastAPI(title="DataFabrik API", version="0.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "datafabrik-api", "version": "0.1.0"}
