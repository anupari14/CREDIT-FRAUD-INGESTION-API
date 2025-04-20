import os

class Config:
    DEBUG = os.getenv("DEBUG", False)
    SECRET_KEY = os.getenv("SECRET_KEY", "supersecret")

def get_config():
    return Config()