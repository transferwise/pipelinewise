class Settings(object):
    DEBUG = False
    TESTING = False
    HOST = "127.0.0.1"
    PORT = 5000

class DevelopmentSettings(Settings):
    DEBUG = True

class TestingSettings(Settings):
    TESTING = True

class ProductionSettings(Settings):
    PORT = 8080

