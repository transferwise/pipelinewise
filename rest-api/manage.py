from flask import Flask
from flask_script import Manager

app = Flask(__name__)
# configure your app

manager = Manager(app)

@manager.command
def hello():
    print("hello command")

if __name__ == "__main__":
    manager.run()