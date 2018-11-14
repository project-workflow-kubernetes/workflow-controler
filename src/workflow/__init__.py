from flask import Flask

from workflow.endpoints import mod

app = Flask(__name__)
app.register_blueprint(mod)


if __name__ == '__main__':
    app.run(debug=True)
