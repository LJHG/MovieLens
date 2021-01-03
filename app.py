from flask import Flask

app = Flask(__name__)


@app.route('/')
def hello_world():
    dic = {"name":"Bob","properties":{"age":"18","gender":"male"}}
    return dic


if __name__ == '__main__':
    app.run()
