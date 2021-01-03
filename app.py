from flask import Flask, jsonify

app = Flask(__name__)


def success(data):
    catch_data = {
        "code": 0,
        "msg": "",
        "data": data
    }
    return jsonify(catch_data)


@app.route('/')
def hello_world():
    dic = {"name": "Bob", "properties": {"age": "18", "gender": "male"}}
    return success(dic)


@app.errorhandler(Exception)
def error_handler(e):
    """
    全局异常捕获
    """
    catch_data = {
        "code": -1,
        "msg": repr(e),
        "data": None
    }
    # print(repr(e))
    # print(str(e))
    # 异常追踪
    # traceback.print_exc()
    return jsonify(catch_data)


if __name__ == '__main__':
    app.run(port=5000, host="127.0.0.1", debug=True)
