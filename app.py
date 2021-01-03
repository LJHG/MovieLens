from flask import Flask, jsonify
import pymongo
import pandas as pd
import numpy as np

app = Flask(__name__)
# 连接数据库
client = pymongo.MongoClient(
    "mongodb://movie1:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
)


def success(data):
    catch_data = {
        "code": 0,
        "msg": "",
        "data": data
    }
    return jsonify(catch_data)


@app.route('/')
def hello_world():
    # 访问数据
    db = client.movielens
    # 从数据表movie_info中读取数据
    test_data = db.movie_info.find_one()
    # bson.decode()
    # dic = {"name": "Bob", "properties": {"age": "18", "gender": "male"}}
    return success(test_data)


"""
# serialize 1D array x
record['feature1'] = x.tolist()

# deserialize 1D array x
x = np.fromiter( record['feature1'] )
对于多维数组，你需要使用pickle和pymongo.binary.Binary：

# serialize 2D array y
record['feature2'] = pymongo.binary.Binary( pickle.dumps( y, protocol=2) ) )

# deserialize 2D array y
y = pickle.loads( record['feature2'] )
"""


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
