import traceback

from flask import Flask, jsonify
import pymongo
import pandas as pd
import numpy as np

app = Flask(__name__)
# 连接数据库
client = pymongo.MongoClient(
    "mongodb://movie1:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
)

db = client.movielens

genres = [
    'action',
    'adventure',
    'animation',
    'children',
    'comedy',
    'crime',
    'documentary',
    'drama',
    'film-noir',
    'horror',
    'musical',
    'mystery',
    'romance',
    'sci-fi',
    'thriller',
    'war',
    'western',
    'no-genres-listed',
]


def success(data):
    catch_data = {
        "code": 0,
        "msg": "",
        "data": data
    }
    return jsonify(catch_data)


def error(msg):
    catch_data = {
        "code": -1,
        "msg": msg,
        "data": None
    }
    return jsonify(catch_data)


@app.route('/')
def hello_world():
    # 访问数据

    # 从数据表movie_info中读取数据
    test_data = db.movie_info.find_one()
    # bson.decode()
    # dic = {"name": "Bob", "properties": {"age": "18", "gender": "male"}}
    return success(test_data)


@app.route('/explore/genres/<string:genre>', defaults={'page': 1})
@app.route('/explore/genres/<string:genre>/<int:page>')
def explore_genres(genre, page):
    item_per_page = 20
    # 先lower
    genre = genre.lower()
    if genre not in genres:
        return error(f"genre {genre} not found")
    # print("request", genre, page)
    res = db[f"movie_genre_{genre}"].find({}, {"_id": 0}).sort([("count", -1), ("avg-rating", -1)]).skip(
        item_per_page * (page - 1)).limit(item_per_page)
    return success(list(res))


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
    print(repr(e))
    # print(str(e))
    # 异常追踪
    traceback.print_exc()
    return error(repr(e))


if __name__ == '__main__':
    app.run(port=5000, host="127.0.0.1", debug=True)
