import json
import traceback

import pandas as pd
import pymongo
from flask import Flask, jsonify, request

from svdRecommendUtils import SVD
from tagRecommendUtils import recommend_by_groups, itemsPaging

app = Flask(__name__)
# 连接数据库
client = pymongo.MongoClient(
    "mongodb://movie3:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
)

db = client.movielens

genres = {
    'action': 'Action',
    'adventure': 'Adventure',
    'animation': 'Animation',
    'children': 'Children',
    'comedy': 'Comedy',
    'crime': 'Crime',
    'documentary': 'Documentary',
    'drama': 'Drama',
    'film-noir': 'Film-Noir',
    'horror': 'Horror',
    'musical': 'Musical',
    'mystery': 'Mystery',
    'romance': 'Romance',
    'sci-fi': 'Sci-Fi',
    'thriller': 'Thriller',
    'war': 'War',
    'western': 'Western',
    'no-genres-listed': '(no genres listed)',
}

groups = {
    1: {'tags': ['sci-fi', 'surreal', 'space'], 'count': 0},
    2: {'tags': ['action', 'superhero', 'visually appealing'], 'count': 0},
    3: {'tags': ['comedy', 'dark comedy', 'funny'], 'count': 1},
    4: {'tags': ['twist ending', 'mindfuck', 'nonlinear'], 'count': 1},
    5: {'tags': ['romance', 'animation', 'music'], 'count': 1},
    6: {'tags': ['classic', 'cinematography', 'masterpiece'], 'count': 0},
}

svd = SVD("C:\\Users\\mayn\\Desktop\\专业综合设计\\model")


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


@app.route('/profile/settings/pick-groups', methods=['POST'])
def add_tag_points():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    groups[1]['count'] = json_data['group1']
    groups[2]['count'] = json_data['group2']
    groups[3]['count'] = json_data['group3']
    groups[4]['count'] = json_data['group4']
    groups[5]['count'] = json_data['group5']
    groups[6]['count'] = json_data['group6']
    return success(groups)


@app.route('/explore/tags-picks/<int:curPage>/<int:pageItemsNum>')
def tag_picks_recommendation(curPage, pageItemsNum):
    all_movies = recommend_by_groups(groups)
    page_movies, total_page_num, total_num = itemsPaging(all_movies, pageItemsNum, curPage)
    data = {'movies': page_movies, 'total_page_num': total_page_num, 'total_num': total_num}
    return success(data)


@app.route('/profile/get-one-rating/<int:movieId>')
def get_one_rating(movieId):
    obj = db.my_rating.find_one({'movieId': movieId})
    return success({'movieId': obj['movieId'], 'rating': obj['rating']})


@app.route('/explore/genres/<string:genre>', defaults={'page': 1})
@app.route('/explore/genres/<string:genre>/<int:page>')
def explore_genres(genre, page):
    item_per_page = 20
    # 先lower
    genre = genre.lower()
    if not genres.__contains__(genre):
        return error(f"genre {genre} not found")
    res = db.movie_info.find({'genre': genres[genre]}).sort([
        ("aggregatingRating.ratingCount", -1),
        ("aggregatingRating.ratingValue", -1)]).skip(item_per_page * (page - 1)).limit(item_per_page)
    return success(list(res))


@app.route('/profile/rate', methods=['POST'])
def rate_movie():
    if request.method == "POST":
        data = request.form.to_dict()
        # print(data.decode("utf-8"))
        # json_data = json.loads(data.decode("utf-8"))
        movieId = int(data['movieId'])
        rating = float(data['rating'])
        print(f"movieId {movieId} rating {rating}")
        # 先去查询 本质就是一个insert or update
        res = db.my_rating.update_one({'_id': movieId}, {
            '$set': {'rating': rating},
            '$setOnInsert': {
                # 当用户对其评分不存在的时候就插入一个
                '_id': movieId
            }
        }, upsert=True)
        print(res.modified_count)
        if res.modified_count == 0:
            # 如果没有变化就不需要重新进行计算
            print("no change")
            return success("ok")
        # 开启多线程计算用户推荐的电影
        ratings = db.my_rating.find()
        df = pd.DataFrame.from_dict(ratings)
        # 计算
        pred, unrate_pred = svd.partial_fit(df)
        db.svd_predict.update_one({'_id': 0}, {
            "$set": {'predict': pred},
            "$setOnInsert": {'_id': 0}
        }, upsert=True)
        # 插入之后执行一下aggregate就行了
        db.svd_predict.aggregate([
            {
                '$project': {
                    '_id': 0
                }
            }, {
                '$unwind': {
                    'path': '$predict',
                    'includeArrayIndex': '_id',
                    'preserveNullAndEmptyArrays': False
                }
            }, {
                '$lookup': {
                    'from': 'movie_info',
                    'localField': 'predict.index',
                    'foreignField': '_id',
                    'as': 'movieInfo'
                }
            }, {
                '$unwind': {
                    'path': '$movieInfo',
                    'preserveNullAndEmptyArrays': False
                }
            }, {
                '$out': 'svd_result'
            }
        ])
        return success("ok")
        # 当用户评分完就要去更新
    return error(f"unsupported request method {request.method}")


@app.route('/profile/about-your-ratings')
def get_my_ratings():
    ratings = db.my_rating.find()
    rating_list = [{'movieId': rating['_id'], 'rating': rating['rating']} for rating in ratings]
    return success(rating_list)


@app.route('/explore/svd-picks', defaults={'page': 1})
@app.route('/explore/svd-picks/<int:page>')
def svd_picks(page: int):
    item_per_page = 20
    # 先lower
    res = db.svd_result.find({}, {"_id": 0}).skip(item_per_page * (page - 1)).limit(item_per_page)
    return success(list(res))
    # 需要读取数据


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
