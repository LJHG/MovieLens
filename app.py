import json
import random
import traceback
import time

import pandas as pd
import pymongo
from flask import Flask, jsonify, request
from flask_cors import *  # 导入跨域模块

from svdRecommendUtils import SVD
from tagRecommendUtils import recommend_by_groups, itemsPaging, get_groups_info_fromdb

app = Flask(__name__)
CORS(app, supports_credentials=True)  # 设置跨域
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

ITEM_PER_PAGE = 24


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


@app.route('/profile/settings/pick-groups', methods=['POST'])
def add_tag_points():
    # data形式：{
    #     "group1":0,
    #     "group2":1,
    #     "group3":1,
    #     "group4":0,
    #     "group5":0,
    #     "group6":1
    # }
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    groups[1]['count'] = json_data['group1']
    groups[2]['count'] = json_data['group2']
    groups[3]['count'] = json_data['group3']
    groups[4]['count'] = json_data['group4']
    groups[5]['count'] = json_data['group5']
    groups[6]['count'] = json_data['group6']
    return success(groups)


@app.route('/profile/setting/get-groups-info')
def get_groups_info():
    """
    选择分组页面的六个类别各自的代表电影信息
    返回 [[movie1,movie2,movie3]....]
    :return:
    """
    data = get_groups_info_fromdb(db)
    return success(data)


@app.route('/explore/top-picks')
def top_picks():
    random_id = random.sample(range(0, 100), 8)  # 随机选8个
    topPick = []
    for i in random_id:
        obj = db.top_movie.find({}, {"_id": 0}).limit(1).skip(i)
        for l in list(obj):
            topPick.append(l)
    return success(topPick)


@app.route('/explore/top-picks/<int:page>')
def top_picks_page(page):
    PageMax = 24
    mvMax = 3931 - (page - 1) * PageMax
    obj = db.top_movie.find({}, {"_id": 0}).limit(min(mvMax, PageMax)).skip((page - 1) * PageMax)
    return success(list(obj))


@app.route('/explore/rate-more')
def rate_more():
    random_id = random.sample(range(0, 10500), 8)  # 随机选8个
    ratePick = []
    for i in random_id:
        obj = db.ratings_m100.find({}, {"_id": 0}).limit(1).skip(i)
        for l in list(obj):
            ratePick.append(l)
    return success(ratePick)


@app.route('/explore/rate-more/<int:page>')
def rate_more_page(page):
    PageMax = 24
    tagMax = 10500 - (page - 1) * PageMax
    obj = db.ratings_m100.find({}, {"_id": 0}).limit(min(tagMax, PageMax)).skip((page - 1) * PageMax)
    return success(list(obj))


@app.route('/movies/<int:movieId>/tags')
def movie_tags(movieId):
    obj = db.tag_movie_60tags.find_one({'_id': movieId})
    return success({'movie_id': obj['_id'], 'tag_list': obj['tag_list']})


@app.route('/movies/<int:movieId>/similar')
def movie_similar(movieId):
    random_id = random.sample(range(0, 64), 8)  # 随机选8个
    obj = db.similar_movie_svd.find_one({'movie_id': movieId})
    similar_id = obj['similar_id']
    similarPick = []
    for i in random_id:
        similarPick.append(similar_id[i])
    return success(similarPick)


@app.route('/movies/<int:movieId>/similar/<int:page>')
def get_similar_movie(movieId, page):
    PageMax = 24
    obj = db.similar_movie_svd.find_one({'movie_id': movieId})
    Lmax = len(obj['similar_id']) - 1
    return success(obj['similar_id'][(page - 1) * PageMax: min(page * PageMax, Lmax)])


@app.route('/explore/tag-picks/<int:page>')
def tag_picks_recommendation(page):
    all_movies = recommend_by_groups(groups, db)
    page_movies, total_page_num, total_num = itemsPaging(all_movies, ITEM_PER_PAGE, page)
    data = {'movies': page_movies, 'total_page_num': total_page_num, 'total_num': total_num}

    return success(data)


@app.route('/explore/genres/<string:genre>/<int:page>')
def explore_genres(genre, page):
    # 先lower
    genre = genre.lower()
    if not genres.__contains__(genre):
        return error(f"genre {genre} not found")
    res = db.movie_info.find({'genre': genres[genre]}).sort([
        ("aggregateRating.ratingCount", -1),
        ("aggregateRating.ratingValue", -1)]).skip(ITEM_PER_PAGE * (page - 1)).limit(ITEM_PER_PAGE)
    return success(list(res))


@app.route('/profile/rate/<int:movieId>')
def get_one_rating(movieId):
    obj = db.my_rating_movies.find_one({'_id': movieId})
    if obj is None:
        return error(f"no rating info for movie {movieId}")
    return success(obj)


@app.route('/profile/rate', methods=['POST'])
def rate_movie():
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
    print("modified", res.modified_count)
    print("upserted id", res.upserted_id)
    if res.upserted_id is None and res.modified_count == 0:
        # 如果没有变化就不需要重新进行计算
        print("no change")
        return success("ok")
    # 开启多线程计算用户推荐的电影
    ratings = db.my_rating.find()
    df = pd.DataFrame.from_dict(ratings)
    # 计算
    print("partial fit")
    pred, unrate_pred = svd.partial_fit(df)
    db.svd_predict.update_one({'_id': 0}, {
        "$set": {'predict': pred, 'timestamp': int(round(time.time() * 1e3))},
        "$setOnInsert": {'_id': 0}
    }, upsert=True)
    return success("ok")


@app.route('/profile/rates/<int:page>', )
def get_my_ratings(page):
    ratings = db.my_rating_movies.find().skip(ITEM_PER_PAGE * (page - 1)).limit(ITEM_PER_PAGE)
    return success(list(ratings))


@app.route('/explore/svd-picks/<int:page>')
def svd_picks(page: int):
    res = db.svd_result.find({}, {"_id": 0}).skip(ITEM_PER_PAGE * (page - 1)).limit(ITEM_PER_PAGE)
    return success(list(res))


if __name__ == '__main__':
    app.run(port=5000, host="127.0.0.1", debug=True)
