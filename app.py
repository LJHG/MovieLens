import json
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

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

svd = SVD("model")

mode = 1

executor = ThreadPoolExecutor(max_workers=2)

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
    # data形式：
    # {
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


@app.route('/profile/settings/get-groups-info')
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
    res = None

    if(mode ==1 ):
        random_size = 12
        obj = db.top_movie.aggregate([{
            '$limit': 100
        }, {
            '$sample': {
                'size': random_size
            }
        }])
        res = list(obj)
    elif(mode == 2):
        tmp_res = json.loads(tag_picks_recommendation(1).data)['data'][0:12]
        obj = []
        for item in tmp_res:
            obj.append({'_id':item['movieId']})
        res = obj
    else:
        res = json.loads(svd_picks(1).data)['data'][0:12]
    return success(res)


@app.route('/explore/top-picks/<int:page>')
def top_picks_page(page):
    res = None
    if(mode == 1):
        obj = db.top_movie.find().skip(ITEM_PER_PAGE * (page - 1)).limit(ITEM_PER_PAGE)
        res = list(obj)
    elif(mode == 2):
        tmp_res = json.loads(tag_picks_recommendation(page).data)['data']
        obj = []
        for item in tmp_res:
            obj.append({'_id': item['movieId']})
        res = obj
    else:
        res = json.loads(svd_picks(page).data)['data']
    return success(res)

@app.route('/profile/settings/change-mode/<int:m>')
def change_mode(m):
    global mode
    mode = m
    print('change mode...',mode)
    return success("ok")

@app.route('/profile/settings/get-mode')
def get_mode():
    print('get mode ...',mode)
    return success(mode)


@app.route('/explore/svd-picks/<int:page>')
def svd_picks(page: int):
    res = db.svd_predict.aggregate([
        {
            '$project': {
                '_id': 0,
                'predict': 1
            }
        }, {
            '$unwind': {
                'path': '$predict'
            }
        }, {
            '$project': {
                '_id': '$predict.index',
                'predict': '$predict.prediction'
            }
        }, {
            '$skip': ITEM_PER_PAGE * (page - 1)
        }, {
            '$limit': ITEM_PER_PAGE
        }
    ])
    return success(list(res))


@app.route('/explore/rate-more')
def rate_more():
    # 随机选取
    random_size = 12
    obj = db.ratings_m100.aggregate([{
        '$limit': 100
    }, {
        '$sample': {
            'size': random_size
        }
    }])
    return success(list(obj))


@app.route('/explore/rate-more/<int:page>')
def rate_more_page(page):
    obj = db.ratings_m100.find().skip(ITEM_PER_PAGE * (page - 1)).limit(ITEM_PER_PAGE)
    return success(list(obj))


@app.route('/movies/<int:movieId>')
def get_movie_info(movieId):
    # 使用相似度计算
    movie_info = db.movie_info.find_one({'_id': movieId}, {'bias': 0, 'factor': 0})
    movie_info['predict'] = svd.predict(movie_info['innerId'])
    # 计算用户和电影的预测评分
    return success(movie_info)


@app.route('/movies/<int:movieId>/tags')
def movie_tags(movieId):
    obj = db.tag_movie_60tags.find_one({'_id': movieId})
    return success(obj)


@app.route('/movies/<int:movieId>/similar')
def movie_similar(movieId):
    random_size = 8
    result = db.similar_movie_svd.aggregate([
        {
            '$match': {
                '_id': movieId
            }
        }, {
            '$project': {
                '_id': 0,
                'movieId': '$similar_id'
            }
        }, {
            '$unwind': {
                'path': '$movieId'
            }
        }, {
            '$limit': 20
        }, {
            '$sample': {
                'size': random_size
            }
        }
    ])
    return success(list(result))


@app.route('/movies/<int:movieId>/similar/<int:page>')
def get_similar_movie(movieId, page):
    result = db.similar_movie_svd.aggregate([
        {
            '$match': {
                '_id': movieId
            }
        }, {
            '$project': {
                '_id': 0,
                'movieId': '$similar_id'
            }
        }, {
            '$unwind': {
                'path': '$movieId'
            }
        }, {
            '$skip': ITEM_PER_PAGE * (page - 1)
        }, {
            '$limit': ITEM_PER_PAGE
        }])
    return success(list(result))


@app.route('/explore/tag-picks/<int:page>')
def tag_picks_recommendation(page):
    all_movies = recommend_by_groups(groups, db)
    page_movies, total_page_num, total_num = itemsPaging(all_movies, ITEM_PER_PAGE, page)
    # data = {'movies': page_movies, 'total_page_num': total_page_num, 'total_num': total_num}
    return success(page_movies)


@app.route('/explore/genres/<string:genre>/<int:page>')
def explore_genres(genre, page):
    # 先lower
    genre = genre.lower()
    if not genres.__contains__(genre):
        return error(f"genre {genre} not found")
    res = db.movie_info.find({'genre': genres[genre]}, {'_id': 1}).sort([
        ("aggregateRating.ratingCount", -1),
        ("aggregateRating.ratingValue", -1)]).skip(ITEM_PER_PAGE * (page - 1)).limit(ITEM_PER_PAGE)
    return success(list(res))


@app.route('/profile/rate/<int:movieId>')
def get_one_rating(movieId):
    obj = db.my_rating.find_one({'_id': movieId})
    if obj is None:
        return error(f"no rating info for movie {movieId}")
    return success(obj)


@app.route('/profile/rate', methods=['POST'])
def rate_movie():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    movieId = int(json_data['movieId'])
    rating = float(json_data['rating'])
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
    executor.submit(update_svd)
    return success("ok")


def update_svd():
    print("background update svd running")
    # 开启多线程计算用户推荐的电影
    ratings_cursor = db.my_rating.aggregate([
        {
            '$lookup': {
                'from': 'movie_info',
                'localField': '_id',
                'foreignField': '_id',
                'as': 'movie_info'
            }
        }, {
            '$unwind': {
                'path': '$movie_info'
            }
        }, {
            '$project': {
                'innerId': '$movie_info.innerId',
                'rating': '$rating',
            }
        }
    ])
    rating_df = pd.DataFrame(ratings_cursor)
    print("partial fit")
    print(rating_df.info())
    pred = svd.partial_fit(rating_df)
    db.svd_predict.update_one({'_id': 0}, {
        "$set": {'predict': pred, 'timestamp': int(round(time.time() * 1e3))},
        "$setOnInsert": {'_id': 0}
    }, upsert=True)


# 那此处就不明确写出共有多少页，直接不断向下添加就行了
@app.route('/profile/rates/<int:page>', )
def get_my_ratings(page):
    ratings = db.my_rating.find().skip(ITEM_PER_PAGE * (page - 1)).limit(ITEM_PER_PAGE)
    return success(list(ratings))


if __name__ == '__main__':
    app.run(port=5000, host="127.0.0.1", debug=True)
