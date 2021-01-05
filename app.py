
import traceback
from flask import Flask, jsonify,request
import pymongo
import pandas as pd
import numpy as np
from tagRecommendUtils import recommend_by_groups,itemsPaging
import json
import random

app = Flask(__name__)
# 连接数据库
client = pymongo.MongoClient(
    "mongodb://movie3:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
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

mv_top100_idList = [159817,    318, 174053,    858,     50,   1221,    527,   2019,
         1203,    904,   2959,   1193,    912,    750,   5618,   1212,
         1178,    908,  44555,   3435,    922,   6016,   3030,   1213,
          296,  58559,    926,    930,   2324,  79132,   1260,   1284,
         4226,   1207,   1252,    593,   1248,    950,   2571,   5971,
         1136,   1217,   1234,   2329, 160718,    905,   1196,    913,
         3134,   1147, 112552,  92259,   1148,   1945,   2203,   5291,
         1197, 163134,  86504,   3000,   1172,   1254,   2858,   2186,
         1198,    260,   1201,   2920,  48516,    903,   4973,    541,
         1280,   3089,   3307,   1204,   6669,    898,    745,   2731,
         1208,   1949,   2351,    608,   1233,   1209,   2905,  98491,
         1131,   7153,   3022,   1262,   7327,   3429,   4993, 116897,
         2859,   3462,   5690,    951]


groups = {
    1:{'tags':['sci-fi','surreal','space'],'count':0},
    2:{'tags':['action','superhero','visually appealing'],'count':0},
    3:{'tags':['comedy','dark comedy','funny'],'count':1},
    4:{'tags':['twist ending','mindfuck','nonlinear'],'count':1},
    5:{'tags':['romance','animation','music'],'count':1},
    6:{'tags':['classic','cinematography','masterpiece'],'count':0},
}



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

@app.route('/profile/settings/pick-groups',methods=['POST'])
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

@app.route('/explore/top-picks')
def top_picks():
    random_id = random.sample(range(0, 100), 8) #随机选8个
    topPick = []
    cnt = 1
    for i in random_id:
        topPick.append( {f'movie{cnt}:' : mv_top100_idList[i]} )
        cnt += 1
    return success(topPick)

@app.route('/explore/tags-picks/<curPage>/<pageItemsNum>')
def tag_picks_recommendation(curPage,pageItemsNum):
    all_movies = recommend_by_groups(groups)
    page_movies,total_page_num,total_num = itemsPaging(all_movies,int(pageItemsNum),int(curPage))
    data = {'movies':page_movies,'total_page_num':total_page_num,'total_num':total_num}
    return success(data)

@app.route('/profile/get-one-rating/<movieId>')
def get_one_rating(movieId):
    db = client.movielens
    obj = db.my_rating.find_one({'movieId':int(movieId)})
    return success({'movieId':obj['movieId'],'rating':obj['rating']})



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



@app.route('/profile/rate',methods=['POST'])
def rate_movie():
    data = request.get_data()
    json_data = json.loads(data.decode("utf-8"))
    movieId = json_data['movieId']
    rating = json_data['rating']
    db = client.movielens

    # 先去查询
    obj = db.my_rating.find_one({'movieId': int(movieId)})
    if(obj != None):
        # 如果已经存在
        db.my_rating.update_one({'movieId':movieId},{'$set':{'rating':rating}})
    else:
        db.my_rating.insert_one({'movieId':movieId,'rating':rating})
    return success("ok")


@app.route('/profile/about-your-ratings')
def get_my_ratings():
    db = client.movielens
    ratings = db.my_rating.find()
    rating_list = []
    for rating in ratings:
        rating_list.append({'movieId':rating['movieId'],'rating':rating['rating']})
    return success(rating_list)

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
