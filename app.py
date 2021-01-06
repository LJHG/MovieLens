
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
    random_id = random.sample(range(0, 100), 8)  # 随机选8个
    topPick = []
    for i in random_id:
        obj = db.top_movie.find({}, {"_id": 0}).limit(1).skip(i)
        for l in list(obj):
            topPick.append(l)
    return success(topPick)


@app.route('/explore/top-picks/<PageNum>')
def top_picks_Page(PageNum):
    PageMax = 24
    mvMax = 3931 - (int(PageNum) - 1) * PageMax
    obj = db.top_movie.find({}, {"_id": 0}).limit( min(mvMax,PageMax) ).skip( (int(PageNum) - 1) * PageMax )
    return success(list(obj))

@app.route('/explore/rate-more')
def rate_more():
    random_id = random.sample(range(0, 10500), 8)  # 随机选8个
    ratePick = []
    for i in random_id:
        obj = db.ratings_m100.find({}, {"_id":0}).limit( 1 ).skip( i )
        for l in list(obj):
            ratePick.append( l )
    return success(ratePick)


@app.route('/explore/rate-more/<PageNum>')
def rate_more_Page(PageNum):
    PageMax = 24
    tagMax = 10500 - (int(PageNum) - 1) * PageMax
    obj = db.ratings_m100.find({}, {"_id": 0}).limit( min(tagMax,PageMax) ).skip( (int(PageNum) - 1) * PageMax )
    return success(list(obj))


@app.route('/movies/<movieId>/tags')
def movie_tags(movieId):
    obj = db.tag_movie_60tags.find_one({'_id':int(movieId)})
    return success({'movie_id': obj['_id'], 'tag_list': obj['tag_list']})

@app.route('/movies/<movieId>/similar')
def movie_similar(movieId):
    random_id = random.sample(range(0, 64), 8)  # 随机选8个
    obj = db.similar_movie_svd.find_one({'movie_id': int(movieId)})
    similar_id = obj['similar_id']
    similarPick = []
    for i in random_id:
        similarPick.append(similar_id[i])
    return success(similarPick)

@app.route('/movies/<movieId>/similar/<PageNum>')
def movie_similar_Page(movieId, PageNum):
    PageMax = 24
    obj = db.similar_movie_svd.find_one( {'movie_id': int(movieId)} )
    Lmax = len(obj['similar_id']) - 1
    return success(obj['similar_id'][(int(PageNum) - 1) * PageMax : min(int(PageNum) * PageMax, Lmax) ] )



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
