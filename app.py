from flask import Flask, jsonify,request
import pymongo
import pandas as pd
import numpy as np
from tagRecommendUtils import recommend_by_groups,itemsPaging,get_movies_by_tag,get_groups_info_fromdb
import json
from flask_cors import * #导入跨域模块

app = Flask(__name__)
CORS(app, supports_credentials=True)  # 设置跨域

# 连接数据库
client = pymongo.MongoClient(
    "mongodb://movie3:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
)

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


@app.route('/')
def hello_world():
    # 访问数据
    db = client.movielens
    # 从数据表movie_info中读取数据
    test_data = db.movie_info.find_one()
    # bson.decode()
    # dic = {"name": "Bob", "properties": {"age": "18", "gender": "male"}}
    return success(test_data)

@app.route('/profile/settings/pick-groups',methods=['POST'])
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
    '''
    选择分组页面的六个类别各自的代表电影信息
    返回 [[movie1,movie2,movie3]....]
    :return:
    '''
    data = get_groups_info_fromdb()
    return success(data)


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
