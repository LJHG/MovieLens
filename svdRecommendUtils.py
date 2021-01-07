# 基于surprise库训练模型

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pymongo
import bson


class SVD(object):
    def __init__(self, model_path: str):
        self.model_path = Path(model_path) / "model.npz"
        if not self.model_path.exists():
            raise ValueError(f"file {self.model_path.name} not exists")

        # 将其转换为npz
        npz_file = np.load(self.model_path)
        # print("load model")
        # shape: (n_movies,)
        self.movie_bias = npz_file["movie_bias"]
        # shape: (n_movies,n_factors)
        self.movie_factor = npz_file["movie_factor"]
        # 标量吧
        self.global_mean = np.float(npz_file["global_mean"])
        # 标签映射文件
        self.map_index = npz_file["map_index"]

        self.n_movies = self.movie_factor.shape[0]
        self.n_factors = self.movie_factor.shape[1]

        # 一来就给他初始化
        self.user_bias, self.user_factor = self.gen_user_params()

        self.rawids = pd.read_csv(Path(model_path) / "rawids.csv", index_col='movieId',
                                  dtype={"movieId": int, "innerId": int})

        # np.savez(self.model_path , movie_bias=self.movie_bias, movie_factor=self.movie_factor,
        #          global_mean=self.global_mean, map_index=self.map_index)

    # 将每一个movie_facotr直接存储到movieInfo中，就可以计算出评分
    def save_movie_factor(self):
        client = pymongo.MongoClient(
            "mongodb://movie3:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
        )
        # svd = SVD("C:\\Users\\mayn\\Desktop\\专业综合设计\\model")
        db = client.movielens
        for i in range(self.n_movies):
            db.movie_info.update_one({'innerId': i}, {'$set': {'bias': float(self.movie_bias[i])}})
        db.movie_info.update_many({'innerId': -1}, {'$set': {'bias': 0.0}})

    def predict(self, innerId):
        res = self.global_mean + self.user_bias + self.movie_bias[innerId] + np.dot(self.user_factor, self.movie_factor[innerId])
        return res[0]

    def gen_user_params(self, mu=0, sigma=0.1):
        return 0.0, np.random.normal(loc=mu, scale=(sigma / self.n_factors), size=self.n_factors)

    # 支持在线更新
    def partial_fit(self,
                    train_df,
                    iterations=20,
                    learning_rate=0.07,
                    user_bias_reg=0.001,
                    user_factor_reg=0.001):
        """
        train set format: [(index,rating),(index,rating),(index,rating)]
        """
        # Compute gradient descent
        for iteration in range(iterations):
            for row in train_df.itertuples():
                i = row[2]
                rating = row[3]
                # 计算细节：global_mean 标量， user_bias 标量 movie_bias[i] 标量 user_factor : (200,) movie_factor[i] : (200,)
                predict = self.global_mean + self.user_bias + self.movie_bias[i] + np.dot(self.user_factor, self.movie_factor[i])
                error = rating - predict
                # update user factor
                # learning_rate 标量 error 标量 movie_factor[i] ：(200,) user_factor_reg：标量 user_factor : (200,)
                self.user_factor += learning_rate * (error * self.movie_factor[i] - user_factor_reg * self.user_factor)
                # update user bias
                # learning_rate 标量 error 标量 user_bias_reg ：(200,) user_bias：标量
                self.user_bias += learning_rate * (error - user_bias_reg * self.user_bias)
        # 输出的时候注意将用户user_factor转换为（200，1），user_bias转换为数字
        uf = self.user_factor.reshape(-1, 1)
        pred = self.global_mean + self.movie_bias + self.user_bias + np.dot(self.movie_factor, uf)
        pred = pred.ravel()
        # for循环还是太慢了，这样不行，还是直接拼接吧
        # 这不是就直接拿self.map_index 和 prediction拼起来就可以
        # 就只返回前1000个
        res = pd.DataFrame({"prediction": pred, "index": self.map_index}, index=self.map_index).drop(
            index=train_df['_id']).sort_values(
            by="prediction", ascending=False)[:1000]
        # 将超出的压制下来
        res.loc[res['prediction'] > 5.0, 'prediction'] = 5.0
        # surpress
        res.loc[res['prediction'] < 0.0, 'prediction'] = 0.0
        return res.to_dict(orient='records')


if __name__ == '__main__':
    svd = SVD("C:\\Users\\mayn\\Desktop\\专业综合设计\\model")
    svd.save_movie_factor()
    # import pymongo
    # import time
    #
    # client = pymongo.MongoClient(
    #     "mongodb://movie3:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
    # )
    # svd = SVD("C:\\Users\\mayn\\Desktop\\专业综合设计\\model")
    # db = client.movielens
    # start_time = time.perf_counter()
    # ratings = db.my_rating.find()
    # df = pd.DataFrame.from_dict(ratings)
    # end_time = time.perf_counter()
    # print("query db :", end_time - start_time)
    # df.info()
    # # 计算
    # print(df)
    # start_time = time.perf_counter()
    # pred, unrate_pred = svd.partial_fit(df)
    # end_time = time.perf_counter()
    # print("svd recommend: ", end_time - start_time)
    # print(pred)
    # start_time = time.perf_counter()
    # db.svd_predict.update_one(
    #     {'_id': 0},
    #     {
    #         "$set": {'predict': pred},
    #         "$setOnInsert": {'_id': 0}
    #     }, upsert=True)
    # # 插入之后执行一下aggregate就行了
    # db.svd_predict.aggregate([
    #     {
    #         '$project': {
    #             '_id': 0
    #         }
    #     }, {
    #         '$unwind': {
    #             'path': '$predict',
    #             'includeArrayIndex': '_id',
    #             'preserveNullAndEmptyArrays': False
    #         }
    #     }, {
    #         '$lookup': {
    #             'from': 'movie_info',
    #             'localField': 'predict.index',
    #             'foreignField': '_id',
    #             'as': 'movieInfo'
    #         }
    #     }, {
    #         '$unwind': {
    #             'path': '$movieInfo',
    #             'preserveNullAndEmptyArrays': False
    #         }
    #     }, {
    #         '$out': 'svd_result'
    #     }
    # ])
    # end_time = time.perf_counter()
    # print("save to db: ", end_time - start_time)
