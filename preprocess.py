import numpy as np
import pandas as pd
from pathlib import Path
import pymongo
import json


def get_movie_genres():
    DATA_PATH = Path("C:\\Users\\mayn\\Desktop\\专业综合设计\\data\\ml-latest")
    movie_data = DATA_PATH / "movies.csv"

    movie_data_df = pd.read_csv(movie_data,
                                usecols=["movieId", "genres"],
                                dtype={"movieId": np.int, "genres": str})

    # 遍历movie
    # print(movie_data_df.info())
    genres = {
        'action': [],
        'adventure': [],
        'animation': [],
        'children': [],
        'comedy': [],
        'crime': [],
        'documentary': [],
        'drama': [],
        'film-noir': [],
        'horror': [],
        'musical': [],
        'mystery': [],
        'romance': [],
        'sci-fi': [],
        'thriller': [],
        'war': [],
        'western': [],
        'fantasy': [],
        'imax': [],
        'no-genres-listed': [],
    }

    def mapToGenre(x: pd.Series):
        """
        :param x:pd.Series
        :return:(int,list)
        """
        # print(f"{type(x)} {x}")
        movieId = x.get("movieId")
        val = x.get("genres")
        if val == "(no genres listed)":
            genres['no-genres-listed'].append(movieId)
            return
        genre_list = [g.lower() for g in val.split("|")]
        # print(genre_list)
        list(map(lambda g: genres[g].append(movieId), genre_list))

    filter_res = movie_data_df.apply(mapToGenre, axis='columns', result_type="reduce")

    return [{"genre": key, "movies": value} for key, value in genres.items()]


class movieInfoSchema(object):
    def __init__(self, _id: int, imdbId: str, tmdbId: str, infoDict: dict):
        self._id = _id
        self.imdbId = imdbId
        self.tmdbId = tmdbId
        self.name = infoDict['name']
        self.image = infoDict['image']
        # 处理list
        self.genre = infoDict['genre']
        self.contentRating = infoDict['contentRating']
        # 处理list
        self.actor = infoDict['actor']
        # 处理list
        self.director = infoDict['director']
        self.description = infoDict['description']
        self.datePublished = infoDict['datePublished']
        # 有可能是timeRequired
        if infoDict.get('duration', None) is None:
            self.duration = infoDict['timeRequired']
        else:
            self.duration = infoDict['duration']
        # self.duration = infoDict['duration']
        self.keywords = infoDict['keywords'].split(',')
        # self.aggregateRating = {
        #     "ratingCount": 0,
        #     "bestRating": "0.0",
        #     "worstRating": "0.0",
        #     "ratingValue": "0.0"
        # }
        self.keywords = infoDict['aggregateRating']
        # self.trailer = {
        #     "embedUrl": "",
        #     "thumbnail": {
        #         "contentUrl": ""
        #     },
        #     "thumbnailUrl": "",
        # }

    def to_dict(self):
        return self.__dict__

    def __repr__(self):
        return str(self.__dict__)


def movie_info_preprocess():
    # 测试序列化方法
    DATA_PATH = Path("C:\\Users\\mayn\\Desktop\\专业综合设计\\data\\ml-latest")
    movie_data = DATA_PATH / "links.csv"

    movie_data_df = pd.read_csv(movie_data,
                                usecols=["movieId", "imdbId", "tmdbId"],
                                dtype={"movieId": int, "imdbId": str, "tmdbId": str},
                                index_col="movieId")

    print(movie_data_df.info())
    JSON_PATH = Path("C:\\Users\\mayn\\Desktop\\专业综合设计\\imdb\\imdb\\spiders\\json")
    for movieId, imdbId, tmdbId in movie_data_df.itertuples():
        filename = JSON_PATH / f"{imdbId}.json"
        if filename.exists():
            print("read file", filename.name)
            jsonfile = json.load(filename.open(encoding='utf8', mode='r'))
            # print()
            movieInfoSchema(movieId, imdbId, tmdbId, jsonfile)
        else:
            print("file not exist", filename.name)


if __name__ == '__main__':
    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    # client = pymongo.MongoClient(
    #     'mongodb://movie1:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')

    movie_info_preprocess()

    # result = client['movielens']['movie_genres'].aggregate([
    #     {
    #         '$lookup': {
    #             'from': 'movie_ratings',  # 进行表连接
    #             'localField': 'movies',  # 以本地的属性值movies作为参考
    #             'foreignField': 'movieId',  # 对应movie_ratings表的movieId
    #             'as': 'movie-rate'  # 输出的结果在movie-rate中
    #         }
    #     },
    #     # {
    #     #     '$sort': {                      # 再进行排序
    #     #         'movie-rate.count': -1,     # 按照个数降序排列
    #     #         'movie-rate.avg-rating': -1 # 按照评分降序排列
    #     #     }
    #     # }
    # ])
    #
    # result = list(result)
    # print(movieInfoSchema(23).to_dict())
