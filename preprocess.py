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
    def __init__(self, _id: int, imdbId: str, tmdbId: str, innerId: str, genres: str, ratingCount, ratingValue,
                 bestRating, worstRating, infoDict: dict):
        self._id = _id
        self.innerId = int(innerId)
        self.imdbId = imdbId
        self.tmdbId = tmdbId
        self.name = infoDict['name']
        if infoDict.__contains__('image'):
            self.image = infoDict['image']
        else:
            self.image = None

        self.genre = genres.split('|')
        # 处理list
        if infoDict.__contains__('contentRating'):
            self.contentRating = infoDict['contentRating']
        else:
            self.contentRating = 'Not Rated'
        # 处理list
        if infoDict.__contains__('actor'):
            self.actor = infoDict['actor']
        else:
            self.actor = None
        # 处理list

        if infoDict.__contains__('director'):
            self.director = infoDict['director']
        else:
            self.director = None

        if infoDict.__contains__('creator'):
            self.creator = infoDict['creator']
        else:
            self.creator = None

        if infoDict.__contains__('description'):
            self.description = infoDict['description']
        else:
            self.description = None

        if infoDict.__contains__('datePublished'):
            self.datePublished = infoDict['datePublished']
        else:
            self.datePublished = None

        # 有可能是timeRequired
        if infoDict.__contains__('timeRequired'):
            self.duration = infoDict['timeRequired']
        elif infoDict.__contains__('duration'):
            self.duration = infoDict['duration']
        else:
            self.duration = None

        if infoDict.__contains__('keywords'):
            self.keywords = infoDict['keywords'].split(',')
        else:
            self.keywords = None

        self.aggregateRating = {
            "@type": "AggregateRating",
            "ratingCount": int(ratingCount),
            "bestRating": bestRating,
            "worstRating": worstRating,
            "ratingValue": ratingValue
        }

        if infoDict.__contains__('trailer'):
            self.trailer = infoDict['trailer']
        else:
            self.trailer = None

        if infoDict.__contains__('review'):
            self.review = infoDict['review']
        else:
            self.review = None

    def to_dict(self):
        return self.__dict__

    def __repr__(self):
        return str(self.__dict__)


def movie_info_preprocess():
    client = pymongo.MongoClient(
        'mongodb://movie1:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')
    db = client['movielens']
    # 测试序列化方法
    DATA_PATH = Path("C:\\Users\\mayn\\Desktop\\专业综合设计\\data\\ml-latest")
    movie_data = DATA_PATH / "links.csv"
    info_data = DATA_PATH / "movies.csv"
    ratings_data = DATA_PATH / "ratings.csv"
    MODEL_PATH = Path("C:\\Users\\mayn\\Desktop\\专业综合设计\\model")
    rawid_data = MODEL_PATH / "rawids.csv"

    movie_data_df = pd.read_csv(movie_data,
                                usecols=["movieId", "imdbId", "tmdbId"],
                                dtype={"movieId": int, "imdbId": str, "tmdbId": str},
                                index_col="movieId")

    rawid_data_df = pd.read_csv(rawid_data,
                                usecols=['movieId', 'innerId'],
                                dtype={"movieId": int, "innerId": str},
                                index_col="movieId")

    movie_info_df = pd.read_csv(info_data,
                                usecols=['movieId', 'genres'],
                                dtype={"movieId": int, "genres": str},
                                index_col="movieId")

    ratings_data_df = pd.read_csv(ratings_data,
                                  usecols=["userId", "movieId", "rating"],
                                  dtype={"userId": np.int32, "movieId": np.int32, "rating": np.float32})
    movie_ratings_group = ratings_data_df.groupby(by="movieId")
    movie_avg_ratings = movie_ratings_group.agg(
        ratingCount=pd.NamedAgg(column="rating", aggfunc='count'),
        ratingValue=pd.NamedAgg(column="rating", aggfunc='mean'),
        bestRating=pd.NamedAgg(column="rating", aggfunc='max'),
        worstRating=pd.NamedAgg(column="rating", aggfunc='min')
    )

    rawid_data_df.info()
    movie_info_df.info()
    movie_data_df = movie_data_df.join(rawid_data_df)
    movie_data_df = movie_data_df.join(movie_info_df)
    movie_data_df = movie_data_df.join(movie_avg_ratings)

    movie_data_df.loc[movie_data_df['innerId'].isna(), 'innerId'] = "-1"
    movie_data_df.loc[movie_data_df['tmdbId'].isna(), 'tmdbId'] = "-1"

    na_ratings = movie_data_df['ratingCount'].isna()

    movie_data_df.loc[na_ratings, 'ratingCount'] = -1.0
    movie_data_df.loc[na_ratings, 'ratingValue'] = -1.0
    movie_data_df.loc[na_ratings, 'bestRating'] = -1.0
    movie_data_df.loc[na_ratings, 'worstRating'] = -1.0

    movie_data_df.info()
    JSON_PATH = Path("C:\\Users\\mayn\\Desktop\\专业综合设计\\imdb\\imdb\\spiders\\json")
    cnt = 1
    batch = []
    for movieId, imdbId, tmdbId, innerId, genres, ratingCount, ratingValue, bestRating, worstRating in movie_data_df.itertuples():
        filename = JSON_PATH / f"{imdbId}.json"
        if filename.exists():
            # print("read file", filename.name)
            jsonfile = json.load(filename.open(encoding='utf8', mode='r'))
            # print()
            info = movieInfoSchema(movieId, imdbId, tmdbId, innerId, genres, ratingCount, ratingValue, bestRating,
                                   worstRating,
                                   jsonfile)
            batch.append(info.to_dict())
            if cnt % 1000 == 0:
                db['movie_info'].insert_many(batch)
                batch.clear()
                print(f'check point {cnt}')
            cnt += 1
        else:
            print("file not exist", filename.name)
    # 最后还要插入一遍
    db['movie_info'].insert_many(batch)
    batch.clear()
    print(f'check point {cnt}')


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
