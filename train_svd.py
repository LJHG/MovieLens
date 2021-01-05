# 基于surprise库训练模型

import numpy as np
import pandas as pd
from pathlib import Path
import pymongo
import surprise
from sklearn.model_selection import train_test_split

DATA_PATH = Path("C:\\Users\\mayn\\Desktop\\专业综合设计\\data\\ml-latest")
movies_data = DATA_PATH / "movies.csv"
genome_scores_data = DATA_PATH / "genome-scores.csv"
genome_tags_data = DATA_PATH / "genome-tsgs.csv"
ratings_data = DATA_PATH / "ratings.csv"
tags_data = DATA_PATH / "tags.csv"
links_data = DATA_PATH / "links.csv"

data = pd.read_csv(ratings_data,
                   usecols=["userId", "movieId", "rating"],
                   dtype={"userId": np.int32, "movieId": np.int32, "rating": np.float32})

print(data.info())

n_users = data['userId'].nunique()
n_movies = data['movieId'].nunique()

# 进行测试集训练集的筛选
train,test = train_test_split(data,test_size=0.001,)
