import pymongo


def tempCmp(movieItem1, movieItem2):
    # 'movieId': 260, 'tag_cnt': 1050, 'rating_cnt': 81815, 'avg_rating': 4.120454788208008}
    if movieItem1['tag_cnt'] == movieItem2['tag_cnt']:
        return movieItem1['rating_cnt'] > movieItem2['rating_cnt']
    else:
        return movieItem1['tag_cnt'] > movieItem2['tag_cnt']


def get_movies_by_tag(tag):
    client = pymongo.MongoClient(
        "mongodb://movie3:123@49.235.186.44:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"
    )
    db = client.movielens
    items = db.tag_movies.find({'tag_name': tag})[0]['info']
    items.sort(key=lambda x: x['tag_cnt'] * 0.999 * -1 + x['rating_cnt'] * 0.001 * -1)
    return items


def itemsPaging(items_all, numPerPage, pageIndex):
    # pageIndex从1开始
    pagesNum = len(items_all) // numPerPage
    if len(items_all) % numPerPage != 0:
        pagesNum += 1
    ans_list = []
    # 1   0~23  24*(pageIndex-1) ~ 24*pagesIndex-1
    startIndex = numPerPage * (pageIndex - 1)
    end = numPerPage * pageIndex - 1
    for i in range(startIndex, end + 1):
        ans_list.append(items_all[i])
    return ans_list, pagesNum, len(items_all)


def recommend_by_groups(groups):
    """
    根据加点情况来推荐电影
    :param groups:
    :return:
    """
    tag_count_dic = {}
    for i in range(1, 7):
        if groups[i]['count'] > 0:
            for item in groups[i]['tags']:
                tag_count_dic[item] = groups[i]['count'] / 9
    recommend_list = []
    for tag in tag_count_dic:
        all_movies = get_movies_by_tag(tag)
        endIndex = int(tag_count_dic[tag] * len(all_movies))
        recommend_list.extend(all_movies[0:endIndex])

    return all_movies


if __name__ == '__main__':
    groups = {
        1: {'tags': ['sci-fi', 'surreal', 'space'], 'count': 0},
        2: {'tags': ['action', 'superhero', 'visually appealing'], 'count': 0},
        3: {'tags': ['comedy', 'dark comedy', 'funny'], 'count': 1},
        4: {'tags': ['twist ending', 'mindfuck', 'nonlinear'], 'count': 1},
        5: {'tags': ['romance', 'animation', 'music'], 'count': 1},
        6: {'tags': ['classic', 'cinematography', 'masterpiece'], 'count': 0},
    }
    recommend_by_groups(groups)
