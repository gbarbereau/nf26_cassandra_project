from database import *
from main import compute_dist
import random
import operator


def null_element():
    return 0, 0


def add_tuple(t1, t2):
    return tuple(map(operator.add, t1, t2))


def div_tuple(t1, integ):
    return (t1[0] / integ, t1[1] / integ)

def sample(iterator, k):
    """
    Samples k elements from an iterable object.

    :param iterator: an object that is iterable
    :param k: the number of items to sample
    """
    # fill the reservoir to start
    result = [next(iterator) for _ in range(k)]

    n = k - 1
    for item in iterator:
        n += 1
        s = random.randint(0, n)
        if s < k:
            result[s] = item
    return result


def naive_random_selector(rows, k, size):
    return [rows[random.randint(0, size - 1)] for _ in range(k)]


def kmeans(Bdd, table, element, k):
    print("Start k-means of ", k, " iterations")
    rows = Bdd.select_all(table)
    # centroids = [getattr(c, element) for c in sample(iter(list(rows)), k)]
    print("Choose random points to start with")
    centroids = [getattr(c, element) for c in naive_random_selector(rows, 5, 1000)]
    count = 0
    while True:
        print("Iteration ", count)
        new_centroids = one_iter_kmean(rows, element, k, centroids)
        count += 1
        if new_centroids == centroids:
            break
        centroids = new_centroids
    return centroids


def one_iter_kmean(rows, attr, k, centroids, limit=None):
    summary_centroids = [[0, null_element()] for i in range(k)]
    count = 0
    for row in rows:
        elem = getattr(row, attr)
        count += 1
        if None not in elem:
            d = [compute_dist(elem, c) for c in centroids]
            k = d.index(min(d))
            summary_centroids[k][0] += 1
            summary_centroids[k][1] = add_tuple(summary_centroids[k][1], elem)
            if count == limit:
                break
    print(summary_centroids)
    return [div_tuple(s[1], s[0]) for s in summary_centroids]