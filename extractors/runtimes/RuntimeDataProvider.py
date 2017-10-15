import time
import json
import requests
import pandas as pd
import sys, os
from parsers.netflix.ActivityParser import NetflixActivityParser

with open('../../data/secrets.json') as f:
    secrets = json.load(f)

def update_catalog():
    """Update the tv shows and movie runtime catalog with latest netflix data"""
    ap = NetflixActivityParser('../../data/netflix/NetflixData.htm')
    nfx = pd.DataFrame(ap.parse())

    # Get updated list of TV Shows
    updated = nfx[~nfx.movie].groupby('series').sum()
    updated = updated.rename(columns={'movie': 'runtime'})
    updated['runtime'] = 0

    # Read list of processed TV Shows (with runtime data)
    old = pd.read_csv('../../data/netflix/runtimes_tv.csv', index_col='series')

    # Merge dataframes
    updated.loc[updated.index.isin(old.index), ['runtime']] = old['runtime']
    print(updated.head())

    # Dump updated TV Shows
    updated.to_csv('../../data/netflix/runtimes_tv.csv')

    # Get updated list of Movies
    updated = nfx[nfx.movie].reset_index()
    updated['runtime'] = 0
    updated = updated[['title', 'runtime']]
    updated = updated.set_index('title')

    # Read list of processed movies with runtime data
    old = pd.read_csv('../../data/netflix/runtimes_movies.csv', index_col='title')

    # Merge dataframes
    updated.loc[updated.index.isin(old.index), ['runtime']] = old['runtime']
    print(updated.head())

    # Dump updated movies
    updated.to_csv('../../data/netflix/runtimes_movies.csv')

def get_movie_runtime(movie):
    url = 'http://www.omdbapi.com/?apikey={}&t={}'.format(secrets['OMDb']['key'], movie)
    r = requests.get(url)
    if ('Error' in r.json()): return 0
    print('Got data for {} : {}'.format(movie, r.json()))
    return int(r.json()['Runtime'].split('min')[0].strip())

def get_movie_runtimes():
    """Use omdbapi to get movie runtime data"""
    movies = pd.read_csv('../../data/netflix/runtimes_movies.csv')

    for i, row in movies[movies.runtime == 0].iterrows():
        movies.loc[i, 'runtime'] = get_movie_runtime(row['title'])
        movies.to_csv('../../data/netflix/runtimes_movies.csv', index=False)
        time.sleep(0.5)

update_catalog()
get_movie_runtimes()
