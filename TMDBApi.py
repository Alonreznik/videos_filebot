import os
from multiprocessing import dummy
import requests
import json
import urllib
from datetime import datetime

BASE_URL = 'http://api.themoviedb.org/{v}/{method}?api_key={api_key}{a}{params}'
pool_ = dummy.Pool(8)


class TMDBApi:
    def __init__(self, api_key, api_version):
        self.api_key = api_key
        self.api_version = api_version

        self.base_headers = {'Accept': 'application/json'}
        self.expires_date_format = '%Y-%m-%d %H:%M:%S UTC'
        self.token = {}

        self.json_rows = []

    def get_method_url(self, method, required_params={}):
        more_params = []
        if required_params:
            more_params = ['{k}={v}'.format(k=k, v=v)
                           for k, v in required_params.iteritems()]
        return BASE_URL.format(v=str(self.api_version),
                               api_key=self.api_key,
                               method=method,
                               a='&' if more_params else '',
                               params='&'.join(more_params))

    def get_token(self):
        if self.token.get('expires', datetime.utcnow()) <= datetime.utcnow():
            token_method = self.get_method_url(method='/authentication/token/new')
            res = requests.request('GET', token_method,
                                   headers=self.base_headers)
            if res.status_code == requests.codes.ok:
                response_ = self._is_succeed(res.json())
                self.token = {'token': response_['request_token'],
                              'expires': datetime.strptime(response_['expires_at'],
                                                           self.expires_date_format)}

    @staticmethod
    def _is_succeed(response):
        # hanlde the response if succeed
        try:
            if response.get('success'):
                return response
            else:
                raise Exception(response.get('msg', ''))
        except Exception:
            raise

    def authenticate(self, username, password):
        # get new token if expires or not exists
        self.get_token()
        # make auth request
        auth_req = self.get_method_url('authentication/token/validate_with_login',
                                       required_params={
                                           'request_token': self.token.get('token'),
                                           'username': username,
                                           'password': password})
        res = requests.get(auth_req)
        return self._is_succeed(res.json())

    def dicover(self, queries={}, additional_movie_data=[], is_write_to_file=False,
                file_=None):
        """search the tmdb by list of queries.
           the full list can get  here:
           http://docs.themoviedb.apiary.io/#reference/discover/discovermovie
        """
        if not self.token:
            self.get_token()

        method_ = 'discover/movie'

        url_ = self.get_method_url(method_, queries)
        page = 1
        res = requests.get(url_, headers=self.base_headers, stream=True)
        all_movies = []
        if res.status_code == requests.codes.ok:
            res_dict = res.json()
            total_pages = res_dict.get('total_pages', 1)
            while page <= total_pages:
                percent_ = (page/float(total_pages))*100
                print 'loading: %{i}'.format(i=str(percent_))

                if is_write_to_file:
                    file_ = self.__download_data(res, file_)
                else:
                    all_movies += (res_dict.get('results') or [])
                page += 1
                queries.update({'page': page})
                url_ = self.get_method_url(method_, queries)
                res = requests.get(url_, headers=self.base_headers)
            else:
                if isinstance(file_, file):
                    file_.close()

            if is_write_to_file:
                return True
            else:
                return all_movies

    def get_movie_data(self, movie_id, data_to_return=[]):
        if not self.token:
            self.get_token()

        method_ = 'movie/{_id}'.format(_id=movie_id)
        append_to_response = ','.join(data_to_return or [])
        url_ = self.get_method_url(method_,
                                   {'append_to_response': append_to_response})
        res = requests.get(url_, headers=self.base_headers, stream=True)

        if res.status_code == requests.codes.ok:
            return res.json()

    def __download_data(self, response, file_, chunk_size=1000,
                        additional_data=[]):

        p_dict = response.json()
        results_ = p_dict.get('results', [])

        new_res = pool_.map(self.__make_json, results_)

        self.json_rows += new_res

        if len(self.json_rows) >= chunk_size \
                or p_dict.get('page') == p_dict.get('total_pages'):
            if isinstance(file_, file):
                file_.writelines(self.json_rows)
                self.json_rows = []
                return file_
            else:
                file_dir = os.path.dirname(os.path.abspath(file_))
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                f = open(file_, 'a')
                f.writelines(self.json_rows)
                self.json_rows = []
                return f
        else:
            return file_

    def __make_json(self, json_, additional_data=['keywords']):
        if additional_data:
            new_json = json.dumps(self.get_movie_data(movie_id=json_.get('id'),
                                                      data_to_return=additional_data)) + '\n'
        else:
            new_json = json.dumps(json_)+'\n'
        return new_json
