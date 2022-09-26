from unittest import TestCase

from TripleQueryWrapper import TripleQueryWrapper


class TripleQueryWrapperTests(TestCase):
    def setup(self):
        self.triple_query_wrapper = TripleQueryWrapper(use_graph_db=False)
        self.triple_query_wrapper.init_params()

    def test_save_to_params(self):
        self.setup()
        params_dict = {'fresh_sync': True, 'pagingcursor': '=test='}
        self.triple_query_wrapper.save_to_params(params_dict)
        result = self.triple_query_wrapper.get_params()
        self.assertDictEqual(params_dict, result)
