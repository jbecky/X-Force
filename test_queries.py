import unittest
import pandas as pd
import os
from queries import cdc_query, trv_query, aux_query, query_all  

# General-format unit test for queries.py. Sample chemicals and expected outputs have to be imported for it to run.

class TestQueries(unittest.TestCase):
    def test_cdc_query(self):
        cas_test_values = ["SAMPLE_CAS_1", "SAMPLE_CAS_2"]
        expected_outputs = [] 
        for cas, expected_output in zip(cas_test_values, expected_outputs):
            result = cdc_query(cas = cas)
            pd.testing.assert_frame_equal(result, expected_output)

    def test_trv_query(self):
        cas_test_values = ["SAMPLE_CAS_1", "SAMPLE_CAS_2"]
        expected_outputs = []
        for cas, expected_output in zip(cas_test_values, expected_outputs):
            result = trv_query(cas = cas)
            pd.testing.assert_frame_equal(result, expected_output)

    def test_aux_query(self):
        cas_test_values = ["SAMPLE_CAS_1", "SAMPLE_CAS_2"]
        expected_outputs = []
        for cas, expected_output in zip(cas_test_values, expected_outputs):
            result = aux_query(cas = cas)
            for key in result:
                pd.testing.assert_frame_equal(result[key], expected_output[key])
    
    def test_query_all(self):
        cas_test_values = ["SAMPLE_CAS_1", "SAMPLE_CAS_2"]
        expected_files = ["EXPECTED_PATH_1", "EXPECTED_PATH_2"]
        for cas, expected_file in zip(cas_test_values, expected_files):
            query_all(cas_list=[cas])
            self.assertTrue(os.path.exists(expected_file))

if __name__ == "__main__":
    unittest.main()
