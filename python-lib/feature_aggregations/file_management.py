# coding: utf-8
import json
import os.path
import logging

from feature_aggregator import AggregationParams
from preprocessing import CardinalityLimiterParams

logger = logging.getLogger('afe')

# TODO write in several files?
class FileManager():

    def __init__(self, dss_folder, filename='feature_aggregator.json'):
        self.folder_path = dss_folder.get_path()
        self.filename = filename

    def write_feature_aggregator_config(self, feature_aggregator):
        self._write({
            "aggregation_params": feature_aggregator.params,
            "cardinality_limiter_params": feature_aggregator.cardinality_limiter_params,
            "categorical_columns_stats": feature_aggregator.categorical_columns_stats
        })

    def _write(self, obj):
        content = json.dumps(obj, sort_keys=True, default=lambda o: o.__dict__, indent=4)
        logger.info("Write file: "+content)
        with open(os.path.join(self.folder_path, self.filename), 'w') as f:
            f.write(content)

    def read_feature_aggregator_config(self):
        serialized_params = self._read()
        logger.info('serialized_params: '+json.dumps(serialized_params))
        serialized_ap = serialized_params.get('aggregation_params', None)
        if serialized_ap is None:
            raise ValueError('No aggregation params in file')
        aggregation_params = AggregationParams.deserialize(serialized_ap)
        serialized_clp = serialized_params.get('cardinality_limiter_params', {})
        cardinality_limiter_params = CardinalityLimiterParams.deserialize(serialized_clp)
        categorical_columns_stats = serialized_params.get('categorical_columns_stats', {})
        return aggregation_params, cardinality_limiter_params, categorical_columns_stats

    def _read(self):
        with open(os.path.join(self.folder_path, self.filename)) as f:
            return json.load(f)