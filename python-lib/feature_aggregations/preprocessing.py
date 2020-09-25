# coding: utf-8
import itertools
import datetime
import pandas as pd
import numpy as np
import logging

import dataiku
from dataiku import pandasutils as pdu
from dataiku.core.sql import SQLExecutor2

logger = logging.getLogger('afe')


class CardinalityLimiterStrategy:
    MAX_NUM_CATEGORIES = 'MAX_NUM_CATEGORIES' # return the first ``max_num_categories``.
    MIN_SAMPLES = 'MIN_SAMPLES' #groups all categories having less than ``min_count samples``.
    CUMULATIVE_THRESHOLD = 'CUMULATIVE_THRESHOLD' # return the cumulative series and regroup all categories above the cumulative threshold.


class CardinalityLimiterParams():
    """
    min_count_threshold: int
        Minimum count for a category to be keeped.
    cumulative_threshold: int
        Percentage threshold for the cumulative count ratio.
    """

    def __init__(self,
                strategy=CardinalityLimiterStrategy.CUMULATIVE_THRESHOLD,
                max_num_categories=30,
                min_count_threshold=100,
                cumulative_threshold=95,
                clipping_categories_difference_threshold=20
                ):

        self.max_safety_num_categories = 100 # TODO do we let the user define this variable ?

        self.strategy = strategy
        self.max_num_categories = max_num_categories
        self.min_count_threshold = min_count_threshold
        self.cumulative_threshold = cumulative_threshold
        self.clipping_categories_difference_threshold = clipping_categories_difference_threshold

    @staticmethod
    def deserialize(serialized_params):
        params = CardinalityLimiterParams()
        params.__dict__ = serialized_params
        params.check()
        return params

    def check(self):
        ALLOWED_STRATEGIES = [
            "MAX_NUM_CATEGORIES",
            "MIN_SAMPLES",
            "CUMULATIVE_THRESHOLD"
        ]

        if self.strategy not in ALLOWED_STRATEGIES:
            raise ValueError(" '{}' is an unknown clipping method. Please choose only method in the following list: {}".format(self.strategy, ALLOWED_STRATEGIES))

        if self.max_safety_num_categories <= self.max_num_categories:
            logger.info("Warning, the chosen max_num_categories is too high!".format(self.max_safety_num_categories))


class CardinalityLimiter():
    def __init__(self, params=CardinalityLimiterParams()):
        self.params = params
        self.params.check()

    def categorical_variable_clipping(self, pandas_series):
        """
        For now, we don't create the column corresponding to "None" because of a SQL problem: for the None cell, we can't find them using the IN syntax, which
        means that ``category_id_x IN ("None")`` won't give a correct answer.

        Parameters
        ----------
        pandas_series: pandas series.
            A series of format (category, count) sorted desceding

        Returns
        -------
        candidates: list of list
            Each element of ``candidates`` is the remained values.

            For example: if a column has five valus from x1 to x5 and we want to keep 3 of them only, after clipping we have [[x1],[x3],[x5]]
        """
        candidates = []

        if self.params.strategy == CardinalityLimiterStrategy.MAX_NUM_CATEGORIES:
            candidates = [k for (k, v) in pandas_series.iloc[0:self.params.max_num_categories].iteritems() if k != "None"]

        if self.params.strategy == CardinalityLimiterStrategy.MIN_SAMPLES:
            candidates = [k for (k, v) in pandas_series.iloc[0:self.params.max_num_categories].iteritems() if v >= self.params.min_count_threshold and k != "None"]

        if self.params.strategy == CardinalityLimiterStrategy.CUMULATIVE_THRESHOLD:
            cumsummed = pandas_series.cumsum()
            total_count = sum(pandas_series.values)
            candidates = [k for (k, v) in cumsummed.iteritems() if 100.*v/total_count <= self.params.cumulative_threshold and k != "None"]
            candidates = candidates[:self.params.max_num_categories]

        logger.info("{0}/{1} values are kept.".format(len(candidates), len(pandas_series)))
        return [[c] for c in candidates]
