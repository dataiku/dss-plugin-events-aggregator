# coding: utf-8
import itertools
import json
import logging
import pandas as pd
import numpy as np

import dataiku
from dataiku.core.sql import SQLExecutor2, HiveExecutor
from dataiku.sql import toSQL
from dataiku.sql import Expression, Column, List, Constant, Interval, SelectQuery, Window, TimeUnit, ColumnType


import sql_generation
from preprocessing import CardinalityLimiter
from sql_generation import dialectHandler
import aggreagation_queries as agg

logger = logging.getLogger('afe')


class FeatureAggregator:

    def __init__(self, aggregation_params, cardinality_limiter_params, categorical_columns_stats={}):
        self.params = aggregation_params
        self.cardinality_limiter_params = cardinality_limiter_params
        self.categorical_columns_stats = categorical_columns_stats

    def fit(self, dataset):
        logger.info('------------------------------------------------')
        logger.info('FIT')
        
        if self.params.auto_infering_col_type:
            numerical_columns, categorical_columns = self._infer_column_types(dataset, ignored_columns=self.params.ignored_columns, threshold=self.params.ratio_threshold)
            self.params.numerical_columns = numerical_columns
            self.params.categorical_columns = categorical_columns
            logger.info('Infering column type')
            logger.info('Numerical columns: ',numerical_columns)
            logger.info('Categorical columns: ',categorical_columns)
        
        if len(self.params.categorical_columns) > 0:
            self._compute_categorical_columns_stats(dataset)
        else:
            logger.info('No categorical columns to analyse')
            

    def transform(self, input_dataset, output_dataset, transform_params):
        logger.info('------------------------------------------------')
        logger.info('TRANSFORM')
        self._check_populations()
        resolved_transform_params = self._compute_resolved_transform_params(transform_params, input_dataset)
        resolved_windows = self._compute_resolved_windows(input_dataset, resolved_transform_params)
        queries = self._make_queries(input_dataset, resolved_transform_params, resolved_windows)
        full_query_sql, reverse_mapping_dict = sql_generation.make_full_transform_query(queries, input_dataset, self.params, resolved_transform_params, encoding_feature=self.params.encoding_feature)
                
        dialectHandler(input_dataset).execute_in_database(full_query_sql, output_dataset)
        self._update_feature_description(output_dataset, reverse_mapping_dict)

    def _infer_column_types(self, dataset, ignored_columns=[], row_limit=1000000, threshold=1):
        df = dataset.get_dataframe(limit=row_limit) # should the limit be a parameter ?
        numerical_columns = []
        categorical_columns = []
        columns = [col for col in df.columns if col not in ignored_columns]
        for col in columns:
            if(df[col].dtype == np.float64 or df[col].dtype == np.int64):
                num_unique = df[col].nunique()
                logger.info(" - Column '{}'. unique values: {}.".format(col, num_unique))
                if num_unique > 1:
                    value_diversity_percentage = 100*float(num_unique)/len(df) # ratio between num unique and num row
                    if value_diversity_percentage >= threshold:
                        logger.info(" -> Column '{}' is numerical (value_diversity_percentage: {}% >= {}%)".format(col, value_diversity_percentage, threshold))
                        numerical_columns.append(col)
                    else:
                        logger.info(" -> Column '{}' is categorical (value_diversity_percentage: {}% < {}%)".format(col, value_diversity_percentage, threshold))
                        categorical_columns.append(col)
            elif df[col].dtype == str or df[col].dtype == object: # V2 how about other type ?
                logger.info(" - Column '{}' is categorical (dtype: {})".format(col, df[col].dtype))
                categorical_columns.append(col)
            else:
                logger.info(" - Column '{}' is of unknown type, ignored. (dtype: {})".format(col, df[col].dtype))
        return numerical_columns, categorical_columns
        
    def _compute_resolved_transform_params(self, transform_params, input_dataset):
        if transform_params.ref_date is not None:
            logger.info('Reference date provided: '+transform_params.ref_date)
            return transform_params;
        else:
            logger.info('No reference date provided, compute the most recent timestamp:')
            most_recent = self._compute_most_recent_timestamp(self.params.timestamp_column, input_dataset) #TODO move the computation later?
            logger.info('most recent timestamp:' + most_recent)
            return TransformParams(buffer_unit=transform_params.buffer_unit, buffer_width=transform_params.buffer_width, ref_date=most_recent)

    def _compute_most_recent_timestamp(self, timestamp_column, dataset):
        sql = sql_generation.make_most_recent_timestamp_query(timestamp_column, dataset)
        df = dialectHandler(dataset).get_executor().query_to_df(sql)
        if df.empty:
            raise RuntimeError("Empty input dataset")
        return df['most_recent_timestamp'][0]

    # TODO don't change things in place
    def _compute_categorical_columns_stats(self, dataset):
        """
        Compute categorical columns cardinality
        In case the column has more distinct values than allowed, apply clipping strategy.
        """
        summary = {}
        for col in self.params.categorical_columns:
            logger.info("Analysing categorical column '{}'".format(col))
            value_occurence_dict = self._compute_distinct_values(col, dataset) # [("category_1", 10)("category_2", 8),...]
            value_series = pd.Series(value_occurence_dict.values(), value_occurence_dict.keys()).sort_values(ascending=False)  # sort by decending order of occurences
            if len(value_series) > self.cardinality_limiter_params.max_num_categories: # TODO move?
                logger.info("Applying clipping method on column {}:".format(col))
                candidates = CardinalityLimiter(self.cardinality_limiter_params).categorical_variable_clipping(value_series)
            else:
                candidates = [val for val in value_occurence_dict.keys() if val != "None"]
                candidates = [[str(int(float(k)))] if is_number(k) else [k] for k in candidates] # this mess is to avoid problem with float string ("1.0") when naming sql table, ie cant have something like "table_1.0"
            summary[col] = candidates # update the log dict

        self.categorical_columns_stats = {}
        for col, val in summary.items():
            self.categorical_columns_stats[col] = [x[0] for x in val]

        #TODO do something with categorical_columns_stats

    def _compute_distinct_values(self, column, dataset):
        sql_query = sql_generation.make_distinct_values_query(column, dataset)
        res = dialectHandler(dataset).get_executor().query_to_iter(sql_query)
        return {(res_item[0]).encode('utf-8'): res_item[1] for res_item in res.iter_tuples()} # {value1: occurence1, ...}


    #TODO too complex
    def _compute_resolved_windows(self, dataset, resolved_transform_params):
        logger.info("_compute_resolved_windows")
        return []
        # if there is the 'whole_history' keyword in the window dict, we compute the max interval (= all) in DAYS for fixed timestamp, or add "unbounded" condition for rolling window
        # if self.params.windows.get('whole_history', False):
        #     if self.params.is_rolling_window():
        #         self.params.windows['row'].append('unbounded')
        #     else:
        #         max_interval_in_days = self._compute_max_time_interval(self.params.timestamp_column, resolved_ref_date, dataset)
        #         self.params.windows['day'].append(int(max_interval_in_days))
        #     # we are done with 'whole_history', remove it from the temporal dict now
        #     self.params.windows.pop('whole_history', None)

    def _compute_max_time_interval(self, timestamp_column, resolved_ref_date, dataset):
        sql = sql_generation.make_max_time_interval_query(timestamp_column, resolved_ref_date, dataset)
        df = dialectHandler(dataset).get_executor().query_to_df(sql)
        if df.empty:
            raise RuntimeError("Empty input dataset")
        max_time_interval = df['max_time_interval'][0]
        if max_time_interval < 0:
            raise ValueError("The chosen reference date ({}) does not exist in the database.".format(resolved_ref_date)) #TODO misleading error message or buggy condition?
        return max_time_interval

    #TODO rename or redo, check should not edit
    def _check_populations(self):
        if self.params.populations_mode == PopulationsDefinitionMode.MANUAL:
            if len(self.params.populations_dict) > 0:
                populations_values = self.params.populations_dict.values()
            else:
                populations_values = [[]]
        else: # brute_force (or None?)
            # We loop the populations_dict and create one list for each granularity column, for example if our granularity dict is {'event_type': ['buy_order'], 'event_dayofweek': [1,2,3]}
            #       we will have something like [ ['buy_order'], [1,2,3] ]
            # Finally, we find all the possible combination of the list above (buy_order and 1, buy_order and 2, ...)
            populations_values = [zip(self.params.populations_dict, prod) for prod in itertools.product(*(self.params.populations_dict[key] for key in self.params.populations_dict))]
            # In the end we have something like this: [[('event_dayofweek', ['1', '2', '3']), ('event_type', ['buy_order'])], [('event_dayofweek', ['4', '5']), ('event_type', ['buy_order'])],...]
        self.params.populations = []
        for population_combination in populations_values:
            print('POPULATION: ', population_combination)
            conditions = []
            # for example, if we have population_combination = [[event_timestmap, ['buy_order']], [event_dayofweek, ['1','2','3']]]
            for combination_val in population_combination:
                condition_column = combination_val[0]
                single_cond_expr = Column(condition_column).in_(List(*combination_val[1]))
                conditions.append(single_cond_expr)
            if len(conditions) > 0:
                population = conditions[0]
                for condition_expr in conditions[1:]:
                    population = population.and_(condition_expr)
                self.params.populations.append(population)
            else:
                self.params.populations.append(None)
        logger.info("There are {} population(s)".format(len(self.params.populations)))

    def _make_queries(self, dataset, resolved_transform_params, resolved_windows):
        all_queries = []

        def add_queries(func):
            queries = func(dataset, self.params, resolved_transform_params, self.categorical_columns_stats) #TODO forward resolved_windows
            all_queries.extend(queries)
            
        print(self.params.selected_aggregation_types)
            
        if 'frequency' in self.params.selected_aggregation_types:
            if self.params.time_reference_type == 'fixed_timestamp':
                add_queries(agg.frequency_query_with_groupby)
            else: 
                add_queries(agg.frequency_query_with_window)

        if 'recency' in self.params.selected_aggregation_types:
            if self.params.time_reference_type == 'fixed_timestamp':
                add_queries(agg.recency_query_with_groupby)
            else: 
                add_queries(agg.recency_query_with_window)

        if 'information' in self.params.selected_aggregation_types:
            if self.params.time_reference_type == 'fixed_timestamp':
                add_queries(agg.information_query_with_groupby)
            else:
                add_queries(agg.information_query_with_window)

        if 'distribution' in self.params.selected_aggregation_types:
            if self.params.time_reference_type == 'fixed_timestamp':
                add_queries(agg.distribution_query_with_groupby)
            else:
                add_queries(agg.distribution_query_with_window)

        if 'monetary' in self.params.selected_aggregation_types:
            if self.params.time_reference_type == 'fixed_timestamp':
                add_queries(agg.monetary_query_with_groupby)
            else:
                add_queries(agg.monetary_query_with_window)

        if 'cross_reference_count' in self.params.selected_aggregation_types:
            if self.params.time_reference_type == 'fixed_timestamp':
                add_queries(agg.cross_reference_count_query_with_groupby)
            else:
                add_queries(agg.cross_reference_count_query_with_window)
                
        if 'custom' in self.params.selected_aggregation_types:
            if self.params.time_reference_type == 'fixed_timestamp':
                add_queries(agg.custom_query_with_groupby)
            else:
                add_queries(agg.custom_query_with_window)
            
        return all_queries
    
    def _update_feature_description(self, dataset, reverse_mapping_dict):
        
        dataset_schema = dataset.read_schema()
        
        for col_info in dataset_schema:
            col_name = col_info.get('name')
            original_col_name = reverse_mapping_dict.get(col_name, col_name)
            col_info['comment'] = self.params.feature_description.get(original_col_name, '')
 
        dataset.write_schema(dataset_schema)
    
    def _encoding_feature_name(self, dataset):
        
        is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
        dataset_schema = dataset.read_schema()
        
        col_list = []
        new_col_alias = []
        
        for col_index, col_info in enumerate(dataset_schema):
            col_name = col_info.get('name')
            col_list.append(Column(col_name))
            col_name_mapping = self.params.feature_name_mapping.get(col_name)
            if col_name_mapping:
                col_alias = '{}_{}'.format(col_name_mapping, col_index)   
            else: 
                col_alias = col_name
            new_col_alias.append(col_alias)
            
        query_to_rename = SelectQuery().select(col_list, new_col_alias)
        if is_hdfs:
            query_to_rename.select_from('_'.join(dataset.name.split('.')))
        else:
            query_to_rename.select_from(dataset)
        dialect_handler = dialectHandler(dataset)
        dialect_handler.get_executor().exec_recipe_fragment(dataset, 
                                                            query=dialect_handler.convertToSQL(query_to_rename))#toSQL(query_to_rename, dataset=dataset))


class TimeReferenceType:
    FIXED_TIMESTAMP = 'fixed_timestamp'
    ROLLING_WINDOW = 'rolling_window'


class WindowWidthUnit:
    ROW = 'row'
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'
    YEAR = 'year'
    # TODO Why not other units

    @staticmethod
    def get_available_units(time_reference_type):
        if time_reference_type == TimeReferenceType.FIXED_TIMESTAMP:
            return [WindowWidthUnit.DAY, WindowWidthUnit.WEEK, WindowWidthUnit.MONTH, WindowWidthUnit.YEAR]
        if time_reference_type == TimeReferenceType.ROLLING_WINDOW:
            return [WindowWidthUnit.ROW]
        return []

    @staticmethod
    def get_all_available():
        return [WindowWidthUnit.ROW, WindowWidthUnit.DAY, WindowWidthUnit.WEEK, WindowWidthUnit.MONTH, WindowWidthUnit.YEAR]


class AggregationType:
    FREQUENCY = 'frequency'
    RECENCY = 'recency'
    INFORMATION = 'information'
    DISTRIBUTION = 'distribution'
    MONETARY = 'monetary'
    CROSS_REFERENCE_COUNT = 'cross_reference_count'
    CUSTOM = 'custom'
    
class PopulationsDefinitionMode:
    BRUTE_FORCE = 'brute_force'
    MANUAL = 'manual'


class AggregationParams:
    """
    populations_dict: dict, optional (default={})
        For example, if we want to keep rows where ``event_type`` is `buy_order`: populations_dict = {'event_type':[['buy_order']]}
    cross_reference_columns: list of string, optional (default=[])
        List of columns that we will use to count (for example, number of users having the same ip_adress, ...).
    """

    def __init__(self,
            time_reference_type=None,
            keys=[],
            timestamp_column=None,
            windows=[],
            num_windows_per_type=1,
            selected_aggregation_types=[AggregationType.FREQUENCY],
            monetary_column=None,
            categorical_columns=[],
            numerical_columns=[],
            cross_reference_columns=[],
            populations_mode=None,
            populations_dict={},
            whole_history_window_enabled=False,
            auto_infering_col_type=False,
            ignored_columns = [],
            ratio_threshold = 1,
            encoding_feature = False
            ): 

        self.time_reference_type = time_reference_type
        self.keys = keys
        self.timestamp_column = timestamp_column
        self.windows = windows
        self.num_windows_per_type = num_windows_per_type
        #self.selected_aggregation_types = selected_aggregation_types
        self.monetary_column = monetary_column
        self.categorical_columns = categorical_columns
        self.numerical_columns = numerical_columns
        self.cross_reference_columns = cross_reference_columns
        self.populations_mode = populations_mode
        if self.populations_mode == PopulationsDefinitionMode.BRUTE_FORCE:
            self.populations_dict = {'"{}"'.format(k):v for k,v in populations_dict.items()} #TODO What?
        else:
            self.populations_dict = populations_dict
            
        self.whole_history_window_enabled = whole_history_window_enabled
        self.auto_infering_col_type = auto_infering_col_type
        self.ignored_columns = ignored_columns
        self.ratio_threshold = ratio_threshold
        self.encoding_feature = encoding_feature
        
        if self.time_reference_type == 'fixed_timestamp':
            self.selected_aggregation_types = [
                AggregationType.FREQUENCY,
                AggregationType.RECENCY,
                AggregationType.INFORMATION,
                AggregationType.DISTRIBUTION,
                #AggregationType.CUSTOM
            ]
        else:
            self.selected_aggregation_types = [
                AggregationType.FREQUENCY,
                AggregationType.RECENCY,
                AggregationType.INFORMATION,
                #AggregationType.MONETARY,
                #AggregationType.CROSS_REFERENCE_COUNT,
                #AggregationType.CUSTOM
            ]
            
        self.feature_description = {}
        self.feature_name_mapping = {}
        
    @staticmethod
    def deserialize(serialized_params):
        params = AggregationParams()
        params.__dict__ = serialized_params
        params.check()
        return params

    def get_effective_keys(self):
        """
        Grouping keys in the SQL sense. They might be different from keys in the recipe sense:
        For rolling windows, we group by keys AND timestamp
        """
        if self.is_rolling_window():
            return set(self.keys + [self.timestamp_column])
        else:
            return set(self.keys)

    def is_rolling_window(self):
        return self.time_reference_type == TimeReferenceType.ROLLING_WINDOW

    def get_row_based_windows(self):
        return [w for w in self.windows if w[1] in ['row', None]]

    def check(self): #TODO error message wording
        if self.time_reference_type not in [TimeReferenceType.FIXED_TIMESTAMP, TimeReferenceType.ROLLING_WINDOW]:
            raise ValueError("time_reference_type must be 'fixed_timestamp' or 'rolling_window'.")

        if self.keys is None or len(self.keys) == 0:
            raise ValueError('Aggregation keys are required')

        if self.timestamp_column is None or len(self.timestamp_column.strip()) == 0:
            raise ValueError('Timestamp column is required')

        aggregated_columns = set(self.categorical_columns + self.numerical_columns)
        if self.timestamp_column in aggregated_columns:
            raise ValueError('Timestamp column cannot be in aggregated columns')

        if len(set(self.keys).intersection(aggregated_columns)) > 0:
            raise ValueError('Subject columns column cannot be in aggregated columns')

        if len(set(self.categorical_columns).intersection(set(self.numerical_columns))) > 0:
            raise ValueError('There is overlap column(s) between categorical and numerical column list!')

        if self.selected_aggregation_types is None or len(self.selected_aggregation_types) == 0:
            raise ValueError("Empty feature types list. Please choose which feature types to compute.")

        AGGREGATION_TYPES_FOR_ROLLING_WINDOW = [
            AggregationType.FREQUENCY,
            AggregationType.RECENCY,
            AggregationType.INFORMATION,
            AggregationType.MONETARY,
            AggregationType.CROSS_REFERENCE_COUNT
        ]
        AGGREGATION_TYPES_FOR_FIXED_TIMESTAMP = [
            AggregationType.FREQUENCY,
            AggregationType.RECENCY,
            AggregationType.INFORMATION,
            AggregationType.DISTRIBUTION
        ]
        for agg in self.selected_aggregation_types:
            if self.time_reference_type == TimeReferenceType.ROLLING_WINDOW and agg not in AGGREGATION_TYPES_FOR_ROLLING_WINDOW:
                if agg in AGGREGATION_TYPES_FOR_FIXED_TIMESTAMP:
                    raise ValueError('Aggrgation type "{}" can only be used in fixed timestamp mode (not rolling window)'.format(agg))
                else:
                    raise ValueError('Unknown aggrgation type "{}"'.format(agg))
            if self.time_reference_type == TimeReferenceType.FIXED_TIMESTAMP and agg not in AGGREGATION_TYPES_FOR_FIXED_TIMESTAMP:
                if agg in TimeReferenceType.ROLLING_WINDOW:
                    raise ValueError('Aggrgation type "{}" can only be used in  fixed rolling window mode (not fixed timestamp)'.format(agg))
                else:
                    raise ValueError('Unknown aggrgation type "{}"'.format(agg))
        # if monetary_column is None and 'monetary' in self.selected_aggregation_types:
        #     error_message = "You need to precise the monetary column in order to compute its features."
        #     logger.error(error_message)
        #     raise ValueError(error_message)
        
        if self.num_windows_per_type < 1:
            raise ValueError('Number of windows per type needs to be >= 1.')


class TransformParams:
    def __init__(self, buffer_unit=None, buffer_width=0, ref_date=None):
        self.buffer_unit = buffer_unit
        self.buffer_width = buffer_width
        self.ref_date = ref_date

    @staticmethod
    def deserialize(serialized_params):
        params = TransformParams()
        params.__dict__ = serialized_params
        params.check()
        return params

    def check(self, time_reference_type): #TODO make sure we use second parameter when calling this method
        if self.buffer_unit is not None:
            if self.buffer_width < 0: #TODO must be positive?
                raise ValueError('Buffer width can not be negative.')

            if self.buffer_unit.lower() not in WindowWidthUnit.get_all_available():
                raise ValueError('Unknown buffer unit. Possible options are: {}'.format(WindowWidthUnit.get_available_units(time_reference_type)))

            if self.buffer_unit.lower() not in WindowWidthUnit.get_available_units(time_reference_type):
                raise ValueError('{} buffer unit not available in {} mode'.format(self.buffer_unit, time_reference_type))
   
def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False