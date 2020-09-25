# coding: utf-8
import ast
import re
import logging

from feature_aggregations import *


def get_aggregation_params(recipe_config):
    def _p(param_name, default=None):
        return recipe_config.get(param_name, default)

    params = AggregationParams(_p('time_reference_type'))
    params.keys = _p('aggregation_keys')
    params.timestamp_column = _p('timestamp_column')

    if _p('input_features_definition') == 'auto': #TODO FIXME don't start computing yet!!
        ignored_columns = _p('ignored_columns', [])
        ignored_columns.extend(params.keys)
        ignored_columns.append(params.timestamp_column)
        ratio_threshold = _p('ratio_threshold', 0.001)
        if ratio_threshold <= 0: #TODO move to check
            raise ValueError('Ratio percentage threshold needs to be between 0 and 100')
        params.auto_infering_col_type = True
        params.ignored_columns = ignored_columns
        params.ratio_threshold = ratio_threshold
        #params.numerical_columns, params.categorical_columns = _infer_column_types(dataset=dataset, ignored_columns=ignored_columns, row_limit=1000000, threshold=ratio_threshold) #TODO move! no processing here
    else:
        params.numerical_columns = _p('numerical_columns', [])
        params.categorical_columns = _p('categorical_columns', [])

    if _p('use_temporal_window'):
        params.windows = []
        for unit in WindowWidthUnit.get_available_units(params.time_reference_type):
            window_sizes = _p('windows_widths_{}'.format(unit.lower()), '')
            try:
                width_values = [int(x) for x in window_sizes.split(',') if len(x.strip())]
                for w in width_values:
                    if w <= 0:
                        raise ValueError('Invalid recipe params: window width must be positive'.format(unit))
                    params.windows.append((w, unit.lower()))
            except:
                raise ValueError('Invalid recipe params: window width must be an integer'.format(unit))
        params.whole_history_window_enabled = _p('whole_history_window_enabled')
        if params.whole_history_window_enabled:
            params.windows.append((None, None))
        params.num_windows_per_type = int(_p('num_windows_per_type', 1))
    else:
        params.whole_history_window_enabled = True
        params.windows = [(None, None)]

    params.populations_dict = {}
    params.populations_mode = None

    #TODO move
    if _p('advance_activate'):
        params.encoding_feature = _p('encoding_feature')
        params.populations_mode = _p('population_definition_method')
        if params.populations_mode == PopulationsDefinitionMode.MANUAL:
            num_populations = int(recipe_config['num_populations'])
            if num_populations < 0:
                raise ValueError('Number of sub-population needs to be a positive integer')
            for population_idx in xrange(1, num_populations+1):
                raw_populations_dict = _p('manual_populations_dict_1{}'.format(population_idx), {})
                temp_populations_dict = {}
                for col in raw_populations_dict:
                    val_list = '[{}]'.format(raw_populations_dict[col])
                    final_val = re.sub(r'([\w\s_]+)', r'"\1"', val_list)
                    temp_populations_dict[col] = ast.literal_eval(final_val)
                new_condition_list= [('"{}"'.format(key), val) for key, val in temp_populations_dict.items()]
                if len(new_condition_list) > 0:
                    params.populations_dict['populations_{}'.format(population_idx)] = new_condition_list
        elif params.populations_mode == PopulationsDefinitionMode.BRUTE_FORCE:
            raw_populations_dict = _p('brute_force_populations_dict', {})
            for col in raw_populations_dict:
                list_of_val_in_string = raw_populations_dict[col].strip()
                val_list = re.findall(r'\[[^\]]+\]', list_of_val_in_string)
                params.populations_dict[col] = []
                for val in val_list:
                    final_val = re.sub(r'([\w_]+)', r'"\1"', val)
                    params.populations_dict[col].append(ast.literal_eval(final_val))
    params.check()
    return params


def get_transform_params(recipe_config):
    aggregation_params = get_aggregation_params(recipe_config)  
    aggregation_params.check()
    transform_params = TransformParams()
    transform_params.ref_date = recipe_config.get('ref_date', None)
    if transform_params.ref_date == '':
        transform_params.ref_date = None
    
    if recipe_config.get('use_buffer'):
        transform_params.buffer_unit = recipe_config.get('buffer_unit', 'row')
        if transform_params.buffer_unit is None:
            raise ValueError('Buffer unit not specified')
        transform_params.buffer_width = recipe_config.get('buffer_width', None)
        if transform_params.buffer_width is None:
            raise ValueError('Buffer width not specified')
        try:
            transform_params.buffer_width = int(transform_params.buffer_width)
        except:
            raise ValueError('Buffer width must be an integer')
    transform_params.check(aggregation_params.time_reference_type)
    return transform_params


def get_cardinality_limiter_params(recipe_config):
    #TODO
    return CardinalityLimiterParams()


#TODO move! no processing here
def _infer_column_types(dataset, ignored_columns=[], row_limit=100000, threshold=1):
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
                    logger.info(" -> Column '{}' is numerical (value_diversity_percentage: {} >= {})".format(col, value_diversity_percentage, threshold))
                    numerical_columns.append(col)
                else:
                    logger.info(" -> Column '{}' is categorical (value_diversity_percentage: {} < {})".format(col, value_diversity_percentage, threshold))
                    categorical_columns.append(col)
        elif df[col].dtype == str or df[col].dtype == object: # V2 how about other type ?
            logger.info(" - Column '{}' is categorical (dtype: {})".format(col, df[col].dtype))
            categorical_columns.append(col)
        else:
            logger.info(" - Column '{}' is of unknown type, ignored. (dtype: {})".format(col, df[col].dtype))
    return numerical_columns, categorical_columns