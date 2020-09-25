# coding: utf-8
import numpy as np
import logging
from dataiku.sql import Expression, Column, List, Constant, Interval, SelectQuery, Window, TimeUnit, ColumnType

logger = logging.getLogger('afe')


def frequency_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]

    for population_idx, population in enumerate(aggregation_params.populations):

        population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

        layer_1 = SelectQuery()
        #layer_1.select_from(dataset)
        if is_hdfs:
            layer_1.select_from('_'.join(dataset.name.split('.')))
        else:
            layer_1.select_from(dataset)
        layer_1.select(keys_expr)

        window = Window(
            partition_by=keys_expr_no_timestamp,
            order_by=[Column(aggregation_params.timestamp_column)],
            end=transform_params.buffer_width,
            end_direction='PRECEDING'
        )

        frequency_expr = Column('*').count().over(window)
        frequency_alias = 'frequency{}'.format(population_suffix)
        frequency_description = "Number of times this event happened in the past. Population filter: {}.".format(population)
        aggregation_params.feature_description[frequency_alias] = frequency_description
        
        layer_1.select(frequency_expr, frequency_alias)
        prefilter = _prefilter(
            query=layer_1,
            aggregation_params=aggregation_params,
            transform_params=transform_params,
            population=population
        )
        layer_1.alias('subquery__frequency_query_with_window'+str(population_idx))
        queries.append(layer_1)
            
    return queries


def frequency_query_with_groupby(dataset, aggregation_params, transform_params, categorical_columns_stats):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    
    for population_idx, population in enumerate(aggregation_params.populations):
        for window_index in xrange(aggregation_params.num_windows_per_type):
            for (window_width, window_unit) in aggregation_params.windows:
                population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)
                window_suffix = _get_window_suffix(window_index, aggregation_params.num_windows_per_type)

                layer_1 = SelectQuery()
                #layer_1.select_from(dataset)
                if is_hdfs:
                    layer_1.select_from('_'.join(dataset.name.split('.')))
                else:
                    layer_1.select_from(dataset)
                layer_1.select(keys_expr)
                
                prefilter = _prefilter(
                    query=layer_1,
                    aggregation_params=aggregation_params,
                    transform_params=transform_params,
                    population=population,
                    window_width=window_width,
                    window_unit=window_unit,
                    window_index=window_index
                )

                layer_2 = SelectQuery()
                layer_2.select_from(layer_1, alias='layer_1')
                for k in keys_expr:
                    layer_2.select(k)
                    layer_2.group_by(k)
                frequency_expr = Column('*').count()
                
                if window_unit:
                    frequency_alias = 'frequency_{}_{}{}{}'.format(window_width, window_unit, window_index, population_suffix)
                    frequency_description = 'Number of time this events happened in the specified window. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                elif window_index == 0:
                    frequency_alias = 'frequency_all_hist{}'.format(population_suffix)
                    frequency_description = 'Number of time this events happened in the past. Population filter: {}.'.format(population)
                else:
                    continue
                    
                layer_2.select(frequency_expr, frequency_alias)
                aggregation_params.feature_description[frequency_alias] = frequency_description

                if window_width:
                    layer_3 = SelectQuery()
                    layer_3.select_from(layer_2, alias='layer_2')
                    layer_3.select(keys_expr)
                    layer_3.select(Column(frequency_alias), frequency_alias)
                    mean_frequency_expr = Column(frequency_alias).cast(ColumnType.FLOAT).div(int(window_width))
                    mean_frequency_alias = 'mean_frequency_{}_{}{}{}'.format(window_width, window_unit, window_index, population_suffix)
                    mean_frequency_description = 'Average number of time this events happened in the specified window. Population: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                    layer_3.select(mean_frequency_expr, mean_frequency_alias)
                    
                    queries.append(layer_3)
                    aggregation_params.feature_description[mean_frequency_alias] = mean_frequency_description
                else:
                    queries.append(layer_2) 
                    
    return queries


def recency_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]
    timestamp_expr = Column(aggregation_params.timestamp_column)

    for population_idx, population in enumerate(aggregation_params.populations):
            population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

            layer_1 = SelectQuery()
            if is_hdfs:
                layer_1.select_from('_'.join(dataset.name.split('.')))
            else:
                layer_1.select_from(dataset)
            layer_1.select(timestamp_expr)
            layer_1.select(keys_expr_no_timestamp)
            
            mean_delta_interval_expr_list = []
            mean_delta_interval_alias_list = []
            std_delta_interval_expr_list = []
            std_delta_interval_alias_list = []
            mean_time_interval_expr_list = []
            mean_time_interval_alias_list = []
            std_time_interval_expr_list = []
            std_time_interval_alias_list = []
            
            for window_index in xrange(aggregation_params.num_windows_per_type):
                window_suffix = _get_window_suffix(window_index, aggregation_params.num_windows_per_type)
                for (window_width, window_unit) in aggregation_params.get_row_based_windows(): # aggregation_params.windows.get('row', ['unbounded']): TODO if empty rows windows
                    if window_unit is not None:
                        start_bound = transform_params.buffer_width + (window_index+1)*window_width - 1
                        end_bound = transform_params.buffer_width + window_index*window_width

                        window = Window(
                            partition_by=keys_expr_no_timestamp,
                            order_by=[timestamp_expr],
                            start=start_bound,
                            end=end_bound,
                            end_direction='PRECEDING'
                            )

                        mean_delta_interval_alias = 'mean_delta_interval_last_{}_times{}{}'.format(window_width, window_suffix, population_suffix)
                        std_delta_interval_alias = 'std_delta_interval_last_{}_times{}{}'.format(window_width, window_suffix, population_suffix)
                        mean_time_interval_alias = 'mean_time_interval_last_{}_times{}{}'.format(window_width, window_suffix, population_suffix)
                        std_time_interval_alias = 'std_time_interval_last_{}_times{}{}'.format(window_width, window_suffix, population_suffix)
                        
                        mean_delta_interval_description = 'Average of the degree of change in time intervals between events in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event from this one'.format(population, end_bound, start_bound)
                        std_delta_interval_description = 'Standard deviation of the degree of change in time intervals between events in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event from this one'.format(population, end_bound, start_bound)
                        mean_time_interval_description = 'Average of time intervals between events in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event from this one'.format(population, end_bound, start_bound)
                        std_time_interval_description = 'Average of time intervals between events in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event from this one'.format(population, end_bound, start_bound)
                        
                    elif window_index == 0: #take all history and when it is the first time
                        window = Window(
                            partition_by=keys_expr_no_timestamp,
                            order_by=[timestamp_expr],
                            end=transform_params.buffer_width,
                            end_direction='PRECEDING'
                            )
                        
                        mean_delta_interval_alias = 'mean_delta_interval_all_hist{}'.format(population_suffix)
                        std_delta_interval_alias = 'std_delta_interval_all_hist{}'.format(population_suffix)
                        mean_time_interval_alias = 'mean_time_interval_all_hist{}'.format(population_suffix)
                        std_time_interval_alias = 'std_time_interval_all_hist{}'.format(population_suffix)
                        
                        mean_delta_interval_description = 'Average of the degree of change in time intervals between events in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward'.format(population, transform_params.buffer_width)
                        std_delta_interval_description = 'Standard deviation of the degree of change in time intervals between events in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward'.format(population, transform_params.buffer_width)
                        mean_time_interval_description = 'Average of time intervals between events in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward.'.format(population, transform_params.buffer_width)
                        std_time_interval_description = 'Average of time intervals between events in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward.'.format(population, transform_params.buffer_width)

                    else: #take all history and it is not the first time -> skip, dont need to dupplicate 
                        continue

                    aggregation_params.feature_description[mean_delta_interval_alias] = mean_delta_interval_description
                    aggregation_params.feature_description[std_delta_interval_alias] = std_delta_interval_description
                    aggregation_params.feature_description[mean_time_interval_alias] = mean_time_interval_description
                    aggregation_params.feature_description[std_time_interval_alias] = std_time_interval_description
                      
                    delta_interval_alias = 'delta_interval{}'.format(population_suffix)
                    delta_interval_description = 'Degree of change in time interval between the {}rd event from this one and the one before that. Population filter: {}.'.format(transform_params.buffer_width, population)
                    aggregation_params.feature_description[delta_interval_alias] = delta_interval_description

                    time_interval_alias = 'time_interval{}'.format(population_suffix)
                    delta_interval_description = 'Time interval between the {}rd event from this one and the one before that. Population filter: {}.'.format(transform_params.buffer_width, population)
                    aggregation_params.feature_description[time_interval_alias] = delta_interval_description

                    mean_delta_interval_expr = Column(delta_interval_alias).avg().over(window)
                    std_delta_interval_expr = Column(delta_interval_alias).std_dev_samp().over(window)
                    mean_time_interval_expr = Column(time_interval_alias).avg().over(window)
                    std_time_interval_expr = Column(time_interval_alias).std_dev_samp().over(window)

                    mean_delta_interval_expr_list.append(mean_delta_interval_expr) 
                    std_delta_interval_expr_list.append(std_delta_interval_expr)
                    mean_time_interval_expr_list.append(mean_time_interval_expr)
                    std_time_interval_expr_list.append(std_time_interval_expr)
                    
                    mean_delta_interval_alias_list.append( mean_delta_interval_alias)
                    std_delta_interval_alias_list.append(std_delta_interval_alias)
                    mean_time_interval_alias_list.append(mean_time_interval_alias)
                    std_time_interval_alias_list.append(std_time_interval_alias)
                    
            
            time_interval_expr = timestamp_expr.lag_diff(1, window).extract(TimeUnit.DAY)
            #time_interval_expr = timestamp_expr.date_diff()
            #time_interval_alias = 'time_interval{}'.format(population_suffix)
            layer_1.select(time_interval_expr, time_interval_alias)
            prefilter = _prefilter(
                query=layer_1,
                aggregation_params=aggregation_params,
                transform_params=transform_params,
                population=population
            )

            layer_2 = SelectQuery()
            layer_2.select_from(layer_1, alias='layer_1')
            layer_2.select(keys_expr)
            layer_2.select(Column(time_interval_alias))

            window_all = Window(
                partition_by=keys_expr_no_timestamp,
                order_by=[timestamp_expr],
                end=transform_params.buffer_width,
                end_direction='PRECEDING'
            )
            
            count_expr = Column('*').count().over(window_all)
            count_alias = 'count{}'.format(population_suffix)
            layer_2.select(count_expr, count_alias)
            
            delta_interval_expr = Column(time_interval_alias).lag_diff(1, window_all)
            layer_2.select(delta_interval_expr, delta_interval_alias)

            latest_timestamp = timestamp_expr.max().over(window_all)
            oldest_timestamp = timestamp_expr.min().over(window_all)
            
            current_day_expr = Constant(transform_params.ref_date).cast(ColumnType.DATE)            
            recency_expr = current_day_expr.minus(latest_timestamp).extract(TimeUnit.DAY)
            recency_alias = 'recency{}'.format(population_suffix)
            recency_description = 'Number of days between the reference date and the {}rd event from this one.'.format(transform_params.buffer_width)
            aggregation_params.feature_description[recency_alias] = recency_description
            layer_2.select(recency_expr, recency_alias)
            
            since_first_expr = timestamp_expr.cast(ColumnType.DATE).minus(oldest_timestamp).extract(TimeUnit.DAY)
            since_first_alias = 'num_day_since_first{}'.format(population_suffix)
            since_first_description = 'Number of days between the {}rd event from this one and the first time we saw it.'.format(transform_params.buffer_width)
            aggregation_params.feature_description[since_first_alias] = since_first_description
            layer_2.select(since_first_expr, since_first_alias)

            layer_2_columns = [Column(recency_alias), Column(since_first_alias), Column(delta_interval_alias), Column(time_interval_alias)] #std_interval_alias, mean_interval_alias,
            layer_2_columns_alias = [recency_alias, since_first_alias, delta_interval_alias, time_interval_alias]

            layer_3 = SelectQuery()
            layer_3.select_from(layer_2, alias='layer_2')
            layer_3.select(keys_expr)
            layer_3.select(layer_2_columns, layer_2_columns_alias)

            layer_3.select(mean_delta_interval_expr_list, mean_delta_interval_alias_list)
            layer_3.select(std_delta_interval_expr_list, std_delta_interval_alias_list)
            layer_3.select(mean_time_interval_expr_list, mean_time_interval_alias_list)
            layer_3.select(std_time_interval_expr_list, std_time_interval_alias_list)

            queries.append(layer_3)
    return queries


def recency_query_with_groupby(dataset, aggregation_params, transform_params, categorical_columns_stats):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_expr = [Column(k) for k in aggregation_params.keys]
    timestamp_expr = Column(aggregation_params.timestamp_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        for window_index in xrange(aggregation_params.num_windows_per_type):
            for (window_width, window_unit) in aggregation_params.windows:
                window_suffix = _get_window_suffix(window_index, aggregation_params.num_windows_per_type)
                population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

                layer_1 = SelectQuery()
                if is_hdfs:
                    layer_1.select_from('_'.join(dataset.name.split('.')))
                else:
                    layer_1.select_from(dataset)
                layer_1.select(keys_expr)
                window = Window(
                    partition_by=keys_expr,
                    order_by=[timestamp_expr]
                )                
                time_interval_expr = timestamp_expr.lag_diff(1, window).extract(TimeUnit.DAY)
                time_interval_alias = 'time_interval{}'.format(population_suffix)
                layer_1.select(time_interval_expr, time_interval_alias)
                layer_1.select(timestamp_expr)
                
                """ 
                prefilter = _prefilter(
                    query=layer_1,
                    aggregation_params=aggregation_params,
                    transform_params=transform_params,
                    population=population
                )
                """
                prefilter = _prefilter(
                    query=layer_1,
                    aggregation_params=aggregation_params,
                    transform_params=transform_params,
                    population=population,
                    window_width=window_width,
                    window_unit=window_unit,
                    window_index=window_index
                )
                
                layer_1_columns = [timestamp_expr, Column(time_interval_alias)]

                layer_2 = SelectQuery()
                layer_2.select_from(layer_1, alias='layer_1')
                layer_2.select(keys_expr)
                layer_2.select(layer_1_columns)
                delta_interval_expr = Column(time_interval_alias).lag_diff(1, window)
                delta_interval_alias = 'delta_interval{}'.format(population_suffix)
                layer_2.select(delta_interval_expr, delta_interval_alias)

                layer_3 = SelectQuery()
                layer_3.select_from(layer_2, alias='layer_2')
                for k in keys_expr:
                    layer_3.select(k)
                    layer_3.group_by(k)
                count_expr = Column('*').count()
                #count_alias = 'count{}'.format(population_suffix)

                current_day_expr = Constant(transform_params.ref_date).cast(ColumnType.DATE)
                if transform_params.buffer_unit is not None:
                    current_day_expr = current_day_expr.minus(Interval(transform_params.buffer_width, transform_params.buffer_unit))
                
                if window_index == 0:
                    recency_expr = current_day_expr.minus(timestamp_expr.max()).extract(TimeUnit.DAY)
                    since_first_expr = current_day_expr.minus(timestamp_expr.min()).extract(TimeUnit.DAY)

                    if window_unit is None:
                        recency_alias = 'recency_history{}'.format(population_suffix)
                        recency_description = 'Number of days between the reference date and the last event. Population filter: {}'.format(population)
                        since_first_alias = 'num_day_since_first_history{}'.format(population_suffix)
                        since_first_description = 'Number of days between the reference datee and the first event. Population filter: {}'.format(population)
                        layer_3.select(recency_expr, recency_alias)
                        layer_3.select(since_first_expr, since_first_alias)
                    else:
                        recency_alias = 'recency_{}_{}{}'.format(window_width, window_unit, population_suffix)
                        recency_description = 'Number of days between the reference date and the last event in the window. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                        since_first_alias = 'num_day_since_first_{}_{}{}'.format(window_width, window_unit, population_suffix)
                        since_first_description = 'Number of days betwen the reference date and the first event in the window. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                        layer_3.select(recency_expr, recency_alias)
                        layer_3.select(since_first_expr, since_first_alias)
                 
                    aggregation_params.feature_description[recency_alias] = recency_description
                    aggregation_params.feature_description[since_first_alias] = since_first_description
                
                if window_unit is None:
                    mean_time_interval_alias = 'mean_interval_history{}{}'.format(window_suffix, population_suffix)
                    std_time_interval_alias = 'std_interval_history{}{}'.format(window_suffix, population_suffix)
                    mean_delta_interval_alias = 'mean_delta_interval_history{}{}'.format(window_suffix, population_suffix)
                    std_delta_interval_alias = 'std_delta_interval_hsitory{}{}'.format(window_suffix, population_suffix)
                    
                    mean_delta_interval_description = 'Average of the degree of change in time intervals between events in all history. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                    std_delta_interval_description = 'Standard deviation of the degree of change in time intervals between events in all history. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                    mean_time_interval_description = 'Average of time intervals between events in all history. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                    std_time_interval_description = 'Average of time intervals between events in all history. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)

                else:
                    mean_time_interval_alias = 'mean_interval_{}_{}{}{}'.format(window_width, window_unit, window_suffix, population_suffix)
                    std_time_interval_alias = 'std_interval_{}_{}{}{}'.format(window_width, window_unit, window_suffix, population_suffix)
                    mean_delta_interval_alias = 'mean_delta_interval_{}_{}{}{}'.format(window_width, window_unit, window_suffix, population_suffix)
                    std_delta_interval_alias = 'std_delta_interval_{}_{}{}{}'.format(window_width, window_unit, window_suffix, population_suffix)

                    mean_delta_interval_description = 'Average of the degree of change in time intervals between events in the specified window. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                    std_delta_interval_description = 'Standard deviation of the degree of change in time intervals between events in the specified window. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                    mean_time_interval_description = 'Average of time intervals between events in the specified window. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)
                    std_time_interval_description = 'Average of time intervals between events in the specified window. Population filter: {}. Temporal window: {} window from the reference date. Window witdth: {} {}'.format(population, window_index, window_width, window_unit)

                aggregation_params.feature_description[mean_delta_interval_alias] = mean_delta_interval_description
                aggregation_params.feature_description[std_delta_interval_alias] = std_delta_interval_description
                aggregation_params.feature_description[mean_time_interval_alias] = mean_time_interval_description
                aggregation_params.feature_description[std_time_interval_alias] = std_time_interval_description
        
        
                mean_interval_expr = Column(time_interval_alias).avg()
                layer_3.select(mean_interval_expr, mean_time_interval_alias)

                std_interval_expr = Column(time_interval_alias).std_dev_samp()
                layer_3.select(std_interval_expr, std_time_interval_alias)

                mean_delta_interval_expr = Column(delta_interval_alias).avg()
                layer_3.select(mean_delta_interval_expr, mean_delta_interval_alias)

                std_delta_interval_expr = Column(delta_interval_alias).std_dev_samp()
                layer_3.select(std_delta_interval_expr, std_delta_interval_alias) 

                queries.append(layer_3)
    return queries


def information_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]
    timestamp_expr = Column(aggregation_params.timestamp_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        for window_index in xrange(aggregation_params.num_windows_per_type):

            population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)
            window_suffix = _get_window_suffix(window_index, aggregation_params.num_windows_per_type)
            
            layer_1 = SelectQuery()
            if is_hdfs:
                layer_1.select_from('_'.join(dataset.name.split('.')))
            else:
                layer_1.select_from(dataset)
            layer_1.select(keys_expr)
            layer_1.select([Column(c) for c in aggregation_params.numerical_columns])
            layer_1.select([Column(c) for c in aggregation_params.categorical_columns])
            
            #TO DO: count distinct for cat columns
            """
            for column in aggregation_params.categorical_columns:
                for (window_width, window_unit) in aggregation_params.windows:
                    if window_unit is not None:
                        start_bound = transform_params.buffer_width + (window_index+1)*window_width - 1
                        end_bound = transform_params.buffer_width + window_index*window_width
                        
                        window1 = Window(
                            partition_by=keys_expr_no_timestamp + [Column(column)],
                            order_by=[timestamp_expr],
                            start=start_bound,
                            end=end_bound, 
                            end_direction='PRECEDING'
                            )
                    else:
                        window1 = Window(
                            partition_by=keys_expr_no_timestamp + [Column(column)],
                            order_by=[timestamp_expr],
                            end=transform_params.buffer_width,
                            end_direction='PRECEDING'
                            )

                    rank_expr = Expression().rank().over(window1)
                    rank_alias = 'rank_{}_{}_rows'.format(column, window_width)
                    layer_1.select(rank_expr, rank_alias)

                    first_seen_expr = timestamp_expr.eq(timestamp_expr.min().over(window1)).coalesce(True)
                    first_seen_alias = 'first_seen_{}_{}_rows'.format(column, window_width)
                    layer_1.select(first_seen_expr, first_seen_alias)
            """
            prefilter = _prefilter(
                query=layer_1,
                aggregation_params=aggregation_params,
                transform_params=transform_params,
                population=population,
                window_index=window_index
            )

            layer_2_columns = []
            layer_2_columns_alias = []
            layer_2 = SelectQuery()
            layer_2.select_from(layer_1, alias='layer_1')
            layer_2.select(keys_expr)
            layer_2.select([Column(c) for c in aggregation_params.categorical_columns])
            
            """
            for column in aggregation_params.categorical_columns:
                for (window_width, window_unit) in aggregation_params.windows:
                    rank_col = 'rank_{}_{}_rows'.format(column, window_width)
                    if window_width is not None:
                        start_bound = transform_params.buffer_width + (window_index+1)*window_width - 1
                        end_bound = transform_params.buffer_width + window_index*window_width
                        
                        window = Window(
                            partition_by=keys_expr_no_timestamp + [Column(column)],
                            order_by=[timestamp_expr],
                            start=start_bound,
                            end=end_bound,
                            end_direction='PRECEDING'
                            )
                    else:
                        window = Window(
                            partition_by=keys_expr_no_timestamp + [Column(column)],
                            order_by=[timestamp_expr],
                            end=transform_params.buffer_width,
                            end_direction='PRECEDING'
                            )
                        
                    first_seen_condition_1_expr = Column(rank_col).eq(Column(rank_col).lag(1).over(window)).coalesce(Constant(False)).eq(Constant(False)) #TODO what?
                    first_seen_condition_2_expr = Column(first_seen_alias).eq(Constant(True))
                    first_seen_expr = first_seen_condition_1_expr.and_(first_seen_condition_2_expr)
                    first_seen_alias = 'first_seen_{}_{}_rows'.format(column, window_width)
                    layer_2.select(first_seen_expr, first_seen_alias)
             """   
                
            for (window_width, window_unit) in aggregation_params.get_row_based_windows(): # aggregation_params.windows.get('row', ['unbounded']): TODO if empty rows windows
                for column in aggregation_params.numerical_columns:
                    if window_unit is None: 
                        min_alias = 'min_{}_history{}'.format(column, population_suffix)
                        max_alias = 'max_{}_history{}'.format(column, population_suffix)
                        sum_alias = 'sum_{}_history{}'.format(column, population_suffix)
                        avg_alias = 'avg_{}_history{}'.format(column, population_suffix)
                        std_dev_alias = 'std_{}_history{}'.format(column, population_suffix) 
                        

                        min_description = 'Min of column {} in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward.'.format(column, population, transform_params.buffer_width)
                        max_description = 'Max of column {} in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward.'.format(column, population, transform_params.buffer_width)
                        sum_description = 'Sum of column {} in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward.'.format(column, population, transform_params.buffer_width)
                        avg_description = 'Average of column {} in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward.'.format(column, population, transform_params.buffer_width)
                        std_dev_description = 'Standard deviation of column {} in all history. Population filter: {}. Temporal window: from the {}rd event from this one backward.'.format(column, population, transform_params.buffer_width)
                        
                        window = Window(
                            partition_by=keys_expr_no_timestamp,
                            order_by=[timestamp_expr],
                            end=transform_params.buffer_width, #transform_params.buffer_width,
                            end_direction='PRECEDING'
                        )
                    else:
                    
                        start_bound = window_width*(window_index+1)-1 if isinstance(window_width, str) else window_width*(window_index+1)+transform_params.buffer_width-1
                        end_bound = window_width*window_index + transform_params.buffer_width
                        

                        min_alias = 'min_{}_last_{}_times{}{}'.format(column, window_width, window_suffix, population_suffix)
                        max_alias = 'max_{}_last_{}_times{}{}'.format(column, window_width, window_suffix, population_suffix)
                        sum_alias = 'sum_{}_last_{}_times{}{}'.format(column, window_width, window_suffix, population_suffix)
                        avg_alias = 'avg_{}_last_{}_times{}{}'.format(column, window_width, window_suffix, population_suffix)
                        std_dev_alias = 'std_{}_last_{}_times{}{}'.format(column, window_width, window_suffix, population_suffix)
                            
                        min_description = 'Min of column {} in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event before this one'.format(column, population, end_bound, start_bound)
                        max_description = 'Max of column {} in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event before this one'.format(column, population, end_bound, start_bound)
                        sum_description = 'Sum of column {} in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event before this one'.format(column, population, end_bound, start_bound)
                        avg_description = 'Average of column {} in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event before this one'.format(column, population, end_bound, start_bound)
                        std_dev_description = 'Standard deviation of column {} in the specified window. Population filter: {}. Temporal window: between the {}rd and {}rd event before this one'.format(column, population, end_bound, start_bound)
                        
                        window = Window(
                            partition_by=keys_expr_no_timestamp,
                            order_by=[timestamp_expr],
                            start=start_bound, 
                            end=end_bound,#transform_params.buffer_width,
                            end_direction='PRECEDING'
                        )
                    

                    aggregation_params.feature_description[min_alias] = min_description
                    aggregation_params.feature_description[max_alias] = max_description
                    aggregation_params.feature_description[sum_alias] = sum_description
                    aggregation_params.feature_description[avg_alias] = avg_description
                    aggregation_params.feature_description[std_dev_alias] = std_dev_description
                    
                    aggregation_params.feature_name_mapping[min_alias] = column
                    aggregation_params.feature_name_mapping[max_alias] = column
                    aggregation_params.feature_name_mapping[sum_alias] = column
                    aggregation_params.feature_name_mapping[avg_alias] = column
                    aggregation_params.feature_name_mapping[std_dev_alias] = column

        
                    if window_width is not None or window_index == 0: # avoid dupplicate all history features
                        layer_2_columns.extend([Column(min_alias), Column(max_alias), Column(sum_alias), Column(avg_alias), Column(std_dev_alias)])
                        layer_2_columns_alias.extend([min_alias, max_alias, sum_alias, avg_alias, std_dev_alias])  
                    
                    min_expr = Column(column).min().over(window)
                    max_expr = Column(column).max().over(window)
                    sum_expr = Column(column).sum().over(window)
                    avg_expr = Column(column).avg().over(window)
                    std_operation = Column(column).std_dev_samp().over(window)

                    layer_2.select(min_expr, min_alias)
                    layer_2.select(max_expr, max_alias)
                    layer_2.select(sum_expr, sum_alias)
                    layer_2.select(avg_expr, avg_alias)
                    layer_2.select(std_operation, std_dev_alias)

            layer_3 = SelectQuery()
            layer_3.select_from(layer_2, alias='layer_2')
            layer_3.select(keys_expr)
            layer_3.select(layer_2_columns, layer_2_columns_alias)
            layer_3.select([Column(c) for c in aggregation_params.categorical_columns])

            """
            for column in aggregation_params.categorical_columns:
                for (window_width, window_unit) in aggregation_params.windows:
                    if window_width is not None:
                        start_bound = transform_params.buffer_width + (window_index+1)*window_width - 1
                        end_bound = transform_params.buffer_width + window_index*window_width
                        window = Window(
                            partition_by=keys_expr_no_timestamp,
                            order_by=[timestamp_expr],
                            start=start_bound,
                            end=end_bound,
                            end_direction='PRECEDING'
                            )
                        num_distinct_alias = 'num_distinct_{}_{}_rows{}{}'.format(column, window_width,window_suffix, population_suffix)

                    else:
                        window = Window(
                            partition_by=keys_expr_no_timestamp,
                            order_by=[timestamp_expr],
                            end=transform_params.buffer_width,
                            end_direction='PRECEDING'
                        )
                        num_distinct_alias = 'num_distinct_{}_all_history{}{}'.format(column,window_suffix, population_suffix)

                    #num_distinct_expr = Column(first_seen_alias).condition(Constant(      1), Constant(0)).sum().over(window) # TODO what?
                    first_seen_alias = 'first_seen_{}_{}_rows'.format(column, window_width)
                    num_distinct_expr = Column(first_seen_alias).is_true().cast(ColumnType.INT).sum().over(window) 
                    layer_3.select(num_distinct_expr, num_distinct_alias)
            """
            queries.append(layer_3)
    return queries


def information_query_with_groupby(dataset, aggregation_params, transform_params, categorical_columns_stats):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]

    for population_idx, population in enumerate(aggregation_params.populations):
        for window_index in xrange(aggregation_params.num_windows_per_type):
            for (window_width, window_unit) in aggregation_params.windows:
                
                population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)
                window_suffix = _get_window_suffix(window_index, aggregation_params.num_windows_per_type)

                layer_1 = SelectQuery()
                if is_hdfs:
                    layer_1.select_from('_'.join(dataset.name.split('.')))
                else:
                    layer_1.select_from(dataset)
                layer_1.select(keys_expr)
                layer_1.select([Column(c) for c in aggregation_params.numerical_columns])
                layer_1.select([Column(c) for c in aggregation_params.categorical_columns])
                prefilter = _prefilter(
                    query=layer_1,
                    aggregation_params=aggregation_params,
                    transform_params=transform_params,
                    population=population,
                    window_width=window_width,
                    window_unit=window_unit,
                    window_index=window_index
                )

                layer_2 = SelectQuery()
                layer_2.select_from(layer_1, alias='layer_1')
                for k in keys_expr:
                    layer_2.select(k)
                    layer_2.group_by(k)
                
                for column in aggregation_params.categorical_columns:
                    if window_unit:
                        num_distinct_alias = 'num_distinct_{}_last_{}_{}{}{}'.format(column, window_width, window_unit, window_suffix, population_suffix)
                        num_distinct_description = 'Number of distinct values of column {} in the specified window. Population filter: {}. Temporal window: {} window from the reference date. Window unit: {} {}.'.format(column, population, window_index, window_width, window_unit)
                        
                    elif window_index == 0:
                        num_distinct_alias = 'num_distinct_{}_all_hist{}'.format(column, population_suffix)
                        num_distinct_description = 'Number of distinct values of column {} in all history. Population filter: {}.'.format(column, population_suffix)
                    else:
                        continue
                        
                    num_distinct_expr = Column(column).count_distinct()
                    aggregation_params.feature_description[num_distinct_alias] = num_distinct_description
                    aggregation_params.feature_name_mapping[num_distinct_alias] = column
                    layer_2.select(num_distinct_expr, num_distinct_alias)

                for column in aggregation_params.numerical_columns:
                    if window_unit:
                        min_alias = 'min_{}_{}_{}{}{}'.format(column, window_width, window_unit, window_suffix, population_suffix)
                        max_alias = 'max_{}_{}_{}{}{}'.format(column, window_width, window_unit, window_suffix, population_suffix)
                        sum_alias = 'sum_{}_{}_{}{}{}'.format(column, window_width, window_unit, window_suffix, population_suffix)
                        avg_alias = 'avg_{}_{}_{}{}{}'.format(column, window_width, window_unit, window_suffix, population_suffix)
                        std_dev_alias = 'std_{}_{}_{}{}{}'.format(column, window_width, window_unit, window_suffix, population_suffix)
                        
                        min_description = 'Min of column {} in the specified window. Population filter: {} window from the reference date. Temporal window: {}. Window unit: {} {}.'.format(column, population, window_index, window_width, window_unit)
                        max_description = 'Max of column {} in the specified window. Population filter: {} window from the reference date. Temporal window: {}. Window unit: {} {}.'.format(column, population, window_index, window_width, window_unit)
                        sum_description = 'Sum of column {} in the specified window. Population filter: {} window from the reference date. Temporal window: {}. Window unit: {} {}.'.format(column, population, window_index, window_width, window_unit)
                        avg_description = 'Average of column {} in the specified window. Population filter: {} window from the reference date. Temporal window: {}. Window unit: {} {}.'.format(column, population, window_index, window_width, window_unit)
                        std_dev_description = 'Standard deviation of column {} in the specified window. Population filter: {} window from the reference date. Temporal window: {}. Window unit: {} {}.'.format(column, population, window_index, window_width, window_unit)
           
                    elif window_index == 0: 
                        min_alias = 'min_{}_all_hist{}'.format(column, population_suffix)
                        max_alias = 'max_{}_all_hist{}'.format(column, population_suffix)
                        sum_alias = 'sum_{}_all_hist{}'.format(column, population_suffix)
                        avg_alias = 'avg_{}_all_hist{}'.format(column, population_suffix)
                        std_dev_alias = 'std_{}_all_hist{}'.format(column, population_suffix)
                        
                        min_description = 'Min of column {} in all history. Population filter: {}.'.format(column, population_suffix)
                        max_description = 'Max of column {} in all history. Population filter: {}.'.format(column, population_suffix)
                        sum_description = 'Sum of column {} in all history. Population filter: {}.'.format(column, population_suffix)
                        avg_description = 'Average of column {} in all history. Population filter: {}.'.format(column, population_suffix)
                        std_dev_description = 'Standard deviation of column {} in all history. Population filter: {}.'.format(column, population_suffix)

                    else: 
                        continue
                      
                    aggregation_params.feature_description[min_alias] = min_description
                    aggregation_params.feature_description[max_alias] = max_description
                    aggregation_params.feature_description[sum_alias] = sum_description
                    aggregation_params.feature_description[avg_alias] = avg_description
                    aggregation_params.feature_description[std_dev_alias] = std_dev_description
                    
                    aggregation_params.feature_name_mapping[min_alias] = column
                    aggregation_params.feature_name_mapping[max_alias] = column
                    aggregation_params.feature_name_mapping[sum_alias] = column
                    aggregation_params.feature_name_mapping[avg_alias] = column
                    aggregation_params.feature_name_mapping[std_dev_alias] = column
                    

                    min_expr = Column(column).min()
                    layer_2.select(min_expr, min_alias)

                    max_expr = Column(column).max()
                    layer_2.select(max_expr, max_alias)

                    sum_expr = Column(column).sum()
                    layer_2.select(sum_expr, sum_alias)

                    avg_expr = Column(column).avg()
                    layer_2.select(avg_expr, avg_alias)

                    std_dev_expr = Column(column).std_dev_samp()
                    layer_2.select(std_dev_expr, std_dev_alias)

                queries.append(layer_2)
    return queries


def distribution_query_with_groupby(dataset, aggregation_params, transform_params, categorical_columns_stats):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]

    for population_idx, population in enumerate(aggregation_params.populations):
        for column_name in aggregation_params.categorical_columns:
            population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

            category_list = list(categorical_columns_stats[column_name]) # list of list
            #all_categories = [x for y in category_list for x in y] # combine all the previous values in the list to create the NOT IN list
            all_categories = tuple([cat for cat in category_list])
            category_list.append(all_categories)
            #category_list = [tuple(cat) for cat in category_list]
            column_expr = Column(column_name)

            smallest_window = _get_smallest_window(aggregation_params)
            widest_window = _get_widest_window(aggregation_params)
            
            for (window_width, window_unit) in [smallest_window, widest_window]:
                layer_1 = SelectQuery()
                if is_hdfs:
                    layer_1.select_from('_'.join(dataset.name.split('.')))
                else:
                    layer_1.select_from(dataset)
                for k in keys_expr:
                    layer_1.select(k)
                    layer_1.group_by(k)
                layer_1.select(column_expr)
                count_expr = Column('*').count()
                count_alias = 'count_instance'
                layer_1.select(count_expr, count_alias)
                layer_1.group_by(column_expr)
                prefilter = _prefilter(
                    query=layer_1,
                    aggregation_params=aggregation_params,
                    transform_params=transform_params,
                    population=population,
                    window_width=window_width,
                    window_unit=window_unit
                )

                layer_2 = SelectQuery()
                layer_2.select_from(layer_1, alias='layer_1')
                layer_2.select(keys_expr)
                layer_2.select(column_expr)
                sum_of_counts = Column(count_alias).sum().over(Window(partition_by=keys_expr))
                percentage_expr = Column(count_alias).div(sum_of_counts).times(100)
                percentage_alias = 'percentage'
                layer_2.select(percentage_expr, percentage_alias)

                layer_3 = SelectQuery()
                layer_3.select_from(layer_2, alias='layer_2')
                for k in keys_expr:
                    layer_3.select(k)
                    layer_3.group_by(k)
                    
                for value_index, distinct_value in enumerate(category_list):
                    in_or_not_in = 'IN'
                    value_name = '{}'.format(distinct_value)
                    if value_index == len(category_list) - 1: #last list
                        in_or_not_in = 'NOT IN'
                        value_name = 'other_values'
                    s = ''.join(value_name.split()) #TODO what?
                    if window_unit:
                        perc_value_alias = 'perc_{}_in_{}_last_{}_{}{}'.format(s, column_name, window_width, window_unit, population_suffix)
                        perc_value_description = 'Percentage of value {} in column {} in the specified window. Population filter: {}. Window unit: {} {}.'.format(s, column_name, population, window_width, window_unit)
                    else:
                        perc_value_alias = 'perc_{}_in_{}_all_hist'.format(s, column_name)
                        perc_value_description = 'Percentage of value {} in column {} in all history. Population: {}.'.format(s, column_name, population)
                    
                    aggregation_params.feature_description[perc_value_alias] = perc_value_description
                    aggregation_params.feature_name_mapping[perc_value_alias] = column_name

                    if in_or_not_in == 'IN':
                        distinct_value = [distinct_value]
                        perc_value = column_expr.in_(List(*distinct_value)).cast(ColumnType.INT).times(Column(percentage_alias)).sum()
                    else:
                        perc_value = column_expr.in_(List(*distinct_value)).not_().cast(ColumnType.INT).times(Column(percentage_alias)).sum()
                    layer_3.select(perc_value, perc_value_alias)
              
                queries.append(layer_3)
                
    return queries


# THIS DOESNT WORK FOR NOW, PERFORMANCE ISSUE
# def distribution_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
# then don't include it in the code to be reviewed :)
# TODO


def monetary_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
    queries = []
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    monetary_column_expr = Column(aggregation_params.monetary_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

        layer_1 = SelectQuery()
        if is_hdfs:
            layer_1.select_from('_'.join(dataset.name.split('.')))
        else:
            layer_1.select_from(dataset)
        layer_1.select(keys_expr)
        layer_1.select(monetary_column_expr)

        amount_with_cent_expr = monetary_column_expr.floor().ne(monetary_column_expr.ceil())
        amount_with_cent_alias = 'amount_with_cent{}'.format(population_suffix)
        layer_1.select(amount_with_cent_expr, amount_with_cent_alias)

        is_round_number_expr = monetary_column_expr.cast('numeric').mod(10).eq(0)
        is_round_number_alias = 'is_round_number{}'.format(population_suffix)
        layer_1.select(is_round_number_expr, is_round_number_alias)

        order_of_magnitude_expr = monetary_column_expr.log().plus(1).floor()
        order_of_magnitude_alias = 'order_of_magnitude{}'.format(population_suffix)
        layer_1.select(order_of_magnitude_expr, order_of_magnitude_alias)

        prefilter = _prefilter(
            query=layer_1,
            aggregation_params=aggregation_params,
            transform_params=transform_params,
            population=population
        )
        layer_1_columns = [Column(amount_with_cent_alias), Column(is_round_number_alias), Column(order_of_magnitude_alias)]

        layer_2 = SelectQuery()
        layer_2.select_from(layer_1, alias='layer_1')
        layer_2.select(keys_expr)
        layer_2.select(layer_1_columns)
        layer_2.select(monetary_column_expr)
        window = Window(
            partition_by=keys_expr_no_timestamp,
            order_by=[Column(aggregation_params.timestamp_column)],
            end=transform_params.buffer_width
        )
        max_price_expr = monetary_column_expr.min().over(window)
        max_price_alias = 'max_price{}'.format(population_suffix)
        layer_2.select(max_price_expr, max_price_alias)
        min_price_expr = monetary_column_expr.max().over(window)
        min_price_alias = 'min_price{}'.format(population_suffix)
        layer_2.select(min_price_expr, min_price_alias)

        layer_3 = SelectQuery()
        layer_3.select_from(layer_2, alias='layer_2')
        layer_3.select(keys_expr)
        layer_3.select(monetary_column_expr)
        layer_3.select(layer_1_columns) # copy feature from layer 1, this is not a typo

        is_max_price_expr = monetary_column_expr.le(Column(min_price_alias))
        is_max_price_alias = 'is_max_price{}'.format(population_suffix)
        layer_3.select(is_max_price_expr, is_max_price_alias)
        is_min_price_expr = monetary_column_expr.ge(Column(max_price_alias))
        is_min_price_alias = 'is_min_price{}'.format(population_suffix)
        layer_3.select(is_min_price_expr, is_min_price_alias)

        queries.append(layer_3)
    return queries


def cross_reference_count_query_with_window(dataset, aggregation_params, transform_params, categorical_columns_stats):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
    queries = []
    keys_expr = [Column(k) for k in aggregation_params.get_effective_keys()]
    keys_without_timestamp_col = [k for k in aggregation_params.keys]
    keys_expr_no_timestamp = [Column(k) for k in aggregation_params.keys]
    timestamp_expr = Column(aggregation_params.timestamp_column)

    for population_idx, population in enumerate(aggregation_params.populations):
        population_suffix = _get_population_suffix(population_idx, aggregation_params.populations)

        layer_1 = SelectQuery()
        layer_1.select(keys_expr)
        if is_hdfs:
            layer_1.select_from('_'.join(dataset.name.split('.')))
        else:
            layer_1.select_from(dataset)
            
        prefilter = _prefilter(
            query=layer_1,
            aggregation_params=aggregation_params,
            transform_params=transform_params,
            population=population,
        )
        for column in aggregation_params.cross_reference_columns:
            concat_key_column = " || ', ' || ".join(keys_without_timestamp_col) # TODO no SQL! and I don't understand what it does anyway
            concat_key_by_col_alias = 'concat_key_by_{}'.format(column)
            layer_1.select(Column(concat_key_column), concat_key_by_col_alias)
            layer_1.select(Column(column))

        layer_2 = SelectQuery()
        layer_2.select_from(layer_1, alias='layer_1')
        layer_2.select(keys_expr)
        for column in aggregation_params.cross_reference_columns:
            concat_key_by_col_alias = 'concat_key_by_{}'.format(column)
            layer_2.select(Column(column))
            layer_2.select(Column(concat_key_by_col_alias))
            window = Window(
                partition_by=[Column(column), Column(concat_key_by_col_alias)],
                order_by=[timestamp_expr],
                end=transform_params.buffer_width
            )
            rank_expr = Expression().rank().over(window)
            rank_alias = 'rank_{}'.format(concat_key_by_col_alias)
            layer_2.select(rank_expr, rank_alias)
            first_seen_alias = 'first_seen_{}'.format(concat_key_by_col_alias)
            first_seen_expr = timestamp_expr.eq(timestamp_expr.min().over(window))
            layer_2.select(first_seen_expr, first_seen_alias)

        layer_3 = SelectQuery()
        layer_3.select_from(layer_2, alias='layer_2')
        layer_3.select(keys_expr)
        for column in aggregation_params.cross_reference_columns:
            concat_key_by_col_alias = 'concat_key_by_{}'.format(column)
            rank_alias = 'rank_{}'.format(concat_key_by_col_alias)
            first_seen_alias = 'first_seen_{}'.format(concat_key_by_col_alias)
            layer_3.select(Column(column))
            layer_3.select(Column(rank_alias))
            layer_3.select(Column(first_seen_alias))
            first_seen_condition_1_expr = Column(first_seen_alias).eq(Constant('true'))
            window = Window(
                partition_by=keys_expr_no_timestamp+[Column(column)],
                order_by=[timestamp_expr],
                end=transform_params.buffer_width
            )
            first_seen_condtion_2_expr = Column(rank_alias).eq(Column(rank_alias).lag(1).over(window)).coalesce('false').eq(Column('false')) # TODO what?

            first_seen_expr = first_seen_condition_1_expr.and_(first_seen_condtion_2_expr)
            layer_3.select(first_seen_expr, first_seen_alias)

        layer_4 = SelectQuery()
        layer_4.select_from(layer_3, alias='layer_3')
        layer_4.select(keys_expr)
        for column in aggregation_params.cross_reference_columns:
            concat_key_by_col_alias = 'concat_key_by_{}'.format(column)
            first_seen_alias = 'first_seen_{}'.format(concat_key_by_col_alias)
            layer_4.select(Column(column))
            layer_4.select(Column(first_seen_alias))
            window = Window(
                partition_by=[Column(column)],
                order_by=[timestamp_expr],
                end=transform_params.buffer_width
            )
            num_distinct_expr = Column(first_seen_alias).condition(Constant(1), Constant(0)).sum().over(window) #TODO what?
            num_distinct_alias = 'num_rows_with_same_value_in_{}{}'.format(column, population_suffix)
            layer_4.select(num_distinct_expr, num_distinct_alias)

        queries.append(layer_4)
    return queries


def _get_population_suffix(idx, populations):
    return '_pop_{}'.format(idx) if len(populations) > 1 else ''


def _get_window_suffix(idx, num_windows_per_type):
    return '_window_{}'.format(idx) if num_windows_per_type > 1 else ''


def _prefilter(query, aggregation_params, transform_params, population=None, window_index=0, window_width=0, window_unit=None):
    typestamp_expr = Column(aggregation_params.timestamp_column)
    ref_date_expr = Constant(transform_params.ref_date).cast(ColumnType.DATE)
    time_filter = None
    
    if aggregation_params.is_rolling_window():
        time_filter = typestamp_expr.le(ref_date_expr) # no buffer days for rolling window (TODO unclear comment)
    elif transform_params.buffer_unit is not None:
        buffer_interval = Interval(transform_params.buffer_width, transform_params.buffer_unit) 
        if window_width > 0 and window_unit is not None:
            base_interval = Interval(window_width, window_unit)
            shift = Constant(window_index).times(base_interval) # TODO does not work for Hive
            upper = ref_date_expr.minus(shift).minus(buffer_interval)
            lower = upper.minus(base_interval)
            time_filter = typestamp_expr.ge(lower).and_(typestamp_expr.le(upper))
        else:
            ref = ref_date_expr.minus(buffer_interval)
            time_filter = typestamp_expr.le(ref)
    
    else:     
        if window_width > 0 and window_unit is not None:
            base_interval = Interval(window_width, window_unit)
            shift = Constant(window_index).times(base_interval)
            upper = ref_date_expr.minus(shift)
            lower = upper.minus(base_interval)
            time_filter = typestamp_expr.ge(lower).and_(typestamp_expr.le(upper))
        else:
            ref = ref_date_expr
            time_filter = typestamp_expr.le(ref)


    if population is not None:
        query.where(population)
    if time_filter is not None:
        query.where(time_filter)

        
def _get_smallest_window(aggregation_params): # or null if there is an unbounded window
    #units = ['day', 'week', 'month', 'row']
    units = ['row', 'month', 'week', 'day'] 
    smallest = (np.inf, 'month')
    for window in aggregation_params.windows:
        if window[0]:
            if (units.index(window[1]) > units.index(smallest[1])) or (units.index(window[1]) == units.index(smallest[1]) and window[0] <= smallest[0]): #NB: 60 days < 1 month here
                smallest = window
    if smallest == (np.inf, 'month'):
        return (1, 'day') 

    return smallest

def _get_widest_window(aggregation_params): # or null if there is an unbounded window
    if aggregation_params.whole_history_window_enabled:
        return None, None
    units = ['day', 'week', 'month', 'row']
    widest = (0, 'day')
    for window in aggregation_params.windows:
        if (units.index(window[1]) > units.index(widest[1])) or (units.index(window[1]) == units.index(widest[1]) and window[0] >= widest[0]): #NB: 60 days < 1 month here
            widest = window
    return widest
