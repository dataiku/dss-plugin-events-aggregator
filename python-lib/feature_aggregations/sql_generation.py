# coding: utf-8
from dataiku.sql import Column, Constant, Expression, Interval, SelectQuery, Window, TimeUnit, JoinTypes, ColumnType, toSQL
from dataiku.core.sql import SQLExecutor2, HiveExecutor


class dialectHandler():
    
    def __init__(self, dataset):
        self.dataset = dataset
        self.is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()
        self.executor = HiveExecutor(dataset=dataset) if self.is_hdfs else SQLExecutor2(dataset=dataset)
        
    def convertToSQL(self, sql_object):
        if self.is_hdfs:
            return toSQL(sql_object, dialect='Hive')
        else:
            return toSQL(sql_object, dataset=self.dataset)
        
    def get_executor(self):
        return self.executor 

    def execute_in_database(self, query, output_dataset=None):
        if self.is_hdfs:
            self.executor.exec_recipe_fragment(query)
        else:
            self.executor.exec_recipe_fragment(output_dataset=output_dataset, query=query)

    
def make_max_time_interval_query(timestamp_column, resolved_ref_date, dataset):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
    max_time_interval = Constant(resolved_ref_date).minus(Column(timestamp_column)).extract(TimeUnit.DAY).max()
    query = SelectQuery()
    query.select(max_time_interval, alias="max_time_interval") #TODO naming
    if is_hdfs:
        query.select_from('_'.join(dataset.name.split('.')))
    else:
        query.select_from(dataset)
    dialect_handler = dialectHandler(dataset)
    return dialect_handler.convertToSQL(query)#toSQL(query, dataset=dataset)


def make_most_recent_timestamp_query(timestamp_column, dataset):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
    query = SelectQuery()
    query.select(Column(timestamp_column).max(), alias='most_recent_timestamp')
    if is_hdfs:
        query.select_from('_'.join(dataset.name.split('.')))
    else:
        query.select_from(dataset)
    dialect_handler = dialectHandler(dataset)
    return dialect_handler.convertToSQL(query)#toSQL(query, dialect='Hive')#dataset=dataset)


def make_distinct_values_query(column, dataset):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  

    column = Column(column)

    layer_1 = SelectQuery()
    layer_1.select(column)
    layer_1.select(Column('*').count(), 'count')
    if is_hdfs:
        layer_1.select_from('_'.join(dataset.name.split('.')))
    else:
        layer_1.select_from(dataset)
    layer_1.group_by(column)

    count = Column('count')
    layer_2 = SelectQuery()
    layer_2.select(column)
    layer_2.select(count.div(count.sum().over(Window())).times(100), 'distribution')
    layer_2.select_from(layer_1, alias='layer_1')

    dialect_handler = dialectHandler(dataset)
    return dialect_handler.convertToSQL(layer_2)#toSQL(query, dialect='Hive')#dataset=dataset)

    #return toSQL(layer_2, dialect='Hive')#dataset=dataset)


def make_full_transform_query(aggregation_queries, dataset, aggregation_params, transform_params, encoding_feature=False):
    
    is_hdfs ='hiveTableName' in dataset.get_config().get('params').keys()  
    inner = SelectQuery()
    if is_hdfs:
        inner.select_from('_'.join(dataset.name.split('.')))
    else:
        inner.select_from(dataset)

    if aggregation_params.is_rolling_window():
        inner.select(Column('*'))
    else:
        inner.distinct() #TODO why?! -> avoid dupplicate 
        for key in aggregation_params.get_effective_keys():
            inner.select(Column(key))
    prefilter = _make_prefilter(aggregation_params, transform_params) 
    inner.where(prefilter)
    
    outer = SelectQuery()
    outer.select_from(inner, alias='inner')
    if aggregation_params.is_rolling_window():
        outer.select(Column('*', 'inner'))
    else:
        for col in aggregation_params.get_effective_keys(): #+ feature_names:
            outer.select(Column(col, 'inner'))
            
    reverse_mapping_dict = {}
            
    for idx, agg_query in enumerate(aggregation_queries):
        agg_query.alias(agg_query.get_alias() or 'cte_'+str(idx)) #TODO remove, make sure they have ids
        outer.with_cte(agg_query)
        join_cond = Expression()
        for key in aggregation_params.get_effective_keys():
            join_cond = join_cond.and_(Column(key, 'inner').eq_null_unsafe(Column(key, agg_query.get_alias())))
        outer.join(agg_query.get_alias(), JoinTypes.LEFT, join_cond)
        
        for idx2, col in enumerate(agg_query.get_columns_alias()):
            if encoding_feature:
                if aggregation_params.feature_name_mapping.get(col):
                    new_alias = '{}_{}_{}'.format(aggregation_params.feature_name_mapping.get(col), idx, idx2)
                    outer.select(Column(col, agg_query.get_alias()), new_alias)
                    reverse_mapping_dict[new_alias] = col
            else:
                outer.select(Column(col, agg_query.get_alias()))
        
    return dialectHandler(dataset).convertToSQL(outer), reverse_mapping_dict
    #return toSQL(outer, dialect='Hive', dataset=dataset), reverse_mapping_dict #dataset=dataset), reverse_mapping_dict

"""
def _make_prefilter(aggregation_params, ref_date):
    timestamp_column_expr = Column(aggregation_params.timestamp_column)
    ref_date_expr = Constant(ref_date).cast(ColumnType.DATE)
    if aggregation_params.is_rolling_window():
        return timestamp_column_expr.le(ref_date_expr)
    else:
        if aggregation_params.windows and not aggregation_params.is_rolling_window():
            # should be the other way: use the smallest unit so that we capture everything
            widest_window = _get_widest_window(aggregation_params)
            if widest_window is not None:
                return timestamp_column_expr.le(ref_date_expr.minus(Interval(widest_window[0], widest_window[1])))
        return timestamp_column_expr.le(ref_date_expr)
"""    

def _make_prefilter(aggregation_params, transform_params):
    timestamp_expr = Column(aggregation_params.timestamp_column)
    ref_date_expr = Constant(transform_params.ref_date).cast(ColumnType.DATE)
    time_filter = None
    
    if aggregation_params.is_rolling_window():
        time_filter = timestamp_expr.le(ref_date_expr)
        
    elif transform_params.buffer_unit is not None:
        buffer_interval = Interval(transform_params.buffer_width, transform_params.buffer_unit) 
        upper = ref_date_expr.minus(buffer_interval)
        
        if aggregation_params.whole_history_window_enabled:
            time_filter = timestamp_expr.le(upper)
        else:
            window_width, window_unit = _get_widest_window(aggregation_params)
            base_interval = Interval(window_width, window_unit)
            lower = upper.minus(base_interval)
            time_filter = timestamp_expr.ge(lower).and_(timestamp_expr.le(upper))        
    else:
        if aggregation_params.whole_history_window_enabled:
            time_filter = timestamp_expr.le(ref_date_expr)
        else:
            window_width, window_unit = _get_widest_window(aggregation_params)
            base_interval = Interval(window_width, window_unit)
            lower = ref_date_expr.minus(base_interval)
            time_filter = timestamp_expr.ge(lower).and_(timestamp_expr.le(ref_date_expr))
    
    return time_filter
    
    
def _get_widest_window(aggregation_params): # or null if there is an unbounded window
    if aggregation_params.whole_history_window_enabled:
        return None, None
    units = ['day', 'week', 'month', 'year', 'row']
    widest = (0, 'day')
    for window in aggregation_params.windows:
        if (units.index(window[1]) > units.index(widest[1])) or (units.index(window[1]) == units.index(widest[1]) and window[0] >= widest[0]): #NB: 60 days < 1 month here
            widest = window

    return widest
