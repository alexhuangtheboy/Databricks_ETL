from sklearn.feature_selection import RFECV
from sklearn.ensemble import RandomForestRegressor
import pandas as pd
import numpy as np
import pulp
import matplotlib.pyplot as plt
import xgboost as xgb
import re
import math
import smtplib
import datetime
import warnings
import ruptures as rpt
import mlflow
mlflow.autolog(disable=True)
warnings.filterwarnings("ignore")
from scipy.stats import norm
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_percentage_error
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
from prophet import Prophet
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import make_scorer
from statsmodels.tsa.arima.model import ARIMA
from pmdarima import auto_arima
!pip install tabulate
from tabulate import tabulate

year_now = str(datetime.datetime.now().year)
this_year = f'{year_now}'+'-01-01'
exclusive_prod = (60011371, 60011466, 60012104, 60011468, 60011470, 60012104, 60011530, 60011469, 60011467, 60012096, 60011467, 60011505)
wing_gids = (20637, 20643, 20653, 20657, 20663, 20689, 20702, 20716, 20723, 20728, 20729, 20742, 20771, 20762, 20764)

today = datetime.datetime.today()
target_date = today + relativedelta(months=-3)
target_year = target_date.year
target_month = target_date.month

if target_year!=year_now and target_month==12:
  target_year+=1
  target_month=1



# Data Loading

model_data = spark.sql(f'''
                        select *
                        from markdb.model_data
                        ''').toPandas()

model_data['ds'] = model_data.apply(lambda x: str(x['year']) + '-' + str(x['month']) + '-' + str(x['day']), axis=1)
model_data['ds'] = pd.to_datetime(model_data['ds'], format='%Y-%m-%d')
top_countries = tuple(set(model_data['country_code']))

# Prediction

class Prediction:
  """模型预测模块"""
  def __init__(self, country_code, model_data, k_folds=10, test_size=30, pred_periods=30, param_grid_rf=dict, param_grid_xgb=dict):
    # 初始化参数
    self.country_code = country_code
    self.model_data = model_data
    self.test_size = test_size
    self.pred_periods = pred_periods
    self.kfold = KFold(n_splits=k_folds, shuffle=True)
    self.scorer = make_scorer(mean_absolute_percentage_error, greater_is_better=False)
    self.param_grid_rf = param_grid_rf
    self.param_grid_xgb = param_grid_xgb
    # 初始化模型
    self.predictor_rf = RandomForestRegressor()
    self.predictor_xgb = xgb.XGBRFRegressor()

  def X_pred_processor(self, X_pred, future=True):
    df = X_pred
    last_row = df.iloc[-1]
    first_future_date = datetime.date(int(last_row['year']), int(last_row['month']), int(last_row['day']))
    if future:
      future_dates = [first_future_date + datetime.timedelta(days=x) for x in range(1, self.pred_periods+1)]
    else:
      future_dates = [first_future_date + datetime.timedelta(days=x) for x in range(1, self.test_size+1)]

    year, month, day = [x.year for x in future_dates], [x.month for x in future_dates], [x.day for x in future_dates]
    res = pd.DataFrame(data={'year': year, 'month': month, 'day': day})
    for col in df.columns:
      if col not in ['year', 'month', 'day']:
        hist_values = df[col].values
        mean, std = hist_values.mean(), hist_values.std()
        values = np.random.normal(mean, std, size=len(future_dates))
        res[col] = values

    return res[X_pred.columns]
  

  def feature_selection(self, data, label_name):

    m = Prophet()
    df = data[['ds', label_name]]
    df['y'] = df[label_name]
    df = df[['ds', 'y']].reset_index(drop=True)
    m.fit(df)
    future = m.make_future_dataframe(periods=1, freq='D')
    forecast = m.predict(future)

    X_raw = data.merge(forecast[['ds', 'trend', 'additive_terms', 'weekly', 'yearly', 'multiplicative_terms']], how='left')
    X = X_raw.drop(['country_code', 'year', 'month', 'day','usage', 'peak', 'ds'], axis=1)
    y = data[label_name]

    # 创建选择器
    rfecv = RFECV(estimator=RandomForestRegressor(),
                  step=1,           # 每次移除1个特征
                  cv=5,             # 5折交叉验证
                  scoring='neg_mean_absolute_error')

    # 执行特征选择
    rfecv.fit(np.array(X), np.array(y))
    features = list(X.columns[rfecv.support_]) + ['year', 'month', 'day', label_name]

    return features, X_raw[features]


  def pred_random_forest(self, X_pred, X_train, y_train, X_test, y_test, verbose=True):

    # CV网格训练随机森林
    grid_search_rf = GridSearchCV(self.predictor_rf, self.param_grid_rf, cv=self.kfold, scoring=self.scorer)
    grid_search_rf.fit(X_train, y_train)
    best_model_rf = grid_search_rf.best_estimator_
    # 获取训练中的指标得分
    scores_rf = grid_search_rf.cv_results_['mean_test_score']
    rf_score_avg = round(-np.mean(scores_rf), 4)
    rf_score_std = round(np.std(scores_rf), 4)
    # 用当前数据的预测与下一个周期的实际值做比较
    rf_test_score = round(mean_absolute_percentage_error(y_test.iloc[1:], best_model_rf.predict(X_test)[:-1]), 4)
    if verbose:
      print(f'Average MAPE for RF model in training: {rf_score_avg} and the std is {rf_score_std}, the CV is {round(rf_score_std/rf_score_avg, 4)}')
      print("================================================================================================")
      print(f'MAPE score on test set with shifted time window for RF is: {rf_test_score}')
      print("================================================================================================")

    # 获取待预测的数据片段
    future_pred = self.X_pred_processor(X_pred)
    X_test_pred = self.X_pred_processor(X_train.tail(30), future=False)
    preds_for_plot_rf = np.concatenate([best_model_rf.predict(X_test_pred), best_model_rf.predict(future_pred)])

    return preds_for_plot_rf.round()

  def pred_xgboost(self, X_pred, X_train, y_train, X_test, y_test, verbose=True):
    # xgb回归模型
    grid_search_xgb = GridSearchCV(self.predictor_xgb, self.param_grid_xgb, cv=self.kfold, scoring=self.scorer)
    grid_search_xgb.fit(X_train, y_train)
    best_model_xgb = grid_search_xgb.best_estimator_
    scores_xgb = grid_search_xgb.cv_results_['mean_test_score']
    xgb_score_avg = round(-np.mean(scores_xgb), 4)
    xgb_score_std = round(np.std(scores_xgb), 4)
    xgb_test_score = round(mean_absolute_percentage_error(y_test.iloc[1:], best_model_xgb.predict(X_test)[:-1]), 4)
    if verbose:
      print(f'Average MAPE for XGB model is: {xgb_score_avg} and the std is {xgb_score_std}, the CV is {round(xgb_score_std/xgb_score_avg, 4)}')
      print("================================================================================================")
      print(f'MAPE score on test set with shifted time window for Xgb is: {xgb_test_score}' )
      print("================================================================================================")
      
    future_pred = self.X_pred_processor(X_pred)
    X_test_pred = self.X_pred_processor(X_train.tail(30), future=False)
    preds_for_plot_xgb = np.concatenate([best_model_xgb.predict(X_test_pred), best_model_xgb.predict(future_pred)])

    return preds_for_plot_xgb.round()
  

  def pred_time_series(self, data, label_name, X_test, verbose=True):

    df = data[['ds', label_name]].iloc[:-(self.test_size)]
    df.columns = ['ds', 'y']
    training_data_ts = df.set_index('ds')['y']
    # 自动搜索最佳的 (p, d, q) 和 (P, D, Q) 参数
    model = auto_arima(training_data_ts, 
                      start_p=1, start_q=1, 
                      max_p=3, max_q=3, 
                      m=12, 
                      seasonal=True,  # 启用季节性
                      d=1, D=1,  # 可以自动选择差分次数
                      trace=True,
                      error_action='ignore',  
                      suppress_warnings=True,
                      stepwise=True)
    # 拟合模型
    model_fit = model.fit(training_data_ts)
    # 预测未来周期
    forecast = model_fit.predict(n_periods=self.test_size+self.pred_periods)

    trend_piece = pd.DataFrame(forecast).reset_index(drop=True)
    trend_piece.columns = ['yhat']
    preds_for_plot_ts = trend_piece['yhat'].round()

    data_for_plot = pd.concat([X_test])
    data_for_plot['date'] = data_for_plot.apply(lambda x: str(int(x['year'])) + '-' + str(int(x['month'])) + '-' + str(int(x['day'])), axis=1)
    data_for_plot['date'] = pd.to_datetime(data_for_plot['date'])
    data_for_plot = data_for_plot.sort_values('date')
    
    x_for_plot = pd.to_datetime(data_for_plot['date'].values)
    y_for_plot = data_for_plot[['year', 'month', 'day']].merge(data[['year', 'month', 'day', label_name]], how='left', 
                                     on=['year', 'month', 'day'])[label_name].values

    # trend_coef = trends / trends[0]
    # ts_test_score = mean_absolute_percentage_error(y_for_plot, preds_for_plot_ts)
    return x_for_plot, y_for_plot, preds_for_plot_ts

  def bayesian_ensembler(self, label_name, verbose=False):
    # 获取训练数据
    data_raw = self.model_data[self.model_data['country_code']==self.country_code].sort_values(['year', 'month', 'day']).reset_index(drop=True)
    data_raw[f'{label_name}_prev'] = data_raw[label_name].shift(1)
    data = data_raw.dropna()
    # 选择最佳的特征
    features_name, data = self.feature_selection(data, label_name)
    # features_name = [x for x in data.columns if x not in self.drop_features and x != label_name]
    X_train, y_train = data.iloc[:-(self.test_size)][features_name], data.iloc[:-(self.test_size)][label_name]
    X_test, y_test = data.iloc[-self.test_size:][features_name], data.iloc[-self.test_size:][label_name]

    X_pred = data.iloc[-self.pred_periods:][features_name]
    ts_data = data[['year', 'month', 'day', label_name]]

    # 获取模型预测值
    rf_preds = self.pred_random_forest(X_pred, X_train, y_train, X_test, y_test, verbose=verbose)
    xgb_preds = self.pred_xgboost(X_pred, X_train, y_train, X_test, y_test, verbose=verbose)
    # nn_preds = self.pred_nn(verbose=verbose)
    x, y, ts_preds = self.pred_time_series(data_raw, label_name, X_test, verbose=verbose)
    # 初始化算法参数
    preds_matrix = np.vstack([rf_preds, xgb_preds, ts_preds]) 
    num_models = len(preds_matrix) 
    num_samples = preds_matrix.shape[1]
    combined_predictions_initial = np.zeros(num_samples)
    def get_combined_predictions(combined_predictions_initial, prior):
      # 计算每个模型的后验概率
      posteriors = []
      for i in range(num_models):
          mean = np.mean(preds_matrix[i])
          std = np.std(preds_matrix[i])
          posterior = norm.pdf(preds_matrix[i], loc=mean, scale=std) * prior[i]
          posteriors.append(posterior)
      # 归一化后验概率
      posteriors_sum = np.sum(posteriors, axis=0)
      posteriors_sum[posteriors_sum == 0] = 1  # 处理权重和为0的情况
      posteriors = posteriors / posteriors_sum
      # 根据后验概率对预测结果进行加权组合
      for i in range(num_samples):
          combined_predictions_initial[i] = np.average(preds_matrix[:, i], weights=posteriors[:, i])
      return combined_predictions_initial

    combined_predictions_without_ts = get_combined_predictions(combined_predictions_initial, np.array([0.5, 0.5, 0.]))
    combined_predictions_initial = np.zeros(num_samples)
    combined_predictions_ts = get_combined_predictions(combined_predictions_initial, np.array([0.1, 0.1, 0.8]))

    ground_true = y.sum()
    total_preds_without_ts = combined_predictions_without_ts[:len(y)].sum()
    total_preds_ts = combined_predictions_ts[:len(y)].sum()
    if abs(ground_true-total_preds_without_ts)/total_preds_without_ts <= abs(ground_true-total_preds_ts)/total_preds_ts or label_name=='peak':
      combined_predictions = combined_predictions_without_ts
    else:
      combined_predictions = combined_predictions_ts
  
    return x, y, combined_predictions, data
    
  def visualization(self, label_name='usage', verbose=False):
    
    x, y, combined_predictions, data = self.bayesian_ensembler(label_name, verbose=verbose)
    ground_true = y.sum()
    total_preds = combined_predictions[:len(y)].sum()
    print(f'MAPE: {abs(ground_true-total_preds)/total_preds*100}%')
    print(f"The predicted {label_name} for the next {self.pred_periods} days is: {round(combined_predictions[-self.pred_periods:].max())}")
    print("======================")
    last_day = x[-1]
    date_list = [last_day + datetime.timedelta(days=x) for x in range(1, self.pred_periods+1)]
    x = pd.to_datetime(list(x) + date_list)
    # print(df['combined_predictions'])
    y = np.concatenate([y, np.array([None]*self.pred_periods)])
    df = pd.DataFrame(data={'x': x, 'y': y, 'combined_predictions': combined_predictions})
    df['MPE%'] = df.apply(lambda x: (x['combined_predictions'] - x['y'])/x['y'] if x['y'] else None, axis=1)
    df = df.sort_values('x')
    # spark_df = spark.createDataFrame(df)
    # spark_df.write.mode("append").saveAsTable("markdb.pred_chart_table")

    df['up_20_pct'] = df['y'] * 1.2
    df['up_10_pct'] = df['y'] * 1.1
    df['down_10_pct'] = df['y'] * 0.9
    df[['up_20_pct', 'up_10_pct', 'down_10_pct']] = df[['up_20_pct', 'up_10_pct', 'down_10_pct']].fillna(value=np.nan)

    # 设置画布参数
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 9))
    plt.style.use('ggplot')
    # 曲线图
    ax1.set_title(f'Predictions versus Actual {label_name} in {self.country_code}')
    ax1.set_ylabel(f'Data(GB)/Peak')
    ax1.plot(df['x'], df['y'], linewidth=3, color='#5e793b')
    ax1.plot(df['x'], df['combined_predictions'], linewidth=3, color='darkred')
    
    # 填充置信区间
    ax1.fill_between(df['x'], df['down_10_pct'], df['up_20_pct'], color='steelblue', alpha=0.5)
    ax1.fill_between(df['x'], df['down_10_pct'], df['up_10_pct'], color='darkred', alpha=0.3)
    ax1.legend(['Actual Value', 'Prediction'], loc=2); # 'Neural Network'
    # 真实流量用量图
    ax2.set_title(f'Historical Value Pattern of {label_name}')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Data(GB)/Peak')

    data['date'] = data.apply(lambda x: str(int(x['year'])) + '-' + str(int(x['month'])) + '-' + str(int(x['day'])), axis=1)
    data['date'] = pd.to_datetime(data['date'])
    x_date = data['date']
    ax2.plot(x_date, data[label_name], linewidth=3, color='#5e793b')
    x_value = data.iloc[-(self.test_size)]['date']
    ax2.axvline(x=x_value, linewidth=3, color='black', alpha=0.3, linestyle='--');


param_grid_rf = {'n_estimators': [10, 50, 100], 'max_depth': [None, 3, 5, 10]}
param_grid_xgb = {'n_estimators': [10, 50, 100], 'max_depth': [None, 3, 5, 10]}
test_size =30
if today.month in (1, 3, 5, 7, 8, 10, 12):
  pred_periods = 31
else:
  pred_periods = 30

# Make prediction and visualize the result on one country

country_code = 'PT'
predictions = Prediction(country_code, model_data, test_size=test_size, pred_periods=60,
                         param_grid_rf = param_grid_rf, 
                         param_grid_xgb = param_grid_xgb)

total_volume = predictions.bayesian_ensembler('usage')[2][-30:].sum()
predictions.visualization(label_name='peak', verbose=False)

class DecisionMaker:

    def __init__(self, country_code, pred_usage, pred_peak, inflate_factor):
        
        self.country_code = country_code
        self.pred_usage, self.pred_peak, self.inflate_factor = pred_usage, pred_peak, inflate_factor
        self.sim_resources = spark.sql(f'''select * from markdb.sim_resources where lower(product_name) not like "%test%" ''').toPandas()

    def decision_maker(self):
        
        country_sim = self.sim_resources[self.sim_resources['country_code']==self.country_code]
        country_sim['price_usd'] = country_sim['package_size'] * country_sim['unit_cost_usd']
        local_products = country_sim[country_sim['is_local']==1]['product_id'].tolist()
        print(f"Curren Sim Resources in {self.country_code}: \n\n{tabulate(country_sim[['product_id', 'product_name', 'is_local', 'price_usd', 'unit_cost_usd','supplier', 'imsi_num']], headers='keys', tablefmt='grid', showindex=False)}")
        inputs = []
        total_cost, total_data_nominal, total_data_adj = 0, 0, 0
        for i in range(len(local_products)):
            value = input(f"请输入{local_products[i]} 需要采购的卡数: ")
            inputs.append(value)
            piece = country_sim[(country_sim['product_id']==local_products[i])]
            total_cost += int(value) * piece['package_size'].values[0]* piece['unit_cost_usd'].values[0]
            total_data_nominal += int(value) * piece['package_size'].values[0]
            total_data_adj  += int(value) * min(60, piece['package_size'].values[0])

        print(f"Total Cost in USD: {total_cost} USD")
        print(f"Total Nominal Data Inventory in GB: {total_data_nominal}")
        print(f"Total Adjusted Data Inventory in GB: {total_data_adj}")
        print(f"Predicted Usage Data in GB: {self.pred_usage}")

        if total_data_nominal > self.pred_usage * (self.inflate_factor+1):
            print(f"Total inventory is enough for {self.inflate_factor*100}% more than the expected usage.")
        else:
            print(f"Total inventory is NOT enough for {round(self.inflate_factor, 2)*100}% more than the expected usage.")
            best_product = input(f"请输入最佳备卡的卡产品ID: ")
            local_sim_plan = country_sim[(country_sim['pooling']==0)&(country_sim['is_local']==1)]
            inflation_cost_local = (self.inflate_factor) * self.pred_peak * local_sim_plan[local_sim_plan['product_id']==int(best_product)]['price_usd'].values[0]
            backup_prods = country_sim[(country_sim['pooling']==1)|(country_sim['is_local']==0)]
            total_cards = backup_prods['imsi_num'].sum()
            backup_prods['weight'] = backup_prods['imsi_num'] / total_cards
            inflation_cost_backup_unit = (backup_prods['weight'] * backup_prods['unit_cost_usd']).sum()
            inflation_cost_backup_total = inflation_cost_backup_unit * self.pred_usage * self.inflate_factor
            if len(backup_prods) == 0:
                print("Be careful, No backup products available.")
            else:
                if inflation_cost_backup_total < inflation_cost_local:
                    print("备卡成本太高，不建议备卡")
                else:
                    print("结合未来几个月的走势再判断是否加本地卡")
        
        utilization_pred = round(self.pred_usage / total_data_adj, 3) * 100
        print(f"The projected utilization is: {utilization_pred}%")

    def redo(self):

        self.decision_maker()

