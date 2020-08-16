from google.oauth2 import service_account
import pandas_gbq

import numpy as np
import pandas as pd
import math as mt
import datetime as dt



"""[summary]
Function for scoring workload by statuses (In Progress and Done) for one employee, NumOfAllDays = 63, NumOfIntervalDays = 7
[description]
Data - pandas dataframe object, with hist data for customer support agent
NumOfAllDays - integer, number of days for all hist data
NumOfIntervalDays - integer, number of days for weekly calculating interval
[example]
Input: Data = id	   created	   updated	   status	assignee_id
              2936149  2018-12-31  2019-01-30  closed	26979706288
              2936171  2018-12-31  2019-01-30  closed	26979706288
       NumOfAllDays = 63
       NumOfIntervalDays = 7
Output: assignee_id	 status	 count_last_period	count_mean_calc_period	count_sem_calc_period	score_value
        12604869947	 closed	 196	            196.62	                9.43	                1
        12604869947	 solved	 0	                0.00	                0.00	                0    
"""
def workloadScoringByStatuses(Data,NumOfAllDays,NumOfIntervalDays):
    assignee_id = np.unique(Data.assignee_id)
    assignee_id = assignee_id[0]
    
    #splitting by status
    statuses = np.unique(Data.status)
    assignee_id_list = []
    status_list = []
    avg_num_of_task_per_week_list = []
    ste_list = []
    num_tasks_per_current_week_list = []
    score_for_status_list = []
    for status in statuses:
        dataframe_status = Data[(Data.status == str(status))][:]
    
        #time borders params
        curr_date = dt.datetime.strptime(str('2017-04-01'),'%Y-%m-%d')
        curr_date = curr_date.date()
        delta = dt.timedelta(days=NumOfAllDays)
        first_date = curr_date-delta
    
        #time interval params
        delta_interval = dt.timedelta(days=NumOfIntervalDays)
        first_interval = first_date+delta_interval
            
        num_of_intervals = int(NumOfAllDays/NumOfIntervalDays)
        num_tasks_per_week = []
        for i in range(0,num_of_intervals):
            interval = dataframe_status[(dataframe_status.updated >= str(first_date)) & (dataframe_status.updated <= str(first_interval))][:]
            first_date = first_date + delta_interval
            first_interval = first_interval + delta_interval
    
            if i != (num_of_intervals-1):        
                num_of_tasks = len(np.unique(interval['id']))
                num_tasks_per_week.append(num_of_tasks) #history number of tasks
            else:
                num_tasks_per_current_week = len(np.unique(interval['id'])) #currently number of tasks
        
        avg_num_of_task_per_week = round(np.mean(num_tasks_per_week),2)

        #squared deviations
        x_values = []
        for num in num_tasks_per_week:
            x = round((num - avg_num_of_task_per_week)**2,2)
            x_values.append(x)

        #data sampling statistics
        x_sum = round(sum(x_values),2) #sum of squared deviations
        dispersion = round(x_sum/(num_of_intervals-1),2) #dispersion
        std = round(mt.sqrt(dispersion),2) #standart deviation for sample
        ste = round(std/mt.sqrt(num_of_intervals),2) #standart error for sample

        #confidence interval
        left_border = int(avg_num_of_task_per_week - ste)
        right_border = int(avg_num_of_task_per_week + ste)

        #workload scoring for status
        score_for_status = workloadScoreStatuses(left_border,right_border,num_tasks_per_current_week)        
        assignee_id_list.append(assignee_id)
        status_list.append(status)
        avg_num_of_task_per_week_list.append(avg_num_of_task_per_week)
        ste_list.append(ste)
        num_tasks_per_current_week_list.append(num_tasks_per_current_week)
        score_for_status_list.append(score_for_status)
        
    score_data = {"assignee_id":assignee_id_list,"status":status_list,
                  "count_last_period":num_tasks_per_current_week_list,"count_mean_calc_period":avg_num_of_task_per_week_list,"count_sem_calc_period":ste_list,
                  "score_value":score_for_status_list}
    scores = pd.DataFrame(data=score_data)

    return scores


"""[summary]
Function for scoring workload for current status
[description]
LeftBoard - float, left boarder for confidence interval
RightBoard - float right boarder for confidence interval
CurrentNumOfTasks - integer, number of customer support agent tasks for current interval (7 days)
[example]
Input: LeftBoard = 187
       RightBoard = 206
       CurrentNumOfTasks = 196
Output: 1
"""
def workloadScoreStatuses(LeftBoard,RightBoard,CurrentNumOfTasks):
    if (LeftBoard == 0) & (CurrentNumOfTasks == 0) & (RightBoard == 0):
        score = 0
    elif (CurrentNumOfTasks >= 0) & (CurrentNumOfTasks < LeftBoard):
        score = 0
    elif (CurrentNumOfTasks >= LeftBoard) & (CurrentNumOfTasks <= RightBoard):
        score = 1
    else:
        score = 2
    
    return score


"""[summary]
Function for inserting data to BigQuery database
[description]
InsertDataFrame - pandas dtaframe object, with score result data by statuses
ProjectId - string, name of project in google cloud platform 
DatasetId - string, name of dataset in bigquery for raw data
TableId - string, name of table for raw data
[example]
Input: InsertDataFrame = assignee_id	status	count_last_period	count_mean_calc_period	count_sem_calc_period	score_value
                         11527290367	closed	163	                140.38	                12.4	                2
                         11527290367	solved	0	                0.00	                0.0 	                0
       ProjectId = 'test-gcp-project'
       DatasetId = 'test_dataset'
       TableId = 'test_table'
"""
def insertScoreResultData(InsertDataFrame,ProjectId,DatasetId,TableId):
    destination_table = f"{DatasetId}.{TableId}"
    
    res_df = pd.DataFrame()
    res_df['assignee_id'] = InsertDataFrame['assignee_id'].astype('int64')
    res_df['status'] = InsertDataFrame['status'].astype('str')
    res_df['count_last_period'] = InsertDataFrame['count_last_period'].astype('int')
    res_df['count_mean_calc_period'] = InsertDataFrame['count_mean_calc_period'].astype('float')
    res_df['count_sem_calc_period'] = InsertDataFrame['count_sem_calc_period'].astype('float')
    res_df['score_value'] = InsertDataFrame['score_value'].astype('int')
    res_df['developer'] = 'liza.grebenshchikova'
    res_df['developer'] = res_df['developer'].astype('str')

    pandas_gbq.to_gbq(res_df, destination_table=destination_table, project_id=ProjectId, if_exists='append')
 
    
    
"""[summary]
Function for inserting data to BigQuery database
[description]
InsertDataFrame - pandas dtaframe object, with total score result data 
ProjectId - string, name of project in google cloud platform 
DatasetId - string, name of dataset in bigquery for raw data
TableId - string, name of table for raw data
[example]
Input: InsertDataFrame = assignee_id	score_value
                         123193832         0.0	
                         288517962	       1.0               
       ProjectId = 'test-gcp-project'
       DatasetId = 'test_dataset'
       TableId = 'test_table'
"""

def insertScoreResultTotal(InsertDataFrame,ProjectId,DatasetId,TableId):
    destination_table = f"{DatasetId}.{TableId}"
    
    res_df = pd.DataFrame()
    res_df['assignee_id'] = InsertDataFrame['assignee_id'].astype('int64')
    res_df['score_value'] = InsertDataFrame['score_value'].astype('float')
    res_df['developer'] = 'liza.grebenshchikova'
    res_df['developer'] = res_df['developer'].astype('str')

    pandas_gbq.to_gbq(res_df, destination_table=destination_table, project_id=ProjectId, if_exists='append')


"""[summary]
Function for aggregations scoring workload by statuses (In Progress and Done) for one employee
[description]
Data - pandas dataframe object, with scoring workload by statuses (In Progress and Done) for one employee
[example]              
Input: DataFrame = assignee_id	 status	 count_last_period	count_mean_calc_period	count_sem_calc_period	score_value
                    288517962	 closed	     49	                50.75	                4.18	                1
                    235568	     closed	     0	                0.00	                0.00                 	0
                    291235568	 solved	     0	                0.00	                0.00	                0
Output: assignee_id	 score_value
        123193832         0.0	
        288517962	      1.0    	               
"""    
def agr(x):
    score_data_total=pd.DataFrame()
    score_data_total = {"assignee_id":x.assignee_id, "score_value":x.score_value.mean()}
    score_data_total = pd.DataFrame(data=score_data_total).drop_duplicates()
    score_data_total.reset_index(inplace=True, drop=True)
    return score_data_total



"""[summary]
Function for scoring workload by statuses (In Progress and Done) for ALL employee, NumOfAllDays = 63, NumOfIntervalDays = 7
[description]
Data - pandas dataframe object, with hist data for customer support agents
[example]
Input: Data = id	   created	   updated	   status	assignee_id
              2936149  2018-12-31  2019-01-30  closed	26979706288
              2936171  2018-12-31  2019-01-30  closed	26979706288
Output: assignee_id	 status	 count_last_period	count_mean_calc_period	count_sem_calc_period	score_value
        288517962	 closed	 49	                50.75	                4.18	                1
	    291235568	 closed	 0	                0.00	                0.00                 	0
        291235568	 solved	 0	                0.00	                0.00	                0
"""

def Test_result(DataFrame):
    unique_support_agents=np.unique(DataFrame.assignee_id)
    test_result= pd.DataFrame()
    for item in unique_support_agents:
        test_date=pd.DataFrame(DataFrame[DataFrame.assignee_id == item])[:]
        temp = workloadScoringByStatuses(test_date,63,7)
        test_result=pd.concat([test_result, temp])
        test_result.reset_index(inplace=True, drop=True)
    return test_result


"""[summary]
Function for total scoring workload by statuses (In Progress and Done) for ALL employee
[description]
Data - pandas dataframe object, with scoring workload by statuses (In Progress and Done) for ALL employee
[example]              
Input: DataFrame = assignee_id	 status	 count_last_period	count_mean_calc_period	count_sem_calc_period	score_value
                    288517962	 closed	 49	                50.75	                4.18	                1
                    291235568	 closed	 0	                0.00	                0.00                 	0
                    291235568	 solved	 0	                0.00	                0.00	                0
Output: assignee_id	 score_value
        123193832         0.0	
        288517962	      1.0    	               
"""

def Score_data_total(DataFrame):
    unique_support_agents=np.unique(DataFrame.assignee_id)
    score_data_total = pd.DataFrame()
    score_data=pd.DataFrame()
    for item in unique_support_agents:
        score_date=pd.DataFrame(DataFrame[DataFrame.assignee_id == item])[:]
        temp = agr(score_date)
        score_data_total=pd.concat([score_data_total, temp])
        score_data_total.reset_index(inplace=True, drop=True)
    return score_data_total
    
    
    

    
    
    
    