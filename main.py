# -*- coding: utf-8 -*-
"""
Created on Sat Aug 15 23:37:45 2020

@author: Лиза
"""


from google.oauth2 import service_account
import pandas_gbq

import numpy as np
import pandas as pd
import math as mt
import datetime as dt
import lib_main as lm


CREDENTIALS = 

"""[summary]
Funtion for getting fresh data from BigQuery for workload scoring model
[description]
Credentials - google service account object with credentials data for project
SqlQuery - string, sql query for BigQeury database
[example]
Input: Credentials = credentials_object
       SqlQuery = 'select * from dataset_name.table_name'
Output: id	    created_at	        updated_at	        type	  subject   description	                                                                                          status	requester_id	submitter_id   assignee_id	 id_project	 id_invoice	channel	country	 manual_category  auto_category	 subcategory   feedback_score	feedback_comment
	    2520211	2018-02-05 08:59:15	2018-02-23 13:05:39	question	        Credit card payments not working from website.	I have been trying since Thursday 1st of Jan t...	  closed	360790164527	360790164527   20890258907	 21190	     316520736	other	za	     None	          None	         None	       unoffered	    None
        2740781	2018-08-17 01:48:04	2018-09-15 11:00:15	question	        Re: error showed during paid subscription on t...	__________________________________\nType your ... closed	365082895633	951579756	   360133124587	 15174	     367443669	email	za	     None	          None	         None	       offered	        None
"""
def getFreshData(Credentials,ProjectId):
    bigquery_sql = " ".join(["SELECT id, DATE(CAST(created_at AS DATETIME)) AS created, DATE(CAST(updated_at AS DATETIME)) AS updated, status, assignee_id",
                             "FROM `xsolla_summer_school.customer_support`",
                             "WHERE status IN ('closed','solved')",
                             "ORDER BY updated_at"])

    dataframe = pandas_gbq.read_gbq(bigquery_sql,project_id=ProjectId, credentials=Credentials, dialect="standard")

    return dataframe

# подгружаем данные
DataFrame = getFreshData(CREDENTIALS,'findcsystem')

# Функция рассчета скоринга агентов поддержки в разрезе статуса (closed, solved)
Test_result=lm.Test_result(DataFrame)

# Функция рассчета итогового скоринга агентов поддержки 
Score_data_total=lm.Score_data_total(Test_result)

# Записать результатов расчетов в соответствующие таблицы базы данных BigQuery
lm.insertScoreResultData(Test_result,'findcsystem','xsolla_summer_school','score_result_status')
lm.insertScoreResultTotal(Score_data_total,'findcsystem','xsolla_summer_school','score_result_total')