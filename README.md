# Xsolla_week_3

1. Т.к в нашем распоряжении есть расчет скора по нескольким статусам, необходимо объединить 2 показателя в единую метрику оценки. Разработайте функцию объеденения, например рачетом среднего арифметического показателей по статусам. На вход функция должна принимать pandas dataframe с расчетом скора по статуасам, на выходе должен быть pandas dataframe со схемой {"assignee_id":assignne_id,"score_value":score_total}

2. В примере представлен скоринг только по одному агенту поддержки. Необходимо разработать программный код для скоринга по статусам всех имеющихся пользователям, результаты расчета сохранить в один pandas dataframe. Точно так же сформировать итоговый скор по каждому агенту поддержки и сохранить во второй pandas dataframe. В итоге у вас должно получиться 2 pandas dataframe, в одном результаты скоринга агентов поддержки в разрезе статуса (closed, solved), во втором результаты итогово скоринга агентов поддержки

3. Записать результаты расчетов в соответствующие таблицы базы данных BigQuery. В примере представлена функция для записи данных скоринга по статусам, сделайте это функцию универсальной для записи обоих наборов данных в соответствующие таблицы

4. Подготовка продакшен программного кода. Организуйте функции расчета скоринга в отдельный lib_main.py . Ораганизуйте main.py файл, импортируйте в него сожержимое файла lib_main.py, разработайте программный код извлечения сырых данных из базы данныз BigQuery, расчет скоринга по статусам и итогового скоринга, сделайте запись результатов работы алгоритма скоринга в соответствующие таблицы базы данных BigQuery. В итоге при запуске main.py файла должен полностью выполняться весь алгоритм скоринга без дополнительного вмешательства.
