# -*- coding:utf-8 -*-

#功能：1.读入json文件到df—合并df
#     2.数据清洗和预处理
#     3.增加companyid和cityid
#     3-1.对岗位进行分类（高级、普通、实习助理），增加position列
#     4.返回一个新的df 
#缺陷：对salary和date的处理部分其实用map函数很方便，下次吧

from threading import Thread
import re

import pandas as pd

def read_json(filename):#读取json文件到dataframe
	df = pd.read_json(filename)
	return df

def pd_concat(df_list):#合并df，按照统一顺序重新设置index
	df = pd.concat(df_list, ignore_index = True)
	return df

def get_df(file_list):#依次读取各json到df，然后返回合并后的df
	df_list = []
	for file in file_list:
		df_list.append(pd.read_json(file))
	df = pd_concat(df_list)
	return df

def cre_dict(series):#对city、company这种存在重复数据的列进行去重、赋id值，生成dict
	sets = set(series)#去重
	sets_num = list(range(1, len(sets)+1))#生成与sets同样长度的数值序列
	dic = dict(zip(sets, sets_num))
	#print(len(dic))
	return dic

def cre_id(df, col):#对df中指定的列赋予id，这里主要用于cityid和companyid,因为两者重复数据较多
	name_series = df[col]
	id_series = []
	dic = cre_dict(name_series)
	for name in name_series:
		id_series.append(dic[name])
	col = col[:4] + '_id'
	df[col] = id_series
	return df

def clean_date(df, col):#对发布日期列进行清洗，只保留到日期
	date_series = df[col].copy()
	for i in range(len(date_series)):
		date_series[i] = date_series[i].split(' ')[0]
		date_series[i] = date_series[i].replace(r'_', r'/')#统一日期格式
	df[col] = date_series
	return df

def clean_salary(df, col):
#取出工资区间的最小值和最大值，分两列增加到dataframe中
#对工资数值进行转换，去除K这个单位
	salary_series = df[col].copy()
	salary_series_max = []
	salary_series_min = []
	pattern = re.compile(r'\d+k', re.I)#建立匹配规则，大小写不敏感
	for i in range(len(salary_series)):
		tmp_li = pattern.findall(salary_series[i])#该列表用于保存薪资区间最大最小值，也有可能只有一个值
		if len(tmp_li) > 1:
			salary_series_min.append(tmp_li[0])
			salary_series_max.append(tmp_li[1])
		else:
			salary_series_min.append(tmp_li[0])
			salary_series_max.append(tmp_li[0])
	#print(len(salary_series), len(salary_series_min), len(salary_series_min))
	for i in range(len(salary_series_min)):#将k和K用000替换，目的是便于检索
		salary_series_min[i] = re.sub(r'k', '000', salary_series_min[i], flags = re.I)
		salary_series_max[i] = re.sub(r'k', '000', salary_series_max[i], flags = re.I)
	df['salary_max'] = salary_series_max#在pandas中增加两列，分别记录薪资最大值和最小值
	df['salary_min'] = salary_series_min
	return df

def clean_position(df, col):
	#把岗位分为高级、普通、初级三类，其中lead、高级、架构、资深属于高级岗位，初级、助理、实习属于初级岗位，其他属于普通岗位
	def get_class(pos):
		senior = ['Lead', 'lead', '高级', '资深', '架构', '专家']
		junior = ['实习', '初级', '助理']
		for i in senior:
			tmp = re.search(i, pos)
			if tmp:
				return '高级'
		for i in junior:
			tmp = re.search(i, pos)
			if tmp:
				return '初级'
		return '普通'

	pos_class = list(df[col].copy())
	#print(type(pos_class))
	pos_class = list(map(get_class, pos_class))
	#print(pos_class)
	df['pos_class'] = pos_class
	return df


def add_company_id(df, col):
	df = cre_id(df, col)
	return df

def add_city_id(df, col):
	df = cre_id(df, col)
	return df


def data_clean(file_list):
	df = get_df(file_list)
	df = clean_date(df, 'create_time')
	df = clean_salary(df, 'salary')
	df = add_company_id(df, 'company_full_names')
	df = add_city_id(df, 'city')
	df = clean_position(df, 'positionName')
	#print(df.head())
	#df.to_csv('cleaned.csv', encoding = 'utf_8_sig')#encoding参数解决windows下直接打开csv文件乱码的问题
	df.to_csv('cleaned_new.csv', encoding = 'utf_8_sig')
	return df

if __name__ == '__main__':
	#python_file = r'D:\workspace\python训练营\2018第三期数据可视化\job_全国_Python_2018_05_06.json'
	python_file = r'E:\A_Workspace\Python\数据可视化\job_全国_Python_2018_05_06.json'
	file_li = [python_file]
	data_clean(file_li)