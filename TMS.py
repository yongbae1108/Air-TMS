#-*- coding: utf8 -*-

import csv
import os
import pandas as pd
import datetime

def init_codename(yr):
    global codename
    with open('D:/OneDrive - inu.ac.kr/대기연구실/201905 미세먼지 재난사태 선포/논문 그림 데이터/2015 pm25/{} 대기오염측정망 측정소.csv'.format(yr), 'r', encoding='utf-8') as f:
        csv_reader = csv.reader(f, delimiter=',')
        next(csv_reader)
        codename = {}
        for line in csv_reader:
            codename[line[0]] = line[1]


def make_annual_file_meteo(yr):
    global codename
    file_exists = set()
    os.mkdir("C:/Users/Airgroup-3/.vscode/extensions/vs_py_TMS/{yr}_meteo/".format(yr=yr)) 
    fn = 'C:/Users/Airgroup-3/.vscode/extensions/vs_py_TMS/{yr}_meteo/{location}_{yr}_meteo.csv'

    f = 'C:/Users/Airgroup-3/.vscode/extensions/vs_py_TMS/{yr}년.txt'
    with open(f.format(yr=yr), 'r', encoding='cp949') as f1:

        stripped = (line.strip() for line in f1)
        lines = (line.split(",") for line in stripped if line)
        next(lines)
        for line2 in lines:
            loc = line2[4]

            if line2[4] in codename :
                with open(fn.format(yr=yr, location=loc), 'a', encoding='cp949', newline='') as f2:
                    csv_writer = csv.writer(f2, delimiter=',')
                    if line2[4] not in file_exists:
                        file_exists.add(line2[4])
                        csv_writer.writerow(['시도','도시','시군구','측정소명','TMSID','YYYYMMDDHH','WD','WS','TMP','HUM(%)'])
                    csv_writer.writerow(line2)

def make_annual_file(yr):
    global codename
    os.mkdir("D:/OneDrive - inu.ac.kr/대기연구실/201905 미세먼지 재난사태 선포/논문 그림 데이터/2015 pm25/2019/{yr}_TMS/".format(yr=yr))
    fn = 'D:/OneDrive - inu.ac.kr/대기연구실/201905 미세먼지 재난사태 선포/논문 그림 데이터/2015 pm25/2019/{yr}_TMS/{location}_{yr}_TMS.csv'
    file_exists = set()

    for i in range(1,13):
        f = 'D:/OneDrive - inu.ac.kr/대기연구실/201905 미세먼지 재난사태 선포/논문 그림 데이터/2015 pm25/2019/{}년 {}월.csv'.format(yr, i)
        with open(f, 'r', encoding='utf-8') as f1:
            csv_reader = csv.reader(f1, delimiter=',')
            next(csv_reader)
            for line in csv_reader:

                try:
                    loc = codename[line[0]]
                except:
                    loc = line[1]

                with open(fn.format(yr=yr,location=loc), 'a', encoding='cp949', newline='') as f2:
                    csv_writer = csv.writer(f2, delimiter=',')
                    if line[1] not in file_exists:
                        file_exists.add(line[1])
                        csv_writer.writerow(['지역','측정소코드','측정소명', '측정일시','SO2','CO','O3','NO2','PM10','PM25', '주소'])

                    csv_writer.writerow(line)

def data_mul(yr):
    global codename

    site_name = []
    os.mkdir("C:/Users/Airgroup-3/.vscode/extensions/vs_py_TMS/{yr}/".format(yr=yr))
    path_dir = 'C:/Users/Airgroup-3/.vscode/extensions/vs_py_TMS/{yr}_TMS/'
    file_list = os.listdir(path_dir.format(yr=yr))
    
    
    fn = 'C:/Users/Airgroup-3/.vscode/extensions/vs_py_TMS/{yr}_TMS/{location}_{yr}_TMS.csv'
    vs_site_name = codename.keys()
    
    for site_name in vs_site_name:
        
        try:
            df_tms = pd.read_csv(fn.format(yr=yr, location=site_name), encoding='cp949')
            
        except FileNotFoundError:
            print("Skipping {}".format("{location}_{yr}".format(yr=yr, location=site_name)))
            continue
        
        df_tms_part = pd.DataFrame(df_tms[['측정소코드', '측정소명', '측정일시', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'PM25']])
        vs_file = '{location}_{yr}_TMS.csv'.format(yr=yr, location=site_name)
        site_name_re = 'C:/Users/Airgroup-3/.vscode/extensions/vs_py_TMS/{yr}_meteo/{location}_{yr}_meteo.csv'.format(yr=yr, location=site_name)
        

        if vs_file in file_list:
            
            try:
                df_meteo = pd.read_csv(site_name_re, encoding='cp949')
            except FileNotFoundError:
                print("Skipping {}".format("{location}_{yr}".format(yr=yr, location=site_name)))
                continue

            df_meteo_part = pd.DataFrame(df_meteo[['YYYYMMDDHH','WD','WS','TMP','HUM(%)']])  # 기상자료 중 일시도 추가하여 통합
            df_meteo_part = df_meteo_part.rename(columns={'YYYYMMDDHH':'측정일시'}) # TMS와 meteo의 일시 칼럼명 일치하도록 변경
            df_total = pd.merge(df_tms_part, df_meteo_part, on="측정일시", how="outer") # 측정일시를 기준으로 합치기
            df_total.to_csv('C:/Users/Airgroup-3/.vscode/extensions/vs_py_TMS/{yr}/{location}_{yr}.csv'.format(yr=yr, location=site_name), encoding='cp949', index = False)
            
def pre_date(yr):
    os.mkdir("C:/Users/Airgroup-2/Desktop/{yr}_TMS/{yr}_final_2".format(yr=yr)) 
    path_dir = 'C:/Users/Airgroup-2/Desktop/{yr}_TMS'.format(yr=yr)
    file_list = os.listdir(path_dir)
    next_yr = yr + 1
    for i in file_list:
        
        TMS = pd.read_csv('C:/Users/Airgroup-2/Desktop/{yr}_TMS/{i}'.format(yr=yr,i=i), encoding='cp949')
        time = []

        for a in TMS.측정일시:
            
            a=str(a)
            y = a[8:]
            
            if y == '24':
                a = str(a[:8]) + '00'
                a = str(a[:4]) + '-' + str(a[4:6]) + '-' + str(a[6:8]) + '-' + str(a[8:])
                a = datetime.datetime.strptime(a, '%Y-%m-%d-%H')
                a = a + datetime.timedelta(days=1)
                time.append(a)

            else:
                a = str(a[:4]) + '-' + str(a[4:6]) + '-' + str(a[6:8]) + '-' + str(a[8:])
                a = datetime.datetime.strptime(a, '%Y-%m-%d-%H')
                time.append(a)


        time = pd.DataFrame(time, columns=['측정일시'])

        TMS.측정일시 = time.측정일시

        total_time = pd.date_range('{yr}-01-01'.format(yr=yr), '{next_yr}-01-01'.format(next_yr=next_yr), freq='H')
        total_time = pd.DataFrame(total_time, columns=['측정일시'])
        total_time = total_time.drop(0)
        
        code = TMS['측정소코드'].unique()
        name = TMS['측정소명'].unique()

        re_TMS = pd.merge(TMS, total_time, on="측정일시", how="outer")

        re_TMS.sort_values(by = '측정일시', inplace=True)

        re_TMS["측정소코드"] = code[0]
        re_TMS["측정소명"] = name[0]
        
        re_TMS.to_csv("C:/Users/Airgroup-2/Desktop/{yr}_TMS/{yr}_final_2/{i}".format(yr=yr, i=i), encoding='cp949', index = False)
# edit test
# third edit test in VS CODE
        
        
init_codename(2019)
#make_annual_file(2019)
#make_annual_file_meteo(2019)
#data_mul(2019)
pre_date(2019)
