import csv
import pygrib
import numpy as np
from datetime import date, timedelta
from pathlib import Path

class extractor():
    def __init__(self):
        self.version = '1.0'
        self.info = 'extract location point data from WRF dataset'

        self.rw = "r"
        self.loc = [(30.20, -123.04), (35.20, -111.04)]
        self.loc_info = ["loc1", "loc2"]
        self.parameter = [22,35,66,67]
        self.parameter_info = ["p1", "p2", "p3", "p4"]
        
        self.data_path = "N:/weather/WRF/"
        self.write_data_path = "F:/extract_weather_parameter/Data/WRF/"
        self.start_date = date(2019, 1, 1)
        self.end_date = date(2019, 1, 3)

    

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def hourrage(self):
        for n in range(24):
            yield "%02d"%n

    
    def read_loop(self):
        for single_date in self.daterange(self.start_date, self.end_date):
            for single_hour in self.hourrage():
                data_path = self.data_path+single_date.strftime("%Y")+"/"+single_date.strftime("%Y%m%d")+"/"+"hrrr."+single_date.strftime("%Y%m%d")+"."+single_hour+".00.grib2"
                write_path = self.write_data_path+single_date.strftime("%Y")+"/"+single_date.strftime("%Y%m%d")+"/"
                data = self.read_data(data_path, write_path)
                self.write_data(data, write_path, single_date,single_hour)
  



    def read_data(self, data_path, write_path):
        grib = pygrib.open(data_path)

        data_dic = {}
        for l in self.loc_info:
            data_dic[l] = []

        for p in self.parameter:
            tmpmsgs = grib[p]
            lt, ln = tmpmsgs.latlons() # lt - latitude, ln - longitude
            data = tmpmsgs.values
            

            for (l_lt, l_ln), l_info in zip(self.loc, self.loc_info):
                
                l_lt_m = np.full_like(lt, l_lt)
                l_ln_m = np.full_like(ln, l_ln)
                dis_mat = (lt-l_lt_m)**2+(ln-l_ln_m)**2
                p_lt, p_ln = np.unravel_index(dis_mat.argmin(), dis_mat.shape)
                value = data[p_lt, p_ln]

                data_dic[l_info].append(value)

        return data_dic

    def write_data(self, data, w_data_path,single_date,single_hour):

        for k, v in data.items():
            w_path = w_data_path
            Path(w_path).mkdir(parents=True, exist_ok=True)
            w_path_file = w_data_path+k+"."+single_date.strftime("%Y%m%d")+".csv"
            with open(w_path_file, 'a', newline='') as file_out:
                writer = csv.writer(file_out, delimiter=',')
                write_list = []
                write_list.append(single_date.strftime("%Y/%m/%d")+":"+single_hour)
                write_list = write_list+v
                writer.writerow(write_list)

e = extractor()
e.read_loop()
