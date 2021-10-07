'''
@Author: Naziya
@Date: 2021-10-05
@Last Modified by: Naziya
@Last Modified : 2021-10-05
@Title : Program Aim is to send stock data from producer to consumer
and store it in hdfs
'''

from kafka import KafkaConsumer
import pydoop.hdfs as hdfs
import subprocess

consumer = KafkaConsumer('topic9')
hdfs.mkdir('hdfs://localhost:9000/stock_price_data')
file = '/home/naziya/stock_data.csv'
args_list = [ 'hdfs', 'dfs', '-put',file, '/stock_price_data']
print('Running system command: {}'.format(' '.join(args_list)))
proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
proc.communicate()
hdfs_path = 'hdfs://localhost:9000/stock_price_data/stock_data.csv'


for message in consumer:
    values = message.value.decode('utf-8')
    with hdfs.open(hdfs_path, 'at') as f:
        f.write(f"{values}\n")