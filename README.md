
## Comando de ejecucion del EPGA

hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar -input /Challenge_adge/epga/islands.txt -output /Challenge_adge/epga/outputi -mapper "python3 mapper.py" -reducer "python3 reducer.py" -file /home/hadoop/Challenge-Adge/epga/mapper.py -file /home/hadoop/Challenge-Adge/epga/reducer.py -file /home/hadoop/Challenge-Adge/epga/config.json -file /home/hadoop/Challenge-Adge/epga/utils/mapper_utils.py -file /home/hadoop/Challenge-Adge/epga/utils/reducer_utils.py -file /home/hadoop/Challenge-Adge/epga/utils/ga_utils.py -file /home/hadoop/Challenge-Adge/epga/utils/hdfs_utils.py -file /home/hadoop/Challenge-Adge/epga/utils/_init_.py -cmdenv PYTHONPATH=.

## Comando de ejecucion de SGA

python main.py
