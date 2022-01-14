# MySQL-DataProcessing
The code showcase how data processing from MySQL can be done via Python script. 
The script can perform the following functions: 
- Calculate a period of sensor below/over threshold value
- Separate sensor set
- Sort different sensor modules into their own categories
- Day/night classification
- Sensor values classification into LOW/HIGH (depending on threshold)
- Channel the processed data to another server
## Before Processing
The following image shows the raw data from MySQL server before processing: </br> 

![image](https://user-images.githubusercontent.com/22144223/149451181-4e0b96f1-c080-4274-afd7-33673f64b951.png)
## After Processing
The following image shows the processed data after running through [Python script](https://github.com/xian-cong/MySQL-DataProcessing/blob/main/MySQL-DataProcessing.py): </br> 
#### Processed data in xls format
![image](https://user-images.githubusercontent.com/22144223/149450997-fb80c092-4bb4-41be-bee3-120961cf600b.png)
#### Processed data in new MySQL server
![image](https://user-images.githubusercontent.com/22144223/149451144-e1cf4159-e545-4143-a3ab-313b7d58498d.png)
