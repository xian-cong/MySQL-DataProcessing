import mysql.connector
import datetime
from datetime import timedelta
from datetime import datetime

#import timestamp

testdb = mysql.connector.connect(
	host = "sql.hphttech.com",
	user = "xiancong",
	passwd = "********",
	database = "clds_compressor"
	)
test_cursor = testdb.cursor(buffered=True)


mydb = mysql.connector.connect(
	host = "sql.hphttech.com",
	user = "xiancong",
	passwd = "********",
	)
my_cursor = mydb.cursor(buffered=True)

db2 = mysql.connector.connect(
	host = "localhost",
	user = "root",
	passwd = "",
	database = "testdatabase"
	)

p_cursor = db2.cursor(buffered=True)

my_cursor.execute(f"SELECT id, cast(datetime as time) as Time, cast(datetime as date) as Date, ph,ec, wt, serial, sensor_set, h, t, vp, bp, rain, st, wet, st2, wet2, k, n, p, par, rainp, rainv, wd, ws FROM clds_compressor.monitor_gh00xx order by serial, datetime")
myresult = my_cursor.fetchall()

p_cursor.execute(f"SELECT * FROM testdatabase.processed_daily_threshold")
daily_threshold_data = p_cursor.fetchall()

pHLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
ecLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
wtLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
hLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
tLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
vpLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
bpLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
rainLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
stLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
wetLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
st2Limit = {
	"low": 1,
	"high": 1,
	"limit": False
}
wet2Limit = {
	"low": 1,
	"high": 1,
	"limit": False
}
kLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
nLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
pLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
parLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
rainpLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
rainvLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
wdLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
wsLimit = {
	"low": 1,
	"high": 1,
	"limit": False
}
oldSerial = None
occurrenceExist = False
occurrenceInfo = []

if daily_threshold_data != None:
	for data in daily_threshold_data:
		datetime_object = datetime.strptime(data[6],"%H:%M:%S")
		occurrenceInfo.append(f"{data[1]}|{data[2]}|{data[3]}|{data[4]}|{data[5]}|{datetime_object}|{data[7]}")

for data in myresult:
	# print(data)
	ph = float(data[3]) if data[3] != None else 0
	ec = float(data[4]) if data[4] != None else 0
	wt = float(data[5]) if data[5] != None else 0
	serial = str(data[6])
	sensor_set = data[7]
	h = float(data[8]) if data[8] != None else 0
	t = float(data[9]) if data[9] != None else 0
	vp = float(data[10]) if data[10] != None else 0
	bp = float(data[11]) if data[11] != None else 0
	rain = float(data[12]) if data[12] != None else 0
	st = float(data[13]) if data[13] != None else 0
	wet = float(data[14]) if data[14] != None else 0
	st2 = float(data[15]) if data[15] != None else 0
	wet2 = float(data[16]) if data[16] != None else 0
	k = float(data[17]) if data[17] != None else 0
	n = float(data[18]) if data[18] != None else 0
	p = float(data[19]) if data[19] != None else 0
	par = float(data[20]) if data[20] != None else 0
	rainp = float(data[21]) if data[21] != None else 0
	rainv = float(data[22]) if data[22] != None else 0
	wd = float(data[23]) if data[23] != None else 0
	ws = float(data[24]) if data[24] != None else 0
	if oldSerial == None:
		oldSerial = serial
	elif oldSerial != serial:
		oldSerial = serial
		pHLimit["limit"] = False
		ecLimit["limit"] = False
		wtLimit["limit"] = False
		hLimit["limit"] = False
		tLimit["limit"] = False
		vpLimit["limit"] = False
		bpLimit["limit"] = False
		rainLimit["limit"] = False
		stLimit["limit"] = False
		wetLimit["limit"] = False
		st2Limit["limit"] = False
		wet2Limit["limit"] = False
		kLimit["limit"] = False
		nLimit["limit"] = False
		pLimit["limit"] = False
		parLimit["limit"] = False
		rainpLimit["limit"] = False
		rainvLimit["limit"] = False
		wdLimit["limit"] = False
		wsLimit["limit"] = False

	#pH threshold checking
	if pHLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'ph'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			ph_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			ph_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			ph_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			ph_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			ph_plot_limit = thresholdResult[6]
			if ph_plot_limit == 0:
				ph_compare_limit_low = ph_default_limit_low
				ph_compare_limit_high = ph_default_limit_high
			else:
				ph_compare_limit_low = ph_limit_low
				ph_compare_limit_high = ph_limit_high
			pHLimit["limit"] = True
		else:
			ph_compare_limit_low = 0
			ph_compare_limit_high = 0

	if ph != 0:
		if ph < ph_compare_limit_low and ph != 0:
			if pHLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_ph = data[3]
				pHLimit["low"] = 0

		if ph >= ph_compare_limit_low and pHLimit["low"] == 0 and ph != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			pHLimit["low"] = 1
			end_ph = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than pH {ph_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_ph} End time {end_time}, id {end_id} {end_ph}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'pH'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, ph_limit_low, ph_limit_high, ph_default_limit_low, ph_default_limit_high, 'pH', 'LOW', ph_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "pH" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'pH' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|pH|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'pH', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if ph > ph_compare_limit_high and ph != 0:
			if pHLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_ph = data[3]
				pHLimit["high"] = 0

		if ph <= ph_compare_limit_high and pHLimit["high"] == 0 and ph != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			pHLimit["high"] = 1
			end_ph = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than pH {ph_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_ph} End time {end_time}, id {end_id} {end_ph}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'pH'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, ph_limit_low, ph_limit_high, ph_default_limit_low, ph_default_limit_high, 'pH', 'HIGH', ph_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "pH" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'pH' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|pH|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'pH', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#ec threshold checking
	if ecLimit["limit"]== False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'ec'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			ec_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			ec_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			ec_default_limit_low = thresholdResult[4] if thresholdResult[4] != None else 0
			ec_default_limit_high = thresholdResult[5] if thresholdResult[5] != None else 0
			ec_plot_limit = thresholdResult[6]
			if ec_plot_limit == 0:
				ec_compare_limit_low = ec_default_limit_low
				ec_compare_limit_high = ec_default_limit_high
			else:
				ec_compare_limit_low = ec_limit_low
				ec_compare_limit_high = ec_limit_high
			ecLimit["limit"]= True
		else:
			ec_compare_limit_low = 0
			ec_compare_limit_high = 0
	
	if ec != 0:
		if ec < ec_compare_limit_low and ec != 0:
			if ecLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_ec = data[4]
				ecLimit["low"] = 0

		if ec >= ec_compare_limit_low and ecLimit["low"] == 0 and ec != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			ecLimit["low"] = 1
			end_ec = data[4]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)

			print("---Lower than ec 550---")
			print(f"Start time {start_time}, id {init_id} {init_ec} End time {end_time}, id {end_id} {end_ec}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'ec'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, ec_limit_low, ec_limit_high, ec_default_limit_low, ec_default_limit_high, 'ec', 'LOW', ec_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "ec" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'ec' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|ec|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'ec', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if ec > ec_compare_limit_high and ec != 0:
			if ecLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_ec = data[4]
				ecLimit["high"] = 0

		if ec <= ec_compare_limit_high and ecLimit["high"] == 0 and ec != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			ecLimit["high"] = 1
			end_ec = data[4]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)

			print("---Higher than ec 1800---")
			print(f"Start time {start_time}, id {init_id} {init_ec} End time {end_time}, id {end_id} {end_ec}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'ec'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type, plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, ec_limit_low, ec_limit_high, ec_default_limit_low, ec_default_limit_high, 'ec', 'HIGH', ec_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "ec" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'ec' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|ec|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'ec', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		####################################################################################################################

	#wt threshold checking
	if wtLimit["limit"]== False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'wt'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			wt_limit_low = float(thresholdResult[2]) if thresholdResult[4] != None else 0
			wt_limit_high = float(thresholdResult[3]) if thresholdResult[4] != None else 0
			wt_default_limit_low = thresholdResult[4] if thresholdResult[4] != None else 0
			wt_default_limit_high = thresholdResult[5] if thresholdResult[5] != None else 0
			wt_plot_limit = thresholdResult[6]
			if wt_plot_limit == 0:
				wt_compare_limit_low = wt_default_limit_low
				wt_compare_limit_high = wt_default_limit_high
			else:
				wt_compare_limit_low = wt_limit_low
				wt_compare_limit_high = wt_limit_high
			wtLimit["limit"]= True
		else:
			wt_compare_limit_low = 0
			wt_compare_limit_high = 0

	if wt != 0:
		if wt < wt_compare_limit_low and wt != 0:
				if wtLimit["low"] == 1:
					init_id = data[0]
					start_time = data[1]
					init_wt = data[5]
					wtLimit["low"] = 0

		if wt >= wt_compare_limit_low and wtLimit["low"] == 0 and wt != 0:
				time = str(data[1])
				date = str(data[2])

				hour, minute, second = time.split(":")

				if int(hour) >= 7 and int(hour) < 19:
					timeDay = "day"
					print(date, time, "day")
				else:
					timeDay = "night"
					print(date, time, "night")
				end_id = data[0]
				end_time = data[1]
				wtLimit["low"] = 1
				end_wt = data[5]
				difference = end_time - start_time
				days, second = abs(difference.days), difference.seconds
				if difference.seconds == 0:
					continue
				hour = second // 3600
				minute = (second % 3600) // 60
				second = (second % 60)		

				print("---Lower than wt 16---")
				print(f"Start time {start_time}, id {init_id} {init_wt} End time {end_time}, id {end_id} {end_wt}")
				print(f"Duration is {hour} hour {minute} minute {second} second")
				print("--------")
				duration = f"{hour}:{minute}:{second}"
				# if days > 0:
				# 	duration = f"{days} day, {hour}:{minute}:{second}"

				p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'wt'")
				checkResult = p_cursor.fetchone()
				if checkResult[0] == 0:
					sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
					value = (init_id, serial, date,start_time, end_time, duration, wt_limit_low, wt_limit_high, wt_default_limit_low, wt_default_limit_high, 'wt', 'LOW', wt_plot_limit, sensor_set, timeDay)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record inserted")

					for n, x in enumerate(occurrenceInfo):
						info = x.split("|")
						if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "wt" and info[6] == sensor_set:
							info[4] = str(int(info[4])+1)
							newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
							newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
							days, second = abs(newDelta.days), newDelta.seconds
							hour = second // 3600
							minute = (second % 3600) // 60
							second = (second % 60)

							info[5] = f"{hour}:{minute}:{second}"
							# if days > 0:
							# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

							occurrenceExist = True
							occurrenceInfo[n] = "|".join(info)
							#Update
							sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'wt' AND sensor_set = '{info[6]}'"
							p_cursor.execute(sql)
							db2.commit()
							print(p_cursor.rowcount, "record updated")

					if not occurrenceExist: 
						occurrenceInfo.append(f"{date}|{timeDay}|{serial}|wt|1|{hour}:{minute}:{second}|{sensor_set}")
						sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
						value = (date, timeDay, serial, 'wt', '1', duration, sensor_set)
						p_cursor.execute(sql, value)
						db2.commit()
						print(p_cursor.rowcount, "record updated")
					else:
						occurrenceExist = False

		if wt > wt_compare_limit_high and wt != 0:
				if wtLimit["high"] == 1:
					init_id = data[0]
					start_time = data[1]
					init_wt = data[5]
					wtLimit["high"] = 0

		if wt <= wt_compare_limit_high and wtLimit["high"] == 0 and wt != 0:
				time = str(data[1])
				date = str(data[2])

				hour, minute, second = time.split(":")

				if int(hour) >= 7 and int(hour) < 19:
					timeDay = "day"
					print(date, time, "day")
				else:
					timeDay = "night"
					print(date, time, "night")
				end_id = data[0]
				end_time = data[1]
				wtLimit["high"] = 1
				end_wt = data[5]
				difference = end_time - start_time
				days, second = abs(difference.days), difference.seconds
				if difference.seconds == 0:
					continue
				hour = second // 3600
				minute = (second % 3600) // 60
				second = (second % 60)
				
				print("---Higher than wt 30---")
				print(f"Start time {start_time}, id {init_id} {init_wt} End time {end_time}, id {end_id} {end_wt}")
				print(f"Duration is {hour} hour {minute} minute {second} second")
				print("--------")
				duration = f"{hour}:{minute}:{second}"
				# if days > 0:
				# 	duration = f"{days} day, {hour}:{minute}:{second}"

				p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'wt'")
				checkResult = p_cursor.fetchone()
				if checkResult[0] == 0:
					sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
					value = (init_id, serial, date,start_time, end_time, duration, wt_limit_low, wt_limit_high, wt_default_limit_low, wt_default_limit_high, 'wt', 'HIGH', wt_plot_limit, sensor_set, timeDay)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record inserted")

					for n, x in enumerate(occurrenceInfo):
						info = x.split("|")
						if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "wt" and info[6] == sensor_set:
							info[4] = str(int(info[4])+1)
							newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
							newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
							days, second = abs(newDelta.days), newDelta.seconds
							hour = second // 3600
							minute = (second % 3600) // 60
							second = (second % 60)

							info[5] = f"{hour}:{minute}:{second}"
							# if days > 0:
							# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

							occurrenceExist = True
							occurrenceInfo[n] = "|".join(info)
							#Update
							sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'wt' AND sensor_set = '{info[6]}'"
							p_cursor.execute(sql)
							db2.commit()
							print(p_cursor.rowcount, "record updated")

					if not occurrenceExist: 
						occurrenceInfo.append(f"{date}|{timeDay}|{serial}|wt|1|{hour}:{minute}:{second}|{sensor_set}")
						sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
						value = (date, timeDay, serial, 'wt', '1', duration, sensor_set)
						p_cursor.execute(sql, value)
						db2.commit()
						print(p_cursor.rowcount, "record updated")
					else:
						occurrenceExist = False

	#h threshold checking
	if hLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'h'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			h_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			h_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			h_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			h_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			h_plot_limit = thresholdResult[6]
			if h_plot_limit == 0:
				h_compare_limit_low = h_default_limit_low
				h_compare_limit_high = h_default_limit_high
			else:
				h_compare_limit_low = h_limit_low
				h_compare_limit_high = h_limit_high
			hLimit["limit"] = True
		else:
			h_compare_limit_low = 0
			h_compare_limit_high = 0

	if h != 0:
		if h < h_compare_limit_low and h != 0:
			if hLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_h = data[3]
				hLimit["low"] = 0

		if h >= h_compare_limit_low and hLimit["low"] == 0 and h != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			hLimit["low"] = 1
			end_h = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than h {h_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_h} End time {end_time}, id {end_id} {end_h}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'h'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, h_limit_low, h_limit_high, h_default_limit_low, h_default_limit_high, 'h', 'LOW', h_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "h" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'h' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|h|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'h', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if h > h_compare_limit_high and h != 0:
			if hLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_h = data[3]
				hLimit["high"] = 0

		if h <= h_compare_limit_high and hLimit["high"] == 0 and h != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			hLimit["high"] = 1
			end_h = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than h {h_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_h} End time {end_time}, id {end_id} {end_h}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'h'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, h_limit_low, h_limit_high, h_default_limit_low, h_default_limit_high, 'h', 'HIGH', h_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "h" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'h' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|h|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'h', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#t threshold checking
	if tLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 't'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			t_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			t_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			t_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			t_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			t_plot_limit = thresholdResult[6]
			if t_plot_limit == 0:
				t_compare_limit_low = t_default_limit_low
				t_compare_limit_high = t_default_limit_high
			else:
				t_compare_limit_low = t_limit_low
				t_compare_limit_high = t_limit_high
			tLimit["limit"] = True
		else:
			t_compare_limit_low = 0
			t_compare_limit_high = 0

	if t != 0:
		if t < t_compare_limit_low and t != 0:
			if tLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_t = data[3]
				tLimit["low"] = 0

		if t >= t_compare_limit_low and tLimit["low"] == 0 and t != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			tLimit["low"] = 1
			end_t = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than t {t_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_t} End time {end_time}, id {end_id} {end_t}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 't'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, t_limit_low, t_limit_high, t_default_limit_low, t_default_limit_high, 't', 'LOW', t_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "t" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 't' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|t|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 't', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if t > t_compare_limit_high and t != 0:
			if tLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_t = data[3]
				tLimit["high"] = 0

		if t <= t_compare_limit_high and tLimit["high"] == 0 and t != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			tLimit["high"] = 1
			end_t = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than t {t_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_t} End time {end_time}, id {end_id} {end_t}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 't'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, t_limit_low, t_limit_high, t_default_limit_low, t_default_limit_high, 't', 'HIGH', t_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "t" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 't' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|t|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 't', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#vp threshold checking
	if vpLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'vp'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			vp_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			vp_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			vp_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			vp_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			vp_plot_limit = thresholdResult[6]
			if vp_plot_limit == 0:
				vp_compare_limit_low = vp_default_limit_low
				vp_compare_limit_high = vp_default_limit_high
			else:
				vp_compare_limit_low = vp_limit_low
				vp_compare_limit_high = vp_limit_high
			vpLimit["limit"] = True
		else:
			vp_compare_limit_low = 0
			vp_compare_limit_high = 0

	if vp != 0:
		if vp < vp_compare_limit_low and vp != 0:
			if vpLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_vp = data[3]
				vpLimit["low"] = 0

		if vp >= vp_compare_limit_low and vpLimit["low"] == 0 and vp != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			vpLimit["low"] = 1
			end_vp = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than vp {vp_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_vp} End time {end_time}, id {end_id} {end_vp}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'vp'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, vp_limit_low, vp_limit_high, vp_default_limit_low, vp_default_limit_high, 'vp', 'LOW', vp_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "vp" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'vp' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|vp|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'vp', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if vp > vp_compare_limit_high and vp != 0:
			if vpLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_vp = data[3]
				vpLimit["high"] = 0

		if vp <= vp_compare_limit_high and vpLimit["high"] == 0 and vp != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			vpLimit["high"] = 1
			end_vp = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than vp {vp_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_vp} End time {end_time}, id {end_id} {end_vp}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'vp'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, vp_limit_low, vp_limit_high, vp_default_limit_low, vp_default_limit_high, 'vp', 'HIGH', vp_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "vp" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'vp' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|vp|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'vp', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#bp threshold checking
	if bpLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'bp'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			bp_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			bp_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			bp_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			bp_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			bp_plot_limit = thresholdResult[6]
			if bp_plot_limit == 0:
				bp_compare_limit_low = bp_default_limit_low
				bp_compare_limit_high = bp_default_limit_high
			else:
				bp_compare_limit_low = bp_limit_low
				bp_compare_limit_high = bp_limit_high
			bpLimit["limit"] = True
		else:
			bp_compare_limit_low = 0
			bp_compare_limit_high = 0

	if bp != 0:
		if bp < bp_compare_limit_low and bp != 0:
			if bpLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_bp = data[3]
				bpLimit["low"] = 0

		if bp >= bp_compare_limit_low and bpLimit["low"] == 0 and bp != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			bpLimit["low"] = 1
			end_bp = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than bp {bp_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_bp} End time {end_time}, id {end_id} {end_bp}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'bp'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, bp_limit_low, bp_limit_high, bp_default_limit_low, bp_default_limit_high, 'bp', 'LOW', bp_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "bp" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'bp' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|bp|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'bp', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if bp > bp_compare_limit_high and bp != 0:
			if bpLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_bp = data[3]
				bpLimit["high"] = 0

		if bp <= bp_compare_limit_high and bpLimit["high"] == 0 and bp != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			bpLimit["high"] = 1
			end_bp = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than bp {bp_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_bp} End time {end_time}, id {end_id} {end_bp}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'bp'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, bp_limit_low, bp_limit_high, bp_default_limit_low, bp_default_limit_high, 'bp', 'HIGH', bp_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "bp" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'bp' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|bp|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'bp', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#rain threshold checking
	if rainLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'rain'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			rain_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			rain_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			rain_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			rain_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			rain_plot_limit = thresholdResult[6]
			if rain_plot_limit == 0:
				rain_compare_limit_low = rain_default_limit_low
				rain_compare_limit_high = rain_default_limit_high
			else:
				rain_compare_limit_low = rain_limit_low
				rain_compare_limit_high = rain_limit_high
			rainLimit["limit"] = True
		else:
			rain_compare_limit_low = 0
			rain_compare_limit_high = 0

	if rain != 0:
		if rain < rain_compare_limit_low and rain != 0:
			if rainLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_rain = data[3]
				rainLimit["low"] = 0

		if rain >= rain_compare_limit_low and rainLimit["low"] == 0 and rain != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			rainLimit["low"] = 1
			end_rain = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than rain {rain_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_rain} End time {end_time}, id {end_id} {end_rain}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'rain'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, rain_limit_low, rain_limit_high, rain_default_limit_low, rain_default_limit_high, 'rain', 'LOW', rain_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "rain" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'rain' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|rain|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'rain', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if rain > rain_compare_limit_high and rain != 0:
			if rainLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_rain = data[3]
				rainLimit["high"] = 0

		if rain <= rain_compare_limit_high and rainLimit["high"] == 0 and rain != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			rainLimit["high"] = 1
			end_rain = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than rain {rain_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_rain} End time {end_time}, id {end_id} {end_rain}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'rain'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, rain_limit_low, rain_limit_high, rain_default_limit_low, rain_default_limit_high, 'rain', 'HIGH', rain_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "rain" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'rain' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|rain|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'rain', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#st threshold checking
	if stLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'st'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			st_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			st_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			st_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			st_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			st_plot_limit = thresholdResult[6]
			if st_plot_limit == 0:
				st_compare_limit_low = st_default_limit_low
				st_compare_limit_high = st_default_limit_high
			else:
				st_compare_limit_low = st_limit_low
				st_compare_limit_high = st_limit_high
			stLimit["limit"] = True
		else:
			st_compare_limit_low = 0
			st_compare_limit_high = 0

	if st != 0:
		if st < st_compare_limit_low and st != 0:
			if stLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_st = data[3]
				stLimit["low"] = 0

		if st >= st_compare_limit_low and stLimit["low"] == 0 and st != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			stLimit["low"] = 1
			end_st = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than st {st_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_st} End time {end_time}, id {end_id} {end_st}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'st'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, st_limit_low, st_limit_high, st_default_limit_low, st_default_limit_high, 'st', 'LOW', st_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "st" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'st' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|st|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'st', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if st > st_compare_limit_high and st != 0:
			if stLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_st = data[3]
				stLimit["high"] = 0

		if st <= st_compare_limit_high and stLimit["high"] == 0 and st != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			stLimit["high"] = 1
			end_st = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than st {st_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_st} End time {end_time}, id {end_id} {end_st}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'st'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, st_limit_low, st_limit_high, st_default_limit_low, st_default_limit_high, 'st', 'HIGH', st_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "st" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'st' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|st|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'st', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#wet threshold checking
	if wetLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'wet'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			wet_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			wet_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			wet_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			wet_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			wet_plot_limit = thresholdResult[6]
			if wet_plot_limit == 0:
				wet_compare_limit_low = wet_default_limit_low
				wet_compare_limit_high = wet_default_limit_high
			else:
				wet_compare_limit_low = wet_limit_low
				wet_compare_limit_high = wet_limit_high
			wetLimit["limit"] = True
		else:
			wet_compare_limit_low = 0
			wet_compare_limit_high = 0

	if wet != 0:
		if wet < wet_compare_limit_low and wet != 0:
			if wetLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_wet = data[3]
				wetLimit["low"] = 0

		if wet >= wet_compare_limit_low and wetLimit["low"] == 0 and wet != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			wetLimit["low"] = 1
			end_wet = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than wet {wet_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_wet} End time {end_time}, id {end_id} {end_wet}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'wet'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, wet_limit_low, wet_limit_high, wet_default_limit_low, wet_default_limit_high, 'wet', 'LOW', wet_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "wet" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'wet' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|wet|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'wet', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if wet > wet_compare_limit_high and wet != 0:
			if wetLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_wet = data[3]
				wetLimit["high"] = 0

		if wet <= wet_compare_limit_high and wetLimit["high"] == 0 and wet != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			wetLimit["high"] = 1
			end_wet = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than wet {wet_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_wet} End time {end_time}, id {end_id} {end_wet}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'wet'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, wet_limit_low, wet_limit_high, wet_default_limit_low, wet_default_limit_high, 'wet', 'HIGH', wet_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "wet" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'wet' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|wet|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'wet', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#st2 threshold checking
	if st2Limit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'st2'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			st2_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			st2_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			st2_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			st2_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			st2_plot_limit = thresholdResult[6]
			if st2_plot_limit == 0:
				st2_compare_limit_low = st2_default_limit_low
				st2_compare_limit_high = st2_default_limit_high
			else:
				st2_compare_limit_low = st2_limit_low
				st2_compare_limit_high = st2_limit_high
			st2Limit["limit"] = True
		else:
			st2_compare_limit_low = 0
			st2_compare_limit_high = 0

	if st2 != 0:
		if st2 < st2_compare_limit_low and st2 != 0:
			if st2Limit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_st2 = data[3]
				st2Limit["low"] = 0

		if st2 >= st2_compare_limit_low and st2Limit["low"] == 0 and st2 != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			st2Limit["low"] = 1
			end_st2 = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than st2 {st2_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_st2} End time {end_time}, id {end_id} {end_st2}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'st2'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, st2_limit_low, st2_limit_high, st2_default_limit_low, st2_default_limit_high, 'st2', 'LOW', st2_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "st2" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'st2' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|st2|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'st2', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if st2 > st2_compare_limit_high and st2 != 0:
			if st2Limit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_st2 = data[3]
				st2Limit["high"] = 0

		if st2 <= st2_compare_limit_high and st2Limit["high"] == 0 and st2 != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			st2Limit["high"] = 1
			end_st2 = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than st2 {st2_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_st2} End time {end_time}, id {end_id} {end_st2}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'st2'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, st2_limit_low, st2_limit_high, st2_default_limit_low, st2_default_limit_high, 'st2', 'HIGH', st2_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "st2" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'st2' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|st2|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'st2', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#wet2 threshold checking
	if wet2Limit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'wet2'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			wet2_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			wet2_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			wet2_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			wet2_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			wet2_plot_limit = thresholdResult[6]
			if wet2_plot_limit == 0:
				wet2_compare_limit_low = wet2_default_limit_low
				wet2_compare_limit_high = wet2_default_limit_high
			else:
				wet2_compare_limit_low = wet2_limit_low
				wet2_compare_limit_high = wet2_limit_high
			wet2Limit["limit"] = True
		else:
			wet2_compare_limit_low = 0
			wet2_compare_limit_high = 0

	if wet2 != 0:
		if wet2 < wet2_compare_limit_low and wet2 != 0:
			if wet2Limit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_wet2 = data[3]
				wet2Limit["low"] = 0

		if wet2 >= wet2_compare_limit_low and wet2Limit["low"] == 0 and wet2 != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			wet2Limit["low"] = 1
			end_wet2 = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than wet2 {wet2_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_wet2} End time {end_time}, id {end_id} {end_wet2}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'wet2'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, wet2_limit_low, wet2_limit_high, wet2_default_limit_low, wet2_default_limit_high, 'wet2', 'LOW', wet2_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "wet2" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'wet2' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|wet2|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'wet2', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if wet2 > wet2_compare_limit_high and wet2 != 0:
			if wet2Limit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_wet2 = data[3]
				wet2Limit["high"] = 0

		if wet2 <= wet2_compare_limit_high and wet2Limit["high"] == 0 and wet2 != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			wet2Limit["high"] = 1
			end_wet2 = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than wet2 {wet2_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_wet2} End time {end_time}, id {end_id} {end_wet2}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'wet2'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, wet2_limit_low, wet2_limit_high, wet2_default_limit_low, wet2_default_limit_high, 'wet2', 'HIGH', wet2_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "wet2" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'wet2' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|wet2|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'wet2', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#k threshold checking
	if kLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'k'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			k_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			k_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			k_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			k_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			k_plot_limit = thresholdResult[6]
			if k_plot_limit == 0:
				k_compare_limit_low = k_default_limit_low
				k_compare_limit_high = k_default_limit_high
			else:
				k_compare_limit_low = k_limit_low
				k_compare_limit_high = k_limit_high
			kLimit["limit"] = True
		else:
			k_compare_limit_low = 0
			k_compare_limit_high = 0
	
	if k != 0:
		if k < k_compare_limit_low and k != 0:
			if kLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_k = data[3]
				kLimit["low"] = 0

		if k >= k_compare_limit_low and kLimit["low"] == 0 and k != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			kLimit["low"] = 1
			end_k = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than k {k_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_k} End time {end_time}, id {end_id} {end_k}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'k'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, k_limit_low, k_limit_high, k_default_limit_low, k_default_limit_high, 'k', 'LOW', k_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "k" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'k' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|k|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'k', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if k > k_compare_limit_high and k != 0:
			if kLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_k = data[3]
				kLimit["high"] = 0

		if k <= k_compare_limit_high and kLimit["high"] == 0 and k != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			kLimit["high"] = 1
			end_k = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than k {k_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_k} End time {end_time}, id {end_id} {end_k}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'k'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, k_limit_low, k_limit_high, k_default_limit_low, k_default_limit_high, 'k', 'HIGH', k_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "k" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'k' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|k|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'k', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#n threshold checking
	if nLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'n'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			n_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			n_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			n_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			n_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			n_plot_limit = thresholdResult[6]
			if n_plot_limit == 0:
				n_compare_limit_low = n_default_limit_low
				n_compare_limit_high = n_default_limit_high
			else:
				n_compare_limit_low = n_limit_low
				n_compare_limit_high = n_limit_high
			nLimit["limit"] = True
		else:
			n_compare_limit_low = 0
			n_compare_limit_high = 0

	if n != 0:
		if n < n_compare_limit_low and n != 0:
			if nLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_n = data[3]
				nLimit["low"] = 0

		if n >= n_compare_limit_low and nLimit["low"] == 0 and n != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			nLimit["low"] = 1
			end_n = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than n {n_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_n} End time {end_time}, id {end_id} {end_n}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'n'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, n_limit_low, n_limit_high, n_default_limit_low, n_default_limit_high, 'n', 'LOW', n_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "n" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'n' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|n|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'n', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if n > n_compare_limit_high and n != 0:
			if nLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_n = data[3]
				nLimit["high"] = 0

		if n <= n_compare_limit_high and nLimit["high"] == 0 and n != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			nLimit["high"] = 1
			end_n = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than n {n_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_n} End time {end_time}, id {end_id} {end_n}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'n'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, n_limit_low, n_limit_high, n_default_limit_low, n_default_limit_high, 'n', 'HIGH', n_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "n" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'n' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|n|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'n', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#p threshold checking
	if pLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'p'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			p_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			p_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			p_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			p_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			p_plot_limit = thresholdResult[6]
			if p_plot_limit == 0:
				p_compare_limit_low = p_default_limit_low
				p_compare_limit_high = p_default_limit_high
			else:
				p_compare_limit_low = p_limit_low
				p_compare_limit_high = p_limit_high
			pLimit["limit"] = True
		else:
			p_compare_limit_low = 0
			p_compare_limit_high = 0

	if p != 0:
		if p < p_compare_limit_low and p != 0:
			if pLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_p = data[3]
				pLimit["low"] = 0

		if p >= p_compare_limit_low and pLimit["low"] == 0 and p != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			pLimit["low"] = 1
			end_p = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than p {p_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_p} End time {end_time}, id {end_id} {end_p}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'p'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, p_limit_low, p_limit_high, p_default_limit_low, p_default_limit_high, 'p', 'LOW', p_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "p" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'p' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|p|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'p', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if p > p_compare_limit_high and p != 0:
			if pLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_p = data[3]
				pLimit["high"] = 0

		if p <= p_compare_limit_high and pLimit["high"] == 0 and p != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			pLimit["high"] = 1
			end_p = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than p {p_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_p} End time {end_time}, id {end_id} {end_p}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'p'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, p_limit_low, p_limit_high, p_default_limit_low, p_default_limit_high, 'p', 'HIGH', p_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "p" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'p' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|p|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'p', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#par threshold checking
	if parLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'par'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			par_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			par_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			par_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			par_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			par_plot_limit = thresholdResult[6]
			if par_plot_limit == 0:
				par_compare_limit_low = par_default_limit_low
				par_compare_limit_high = par_default_limit_high
			else:
				par_compare_limit_low = par_limit_low
				par_compare_limit_high = par_limit_high
			parLimit["limit"] = True
		else:
			par_compare_limit_low = 0
			par_compare_limit_high = 0

	if par != 0:
		if par < par_compare_limit_low and par != 0:
			if parLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_par = data[3]
				parLimit["low"] = 0

		if par >= par_compare_limit_low and parLimit["low"] == 0 and par != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			parLimit["low"] = 1
			end_par = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than par {par_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_par} End time {end_time}, id {end_id} {end_par}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'par'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, par_limit_low, par_limit_high, par_default_limit_low, par_default_limit_high, 'par', 'LOW', par_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "par" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'par' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|par|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'par', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if par > par_compare_limit_high and par != 0:
			if parLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_par = data[3]
				parLimit["high"] = 0

		if par <= par_compare_limit_high and parLimit["high"] == 0 and par != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			parLimit["high"] = 1
			end_par = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than par {par_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_par} End time {end_time}, id {end_id} {end_par}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'par'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, par_limit_low, par_limit_high, par_default_limit_low, par_default_limit_high, 'par', 'HIGH', par_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "par" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'par' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|par|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'par', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#rainp threshold checking
	if rainpLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'rainp'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			rainp_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			rainp_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			rainp_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			rainp_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			rainp_plot_limit = thresholdResult[6]
			if rainp_plot_limit == 0:
				rainp_compare_limit_low = rainp_default_limit_low
				rainp_compare_limit_high = rainp_default_limit_high
			else:
				rainp_compare_limit_low = rainp_limit_low
				rainp_compare_limit_high = rainp_limit_high
			rainpLimit["limit"] = True
		else:
			rainp_compare_limit_low = 0
			rainp_compare_limit_high = 0

	if rainp != 0:
		if rainp < rainp_compare_limit_low and rainp != 0:
			if rainpLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_rainp = data[3]
				rainpLimit["low"] = 0

		if rainp >= rainp_compare_limit_low and rainpLimit["low"] == 0 and rainp != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			rainpLimit["low"] = 1
			end_rainp = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than rainp {rainp_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_rainp} End time {end_time}, id {end_id} {end_rainp}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'rainp'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, rainp_limit_low, rainp_limit_high, rainp_default_limit_low, rainp_default_limit_high, 'rainp', 'LOW', rainp_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "rainp" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'rainp' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|rainp|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'rainp', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if rainp > rainp_compare_limit_high and rainp != 0:
			if rainpLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_rainp = data[3]
				rainpLimit["high"] = 0

		if rainp <= rainp_compare_limit_high and rainpLimit["high"] == 0 and rainp != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			rainpLimit["high"] = 1
			end_rainp = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than rainp {rainp_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_rainp} End time {end_time}, id {end_id} {end_rainp}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'rainp'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, rainp_limit_low, rainp_limit_high, rainp_default_limit_low, rainp_default_limit_high, 'rainp', 'HIGH', rainp_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "rainp" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'rainp' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|rainp|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'rainp', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#rainv threshold checking
	if rainvLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'rainv'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			rainv_limit_low = float(thresholdResult[2]) if thresholdResult[2 if thresholdResult[4] != None else 0] != None else 0
			rainv_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			rainv_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			rainv_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			rainv_plot_limit = thresholdResult[6]
			if rainv_plot_limit == 0:
				rainv_compare_limit_low = rainv_default_limit_low
				rainv_compare_limit_high = rainv_default_limit_high
			else:
				rainv_compare_limit_low = rainv_limit_low
				rainv_compare_limit_high = rainv_limit_high
			rainvLimit["limit"] = True
		else:
			rainv_compare_limit_low = 0
			rainv_compare_limit_high = 0

	if rainv != 0:
		if rainv < rainv_compare_limit_low and rainv != 0:
			if rainvLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_rainv = data[3]
				rainvLimit["low"] = 0

		if rainv >= rainv_compare_limit_low and rainvLimit["low"] == 0 and rainv != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			rainvLimit["low"] = 1
			end_rainv = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than rainv {rainv_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_rainv} End time {end_time}, id {end_id} {end_rainv}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'rainv'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, rainv_limit_low, rainv_limit_high, rainv_default_limit_low, rainv_default_limit_high, 'rainv', 'LOW', rainv_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "rainv" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'rainv' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|rainv|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'rainv', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if rainv > rainv_compare_limit_high and rainv != 0:
			if rainvLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_rainv = data[3]
				rainvLimit["high"] = 0

		if rainv <= rainv_compare_limit_high and rainvLimit["high"] == 0 and rainv != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			rainvLimit["high"] = 1
			end_rainv = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than rainv {rainv_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_rainv} End time {end_time}, id {end_id} {end_rainv}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'rainv'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, rainv_limit_low, rainv_limit_high, rainv_default_limit_low, rainv_default_limit_high, 'rainv', 'HIGH', rainv_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "rainv" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'rainv' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|rainv|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'rainv', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#wd threshold checking
	if wdLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'wd'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			wd_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			wd_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			wd_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			wd_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			wd_plot_limit = thresholdResult[6]
			if wd_plot_limit == 0:
				wd_compare_limit_low = wd_default_limit_low
				wd_compare_limit_high = wd_default_limit_high
			else:
				wd_compare_limit_low = wd_limit_low
				wd_compare_limit_high = wd_limit_high
			wdLimit["limit"] = True
		else:
			wd_compare_limit_low = 0
			wd_compare_limit_high = 0

	if wd != 0:
		if wd < wd_compare_limit_low and wd != 0:
			if wdLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_wd = data[3]
				wdLimit["low"] = 0

		if wd >= wd_compare_limit_low and wdLimit["low"] == 0 and wd != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			wdLimit["low"] = 1
			end_wd = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than wd {wd_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_wd} End time {end_time}, id {end_id} {end_wd}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'wd'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, wd_limit_low, wd_limit_high, wd_default_limit_low, wd_default_limit_high, 'wd', 'LOW', wd_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "wd" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'wd' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|wd|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'wd', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if wd > wd_compare_limit_high and wd != 0:
			if wdLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_wd = data[3]
				wdLimit["high"] = 0

		if wd <= wd_compare_limit_high and wdLimit["high"] == 0 and wd != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			wdLimit["high"] = 1
			end_wd = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than wd {wd_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_wd} End time {end_time}, id {end_id} {end_wd}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'wd'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, wd_limit_low, wd_limit_high, wd_default_limit_low, wd_default_limit_high, 'wd', 'HIGH', wd_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "wd" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'wd' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|wd|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'wd', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################

	#ws threshold checking
	if wsLimit["limit"] == False:
		test_cursor.execute(f"SELECT serial_id, table2.unit, limit_low, limit_high, default_limit_low, default_limit_high, plot_limit FROM clds_compressor.config_sensor AS table1 JOIN global_settings_parameter AS table2 ON table1.sensor_parameter_id = table2.id  WHERE serial_id = '{serial}' AND unit= 'ws'")
		thresholdResult = test_cursor.fetchone()
		if thresholdResult != None:
			ws_limit_low = float(thresholdResult[2]) if thresholdResult[2] != None else 0
			ws_limit_high = float(thresholdResult[3]) if thresholdResult[3] != None else 0
			ws_default_limit_low = float(thresholdResult[4]) if thresholdResult[4] != None else 0
			ws_default_limit_high = float(thresholdResult[5]) if thresholdResult[5] != None else 0
			ws_plot_limit = thresholdResult[6]
			if ws_plot_limit == 0:
				ws_compare_limit_low = ws_default_limit_low
				ws_compare_limit_high = ws_default_limit_high
			else:
				ws_compare_limit_low = ws_limit_low
				ws_compare_limit_high = ws_limit_high
			wsLimit["limit"] = True
		else:
			ws_compare_limit_low = 0
			ws_compare_limit_high = 0

	if ws != 0:
		if ws < ws_compare_limit_low and ws != 0:
			if wsLimit["low"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_ws = data[3]
				wsLimit["low"] = 0

		if ws >= ws_compare_limit_low and wsLimit["low"] == 0 and ws != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			wsLimit["low"] = 1
			end_ws = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			
			print(f"---Lower than ws {ws_compare_limit_low}---")
			print(f"Start time {start_time}, id {init_id} {init_ws} End time {end_time}, id {end_id} {end_ws}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'ws'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, ws_limit_low, ws_limit_high, ws_default_limit_low, ws_default_limit_high, 'ws', 'LOW', ws_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql, value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "ws" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'ws' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|ws|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'ws', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

		if ws > ws_compare_limit_high and ws != 0:
			if wsLimit["high"] == 1:
				init_id = data[0]
				start_time = data[1]
				init_ws = data[3]
				wsLimit["high"] = 0

		if ws <= ws_compare_limit_high and wsLimit["high"] == 0 and ws != 0:
			time = str(data[1])
			date = str(data[2])

			hour, minute, second = time.split(":")

			if int(hour) >= 7 and int(hour) < 19:
				timeDay = "day"
				print(date, time, "day")
			else:
				timeDay = "night"
				print(date, time, "night")
			end_id = data[0]
			end_time = data[1]
			wsLimit["high"] = 1
			end_ws = data[3]
			difference = end_time - start_time
			days, second = abs(difference.days), difference.seconds
			if difference.seconds == 0:
				continue
			hour = second // 3600
			minute = (second % 3600) // 60
			second = (second % 60)
			

			print(f"---Higher than ws {ws_compare_limit_high}---")
			print(f"Start time {start_time}, id {init_id} {init_ws} End time {end_time}, id {end_id} {end_ws}")
			print(f"Duration is {hour} hour {minute} minute {second} second")
			print("--------")
			duration = f"{hour}:{minute}:{second}"
			# if days > 0:
			# 		duration = f"{days} day, {hour}:{minute}:{second}"

			p_cursor.execute(f"SELECT COUNT(*) FROM testdatabase.data WHERE serial = '{serial}' AND date = '{date}' AND (time_start = '{start_time}' OR time_start LIKE '0%{start_time}') AND (time_end = '{end_time}' OR time_end LIKE '0%{end_time}') AND parameter = 'ws'")
			checkResult = p_cursor.fetchone()
			if checkResult[0] == 0:
				sql = "INSERT INTO Data(init_id, serial, date, time_start, time_end, duration, limit_low, limit_high, default_limit_low, default_limit_high, parameter, limit_type,plot_limit, sensor_set, timeDay) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				value = (init_id, serial, date,start_time, end_time, duration, ws_limit_low, ws_limit_high, ws_default_limit_low, ws_default_limit_high, 'ws', 'HIGH', ws_plot_limit, sensor_set, timeDay)
				p_cursor.execute(sql,value)
				db2.commit()
				print(p_cursor.rowcount, "record inserted")

				for n, x in enumerate(occurrenceInfo):
					info = x.split("|")
					if info[0] == date and info[1] == timeDay and info[2] == serial and info[3] == "ws" and info[6] == sensor_set:
						info[4] = str(int(info[4])+1)
						newDeltaHour, newDeltaMinutes, newDeltaSeconds = info[5].split(":")
						newDelta = timedelta(hours=int(newDeltaHour), minutes=int(newDeltaMinutes), seconds=int(newDeltaSeconds))+difference
						days, second = abs(newDelta.days), newDelta.seconds
						hour = second // 3600
						minute = (second % 3600) // 60
						second = (second % 60)

						info[5] = f"{hour}:{minute}:{second}"
						# if days > 0:
						# 		info[5] = f"{days} day, {hour}:{minute}:{second}"

						occurrenceExist = True
						occurrenceInfo[n] = "|".join(info)
						#Update
						sql = f"UPDATE testdatabase.processed_daily_threshold SET occurrence = '{info[4]}', duration = '{info[5]}' WHERE id= '{n+1}' AND date='{info[0]}' AND time = '{info[1]}' AND serial = '{info[2]}' AND parameter = 'ws' AND sensor_set = '{info[6]}'"
						p_cursor.execute(sql)
						db2.commit()
						print(p_cursor.rowcount, "record updated")

				if not occurrenceExist: 
					occurrenceInfo.append(f"{date}|{timeDay}|{serial}|ws|1|{hour}:{minute}:{second}|{sensor_set}")
					sql = "INSERT INTO processed_daily_threshold(date, time, serial, parameter, occurrence, duration, sensor_set) VALUES (%s,%s,%s,%s,%s,%s,%s)"
					value = (date, timeDay, serial, 'ws', '1', duration, sensor_set)
					p_cursor.execute(sql, value)
					db2.commit()
					print(p_cursor.rowcount, "record updated")
				else:
					occurrenceExist = False

	####################################################################################################################
mydb.close()
db2.close()
testdb.close()

#id, serial,date, time_start, time_end, duration, limit_value,limit_type, sensor_set, timeDay
