from datetime import datetime
import calendar

timestamp = 1660940445279
dt_object = datetime.fromtimestamp(int(timestamp/1000))

print("dt_object =", dt_object)
print("type(dt_object) =", type(dt_object))

print("Day of week =", dt_object.strftime('%A'))
print("Hour of day =", dt_object.strftime('%H'))
print("Min of hour =", dt_object.strftime('%M'))