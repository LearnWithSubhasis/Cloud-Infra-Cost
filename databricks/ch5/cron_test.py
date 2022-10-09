from cron_descriptor import get_description

def parse_cron(schedule_desc):
  schedule_day_of_week = 'NA'
  schedule_hour_of_day = 0
  schedule_min_of_hour = 0

  if schedule_desc is not None:
    sch_list = schedule_desc.split(" ")
    time = ""
    am_pm = ""
    index = 0
    for index in range(0, len(sch_list)):
      if sch_list[index] == 'At':
        time = sch_list[index+1]
        am_pm = sch_list[index+2]
        time_list = time.split(":")
        schedule_hour_of_day = int(time_list[0])
        schedule_min_of_hour = int(time_list[1])
        if am_pm == 'PM':
          schedule_hour_of_day = schedule_hour_of_day + 12

      elif sch_list[index] == 'on':
        schedule_day_of_week = sch_list[index+1]

      if schedule_hour_of_day >= 0 and schedule_day_of_week == 'NA':
        schedule_day_of_week = 'Everyday'

  return schedule_day_of_week, schedule_hour_of_day, schedule_min_of_hour

quartz_cron_expressions = [
  '0 30 1 ? * 2',
  '0 0 3 ? * 2',
  '0 0 1 ? * 2',
  '13 0 12 ? * Tue',
  '0 0 9 * * ?',
  '50 0 5 ? * Tue'
]
for quartz_cron_expression in quartz_cron_expressions:
  schedule_desc = get_description(quartz_cron_expression)
  print(schedule_desc)
  schedule_day_of_week, schedule_hour_of_day, schedule_min_of_hour = parse_cron(schedule_desc)
  print(schedule_day_of_week, schedule_hour_of_day, schedule_min_of_hour)
  print("----------")





