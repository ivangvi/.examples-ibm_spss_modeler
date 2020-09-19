'''
- This py script executes time aggregations for 1 year history of a data source, comprised of 12 separate tables.
- When combined with its corresponding string, it allowed for easy automation of many taks such as adding suffixes to new generated attributes.
- It also generates a log file specifying execution variables and runtime.

In practice it would be used mostly in 2 ways, setting P_FIRST and P_LAST during execution:
	1. For monthly updates, simply set both values to the monthly date.
	2. For reprocessing of several periods, set P_FIRST and P_LAST accordingly.
'''

# Libraries and Modeler API
import datetime
diagram = modeler.script.diagram()
stream  = modeler.script.stream()
p_first = stream.getParameterValue("P_FIRST")
p_last  = stream.getParameterValue("P_LAST")

# Data Directory
direc = "C:/.."

# Periods List
def periods_between(p_first,p_last):
    periods_list = []
    date = datetime.date(int('20' + str(p_first)[0:2]), int(str(p_first)[2:4]), 1)
    iteration = 0
    while True:
        if iteration == 0:
            dxdate = date      
        else:
            dxdate = dxdate - datetime.timedelta(days=1)
            dxdate = dxdate.replace(day=1)
        if len(str(dxdate.month)) == 1:
            dxperiod_month = '0' + str(dxdate.month)
        else:
            dxperiod_month = str(dxdate.month)
        periods_list.insert(iteration, str(dxdate.year)[2:4] + dxperiod_month)
        iteration = iteration + 1
        if (str(dxdate.year)[2:4] + dxperiod_month) == str(p_last):
            break
    return periods_list
periods_list = periods_between(p_first,p_last)

# Renaming on Filter Node
ID_FIELD = "IDFIELD"
renamers = [
    '_3M',
    '_6M',
    '_9M',
    '_12M',
]
for ren in renamers:
    node = diagram.findByType("filternode",ren)
    for field in node.getInputDataModel().nameIterator():
        if field == ID_FIELD:
            pass
        else:
            node.setKeyedPropertyValue("new_name", field, field + ren)

# Rename Aggregations
for period in ('_3M','_6M','_9M','_12M'):
    # DX
    node = diagram.findByType("filternode",'_DX' + period) 
    for field in node.getInputDataModel().nameIterator():
        if field == ID_FIELD:
            pass
        else:
            field_cut = field.replace('_Sum','')
            node.setKeyedPropertyValue("new_name", field, field_cut + '_DX' + period)
    # AVGDX
    node = diagram.findByType("filternode",'_AVGDX' + period) 
    for field in node.getInputDataModel().nameIterator():
        if field == ID_FIELD:
            pass
        else:
            field_cut = field.replace('_Mean_Sum','')
            node.setKeyedPropertyValue("new_name", field, field_cut + '_AVGDX' + period)

# Historia Rename & Run
periods = 12
iteration = 0
for period in periods_list:
    iteration = iteration + 1
    historia_list = []
    date          = datetime.date(int('20'+str(period)[0:2]),int(str(period)[2:4]),1)
    h_iter        = 0
    while True:
        if h_iter == 0:
            dxdate = date      
        else:
            dxdate = dxdate - datetime.timedelta(days=1)
            dxdate = dxdate.replace(day=1)
        dxperiod_year = str(dxdate.year)[2:4]
        if len(str(dxdate.month)) == 1:
            dxperiod_month = '0' + str(dxdate.month)
        else:
            dxperiod_month = str(dxdate.month)
        historia_list.insert(h_iter, str(dxdate.year)[2:4] + dxperiod_month)
        h_iter = h_iter + 1
        if h_iter == periods:
            break   
    # Rename In/Out
    diagram.findByType("statisticsexport","out").setPropertyValue("full_filename", direc + period + "_12M.sav")
    for p in range(periods):
        diagram.findByType("statisticsimport",str(p + 1)).setPropertyValue("full_filename", direc + historia_list[p] +  ".sav")
    # Run Stream
    diagram.findByType("dataaudit","QUALI").run(None)
    diagram.findByType("statisticsexport","out").run(None)