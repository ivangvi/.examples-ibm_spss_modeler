'''
- This py script allows to execute an arbitrary range of periods (months, expressed as YYMM) for any stream.
- The original stream is an ETL process to merge several sources, each with it's own csv file.
- Due to massive temp files on disk, certain Big Data streams won't run without checkpoints to break down execution with intermediate cached waypoints. This is the function of every line related to 'breaks'.
- It also generates a log file specifying execution variables and runtime.

In practice it would be used mostly in 2 ways, setting P_FIRST and P_LAST during execution:
	1. For monthly updates, simply set both values to the monthly date.
	2. For reprocessing of several periods, set P_FIRST and P_LAST accordingly.
'''

import datetime 

# Functions
def periods_range(first,last): 
    """Generates periods list between 2 yymm periods."""
    periods_list = []
    date  = datetime.date(int('20'+str(first)[0:2]),int(str(first)[2:4]),1)
    count = 0
    while True:
        if count == 0:
            dxdate = date      
        else:
            dxdate = dxdate - datetime.timedelta(days=1)
            dxdate = dxdate.replace(day=1)
        if len(str(dxdate.month)) == 1:
            dxperiod_month = '0' + str(dxdate.month)
        else:
            dxperiod_month = str(dxdate.month)
        periods_list.insert(count, str(dxdate.year)[2:4] + dxperiod_month)
        if (str(dxdate.year)[2:4] + dxperiod_month) == str(last):
            break
        count = count + 1
    return periods_list

def periods_count(first,periodos):
    """Generates periods list from first to n amounts of periods before."""
    periods_list = []
    date = datetime.date(int('20'+str(first)[0:2]),int(str(first)[2:4]),1)
    for x in range(periodos):
        if x == 0:
            dxdate = date      
        else:
            dxdate = dxdate - datetime.timedelta(days=1)
            dxdate = dxdate.replace(day=1)
        if len(str(dxdate.month)) == 1:
            dxperiod_month = '0' + str(dxdate.month)
        else:
            dxperiod_month = str(dxdate.month)
        periods_list.insert(x, str(dxdate.year)[2:4] + dxperiod_month)
    return periods_list

def source_loader(node,path):
    """Determine type of source node according to source extension."""
    if path[-4:] == '.sav':
        node_type = 'statisticsimport'
    else:
        node_type = 'variablefile'

    diagram.findByType(node_type,node).setPropertyValue("full_filename", path)
    return

def pathfinder(fuente_info,historia_list):
    """
    Transforms source info + periods list into a filepath.
    @fuente_info should be a list of the following elements:
        1) node name
        2) subdir
        3) extension
    @historia_list should input list with predetermined n-periods.
    """
    fuente = fuente_info[0]
    direc  = fuente_info[1]
    extens = fuente_info[2]

    if fuente[-1].isdigit(): # for data sources with history
        digit  = int(fuente.rsplit('_',1)[1]) - 1
    else:
        digit = 0

    path = direc + str(historia_list[digit]) + extens
    return fuente,path

def datetime_now():
    return str(datetime.datetime.now().replace(microsecond=0))

def sufixxer (node_name,excludes): # used to mass rename fields with a sufixx on a filter node, also giving the option to exclude matching fields
    node = diagram.findByType("filternode",node_name)
    for field in node.getInputDataModel().nameIterator():
        if field in excludes:
            pass
        else:
            node.setKeyedPropertyValue("new_name", field, field + node_name)

# User Inputs
direc_out = "C:..."
direc_qua = "C:..."
h_months  = 1 
breaks    = 6
fuentes   = [
    ("iv"                      , "C:/..", ""),
    ("tb_clientes"             , "C:/..", ""),
    ("sucus"                   , "C:/..", ""),
    ("tc_limite"               , "C:/..", ""),
    ("ingreso"                 , "C:/..", ""),
    ("disponible_paquetes"     , "C:/..", ""),
    ("resultado_test_inversor" , "C:/..", ""),
]
excludes = [
    'id_field_1',
    'id_field_2',
]
suffixes = [
    '_3M',
    '_6M',
    '_12M',
]

# Run
diagram = modeler.script.diagram()
stream  = modeler.script.stream()
p_first = stream.getParameterValue("P_FIRST")
p_last  = stream.getParameterValue("P_LAST")

for s in suffixes:
    sufixxer(s,excludes)

# Periods Run
periods_list = periods_range(p_first,p_last)
for x in periods_list:
    log_dir = direc_out + x + '_log.txt'
    diagram.findByType('outputfile','log_save').setPropertyValue("full_filename", log_dir)
    diagram.findByType('userinput','log_create').setPropertyValue("names", ['-- LOG FILE --'])
    diagram.findByType('userinput','log_create').setKeyedPropertyValue("custom_storage", '-- LOG FILE --', "String")
    logstr = ""
    
    loginp = 'Running period ' + x + ' at ' + datetime_now() + '..'
    logstr = logstr + '"' + loginp + '" '
    mesactual = datetime.date(int('20' + x[0:2]),int(x[2:4]),1)
    stream.setParameterValue("mesactual",mesactual)
    loginp = 'Stream parameter mesactual set to ' + str(mesactual) + '..'
    logstr = logstr + '"' + loginp + '" '

    historia_list = periods_count(x,h_months)

    for y in fuentes:
        fuente,path = pathfinder(y,historia_list)
        source_loader(fuente,path)
        loginp = 'Source node ' + y[0] + ' set to ' + path + '..'
        logstr = logstr + '"' + loginp + '" '
    diagram.findByType('spssexport','out').setPropertyValue("full_filename", direc_out + 'CLP_' + x + '.sav')
    diagram.findByType('outputfile','out_q').setPropertyValue("full_filename", direc_qua + 'CLP_' + x + '.csv')
    loginp = 'Export node out set to ' + direc_out + x + '.sav' + '..'
    logstr = logstr + '"' + loginp + '" '    

    for y in range(breaks): # execute break points
        y = str(y + 1)
        diagram.findByType('setglobals','b' + y).run(None)
    loginp = 'Executed remaining breaks..'
    logstr = logstr + '"' + loginp + '" ' 
    
    diagram.findByType('spssexport','out').run(None)

    loginp = 'Executed QUALI and out..'
    logstr = logstr + '"' + loginp + '" '    
    loginp = 'Finished at ' + datetime_now() + '! Have a cookie.'
    logstr = logstr + '"' + loginp + '"' # last one, no space :)
    
    diagram.findByType('userinput','log_create').setKeyedPropertyValue("data", '-- LOG FILE --', logstr)
    diagram.findByType('table','log_table').run(None)
    diagram.findByType('outputfile','log_save').run(None)