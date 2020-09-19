'''
@desc
- Enables to iterate through a list of values, executing a portion of the stream while selecting stream records based on that value, and exporting a different file for each.
- It also generates a log file specifying execution variables and runtime.
@notes
- If source contains _T1, it will take the previous month - values up to 9 are supported.
- Use SAV_ preffiv to indice statistics origin.
'''

# Libraries
import datetime
import modeler.api

# Modeler API
diagram = modeler.script.diagram()
stream  = modeler.script.stream()

# Renames
renamers = [
    ('CLP','T3'),
    ('CLP','T6'),
    ('CLP','T12'),
]
for x in renamers:
    node   = diagram.findByType("filternode",x[0] + '_' + x[1])
    sufijo = '_' + x[1]
    for field in node.getInputDataModel().nameIterator():
        if field == "PENUMPER" or field == "CUS_NO":
            pass
        else:
            newname = field + sufijo
            node.setKeyedPropertyValue("new_name", field, newname)

# Inputs
direc_inp = "C:/.."
direc_out = "C:/.."
out_file  = ".."
fuentes   = [
# if last character..
#   ... is digit, passes as history
#   ... is _norm, has special treatment
#   ... is _, indicates escaping last two branches
# ( 1/0 , source , subdir)
# 1 = OUTPUTS
    (1,"altas_mm_7_3","MMINER 7/ALTAS/"),
    (1,"altas_mm_7_2","MMINER 7/ALTAS/"),
    (1,"altas_mm_7_1","MMINER 7/ALTAS/"),
    (1,"altas_mm_7_","MMINER 7/ALTAS/"),
# 0 = INPUTS
    (0,"OSB_OM_12M","OSB_12M/"),
    (0,"OSA_OM_6M","OSA_6M/"),
    (0,"OS","OS/"),
    (0,"QYR_12M","QYR/"),
    (0,"CLP","CLP/"),
    (0,"CLP_12M","CLP/"),
    (0,"CLP_NORM_T3","CLP/"),
    (0,"CLP_NORM_T6","CLP/"),
    (0,"CLP_NORM_T12","CLP/"),
    (0,"VRZ","VRZ/"),
]

window  = 2 # months between input and output
periods = 12 + window # this is for data sources that rely on past history

outs = [ # products to iterate through
    "PROD1",
    "PROD2",
    "..",
] 

diagram = modeler.script.diagram()
stream  = modeler.script.stream()

p_first      = stream.getParameterValue("P_FIRST")
p_last       = stream.getParameterValue("P_LAST")


# -- generate list of periods to run in batch
periods_list = []
date         = datetime.date(int('20'+str(p_first)[0:2]),int(str(p_first)[2:4]),1)
iteration    = 0
while True:
    if iteration == 0:
        dxdate = date      
    else:
        dxdate = dxdate - datetime.timedelta(days=1)
        dxdate = dxdate.replace(day=1)
    dxperiod_year = str(dxdate.year)[2:4]
    if len(str(dxdate.month)) == 1:
        dxperiod_month = '0' + str(dxdate.month)
    else:
        dxperiod_month = str(dxdate.month)
    periods_list.insert(iteration, str(dxdate.year)[2:4] + dxperiod_month)
    iteration = iteration + 1
    if (str(dxdate.year)[2:4] + dxperiod_month) == str(p_last):
        break

batch_iteration = 0
for period in range(len(periods_list)):
    stream.setParameterValue("mesactual","20" + str(periods_list[period])[0:2] + "-" + str(periods_list[period])[2:4] + "-01")  
# -- generate historia_list for sources with history and for the input/output window
    batch_iteration = batch_iteration + 1
    print ("RUNNING PERIOD " + str(periods_list[period]) + " -- " + str(batch_iteration) + "/" + str(len(periods_list)))
    historia_list = []
    date          = datetime.date(int('20'+str(periods_list[period])[0:2]),int(str(periods_list[period])[2:4]),1)
    iteration     = 0
    while True:
        if iteration == 0:
            dxdate = date      
        else:
            dxdate = dxdate - datetime.timedelta(days=1)
            dxdate = dxdate.replace(day=1)
        dxperiod_year = str(dxdate.year)[2:4]
        if len(str(dxdate.month)) == 1:
            dxperiod_month = '0' + str(dxdate.month)
        else:
            dxperiod_month = str(dxdate.month)
        historia_list.insert(iteration, str(dxdate.year)[2:4] + dxperiod_month)
        iteration = iteration + 1
        if iteration == periods:
            break    
# -- load all sources
    for x in fuentes: 
        es_out = x[0]
        fuente = x[1]
        subdir = x[2]
        exten  = ".sav"
    # delay history if its input, using window
        if es_out == 0: 
            digit = window 
        else:
            digit = 0          
    # if source is history, delay based on last digit
        if fuente[-1].isdigit(): 
            digit  = digit + int(fuente[-1]) 
            fuente = fuente[:-2]
    # suffix based on norm or not
        elif fuente[-5:] == "_norm":
            fuente = fuente[:-5]
            exten  = "_norm" + exten
    # escape the escape
        if fuente[-1] == "_":
            fuente = fuente[:-1]
    # clp_norm T12 exception
        if fuente.startswith("CLP_NORM"):
            fuente = 'CLP_NORM'   
        if x[1] == 'CLP_NORM_T12':
            digit = 12
        print 'Trying ' + x[1] + ' on ' + direc_inp + subdir + fuente + "_" + str(historia_list[digit]) + exten
        diagram.findByType("statisticsimport",x[1]).setPropertyValue("full_filename", direc_inp + subdir + fuente + "_" + str(historia_list[digit]) + exten)
    diagram.findByType("outputfile","out_altas").setPropertyValue("full_filename", direc_inp + "MMINER 7\ALTAS\\ALTAS_7_" + str(periods_list[period]) + "_POST_INV.csv")
    diagram.findByType("outputfile","out_altas").run(None)

# -- iterate
    for x in outs:
        diagram.findByType("outputfile","out").setPropertyValue("full_filename", direc_out + x + "_" + str(periods_list[period]) + ".csv")
        # generate label field
        f = "COMPRA_" + x
        diagram.findByType("derivenode","COMPRA").setPropertyValue("flag_expr", f + " = 1")
        # exceptional rules
        if x == "PAQUETE":
            condition = "TB_BLACK /= 1" # PAQ
        elif x == "SC":
            condition = "PAQ_FLAG = 0 and TB_SC1 = 0" # SC
        else:
            condition = "PENUMPER = PENUMPER" # DUMMY
        diagram.findByType("selectnode","CONDICION").setPropertyValue("condition", condition)

        diagram.findByType("setglobals","T/F COUNT").run(None)

        globals = modeler.script.stream().getGlobalValues()
        T_COUNT = globals.getValue(modeler.api.GlobalValues.Type.SUM, "T_COUNT")
        F_COUNT = T_COUNT * 2
        if F_COUNT < 150000:
            F_COUNT = 150000 - T_COUNT
        diagram.findByType("sample", "F_SAMPLER").setPropertyValue("fixed_counts", F_COUNT)
        diagram.findByType("outputfile","out").run(None)