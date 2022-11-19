import pandas as pd
import asyncio

import warnings


warnings.filterwarnings('ignore')

def asincrono(funcion):
    
    def eventos(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, funcion, *args, **kwargs)
    
    return eventos


def generador(len):
    na='N/D'
    gen=[]
    for i in range(0,len):
        gen.append(na)
    fila=pd.Series(gen)
    return fila


def anio(len,anio):
    na=anio
    gen=[]
    for i in range(0,len):
        gen.append(na)
    fila=pd.Series(gen)
    return fila

def carrera(len,nom):
    na=nom
    gen=[]
    for i in range(0,len):
        gen.append(na)
    fila=pd.Series(gen)
    return fila

def dif(len):
    na='00:00:00'
    gen=[]
    for i in range(0,len):
        gen.append(na)
    fila=pd.Series(gen)
    return fila

def marca(lista):
    marcas=[]
    for e in lista:
        if 'COLUMBIA' in e:
            marcas.append('COLUMBIA')
        elif 'SALOMON' in e:
            marcas.append('SALOMON')
        elif 'NORTH FACE' in e:
            marcas.append('SALOMON')
        elif 'HOKA' in e:
            marcas.append('HOKA')
        elif 'VIBRAM' in e:
            marcas.append('VIBRAM')
        elif 'ADIDAS' in e:
            marcas.append('ADIDAS')
        elif 'PATAGONIA' in e:
            marcas.append('PATAGONIA')
        elif 'NIKE' in e:
            marcas.append('NIKE')
        elif 'RAIDLIGHT' in e:
            marcas.append('RAIDLIGHT')
        elif 'SPORTIVA' in e:
            marcas.append('SPORTIVA')
        elif 'COMPRESSPORT' in e:
            marcas.append('COMPRESSPORT')
        elif 'ALTRA' in e:
            marcas.append('ALTRA')
        elif 'DYNAFIT' in e:
            marcas.append('DYNAFIT')
        elif 'NEW BALANCE' in e:
            marcas.append('NEW BALANCE')
        elif 'ASICS' in e:
            marcas.append('ASICS')
        elif 'SAUCONY' in e:
            marcas.append('SAUCONY')
        elif 'NNORMAL' in e:
            marcas.append('NNORMAL')
        elif 'INOV' in e:
            marcas.append('INOV')
                
        else:
            marcas.append('N/D')
    return marcas
            
        
        

@asincrono
def revision_idiomas(df_column,lista_cruce):
    lst=(list((filter(lambda idioma: idioma not in df_column,lista_cruce))))
    return lst


@asincrono
def corredores(df_column,lista_cruce):
    lst=(list((filter(lambda corredor: corredor not in df_column,lista_cruce))))
    return lst






def paises(df_column,nom_tabla,nom_colum,nom_colum_dejar):
    '''
    La funcion toma el nomber de un dataframe con la columna que vamos a modificar y la cruza con una columna de una tabla dada
    PARAMS:
    DF_COLUMNS=  Hya que poner el dataframe.columna
    NOM_TABLA= nombre de la tabla a cruzar. En  este caso es paises y nombres
    NOM_COLUM= Es el nombre de la columna de la tabla anterior que vamos a cruzar. En este caso será largo y corto para paises e ingles para nombres
    NOM_COLUM:DEJAR= Es la columna que queremos dejar de la tabla como nuevo valor en el dataframe. En el caso de paises es pais y en el caso de nombres es espagnol
    
    '''
    
    
    for i in range(len(df_column)):
        for j in nom_tabla[nom_colum]:
            if str(j) in df_column[i]:
                df_column[i]= list(nom_tabla.nom_colum_dejar[nom_tabla[nom_colum]==j])[0]
                
                

                
'''
#Normalizamos formato de campos de las columnas añadiendo la configuración final para todas las tablas

blue22.drop(['rango_edad'],axis='columns',inplace=True) #damos de baja columnas

#Generamos las columnas restanetes
blue22['dif']=generador.generador(len(blue22))
blue22['club']=generador.generador(len(blue22))
blue22['pais']=generador.generador(len(blue22))
blue22['carrera']=generador.carrera(len(blue22),'bluetrail')

blue22=blue22.reindex(columns=['posicion','tiempo','dif','nombre','club','pais','sex','cat','carrera']) #Reorganizamos





'''



if __name__=='__main__':
    ...
