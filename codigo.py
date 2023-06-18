import pandas as pd
df = pd.read_csv('datosrefinados.csv',)
df.index = df.index + 1
columnas_a = ['L0rsrs', 'L1rsrs', 'L2rsrs', 'L3rsrs', 'Mrsrs', 'Nrsrs']
columnas_b = ['L0rsrsnt', 'L1rsrsnt', 'L2rsrsnt', 'L3rsrsnt', 'Mrsrsnt', 'Nrsrsnt']
columnas_c = ['L0rcrs', 'L1rcrs', 'L2rcrs', 'L3rcrs', 'Mrcrs', 'Nrcrs']
columnas_d = ['L0rcrsnt', 'L1rcrsnt', 'L2rcrsnt', 'L3rcrsnt', 'Mrcrsnt', 'Nrcrsnt']


# Alineamiento de RSRS con respecto al index
for columna_a, columna_b in zip(columnas_a, columnas_b):

    # Crear una nueva columna alineada para el par de columnas actual
    columna_alineada = columna_a + '_' + columna_b + '_Alineada'

    # Inicializar la columna alineada con valores vacíos
    df[columna_alineada] = ''

    # Iterar sobre los valores de la columna "a" y escribir los valores correspondientes de la columna "b" en las celdas correspondientes
    for valor_a, valor_b in zip(df[columna_a], df[columna_b]):
        if valor_a in df.index:
            df.at[valor_a, columna_alineada] = str(int(valor_a)) + valor_b

# Alineamiento de rCRS con respecto al index
for columna_c, columna_d in zip(columnas_c, columnas_d):

    # Crear una nueva columna alineada para el par de columnas actual
    columna_alineada1 = columna_c + '_' + columna_d + '_Alineada'

    # Inicializar la columna alineada con valores vacíos
    df[columna_alineada1] = ''

    # Iterar sobre los valores de la columna "a" y escribir los valores correspondientes de la columna "b" en las celdas correspondientes
    for valor_c, valor_d in zip(df[columna_c], df[columna_d]):
        if valor_c in df.index:
            df.at[valor_c, columna_alineada1] = str(int(valor_c)) + valor_d


#Covert all dataframe f to string
df = df.applymap(str)
df=df.mask(df == '')


#se crea filtro comparando RSR y L0rsrs_L0rsrsnt_Alineada
#separamos la base de la polimorfismo
a = df[['rCRS','RSRS','L0rcrs_L0rcrsnt_Alineada','L0rsrs_L0rsrsnt_Alineada']]
a['L0rcrs_L0rcrsnt_AlineadaS'] = a['L0rcrs_L0rcrsnt_Alineada'].str.extract(r'([\d]+)')
a['L0rcrs_L0rcrsnt_AlineadaT'] = a['L0rcrs_L0rcrsnt_Alineada'].str.strip().str[-1]
a['L0rsrs_L0rsrsnt_AlineadaS'] = a['L0rsrs_L0rsrsnt_Alineada'].str.extract(r'([\d]+)')
a['L0rsrs_L0rsrsnt_AlineadaT'] = a['L0rsrs_L0rsrsnt_Alineada'].str.strip().str[-1]
a = a[['rCRS', 'RSRS', 'L0rcrs_L0rcrsnt_AlineadaS', 'L0rcrs_L0rcrsnt_AlineadaT', 'L0rsrs_L0rsrsnt_AlineadaS', 'L0rsrs_L0rsrsnt_AlineadaT']]
a = a[['rCRS', 'RSRS', 'L0rcrs_L0rcrsnt_AlineadaS', 'L0rcrs_L0rcrsnt_AlineadaT', 'L0rsrs_L0rsrsnt_AlineadaS', 'L0rsrs_L0rsrsnt_AlineadaT']]
a = a.dropna(subset= ['L0rcrs_L0rcrsnt_AlineadaS', 'L0rcrs_L0rcrsnt_AlineadaT', 'L0rsrs_L0rsrsnt_AlineadaS', 'L0rsrs_L0rsrsnt_AlineadaT'], how='all')
a = a.mask(a == '')
a = a.applymap(str)


#create b
b = df[['rCRS','RSRS','L1rcrs_L1rcrsnt_Alineada','L1rsrs_L1rsrsnt_Alineada']]
b['L1rcrs_L1rcrsnt_AlineadaS'] = b['L1rcrs_L1rcrsnt_Alineada'].str.extract(r'([\d]+)')
b['L1rcrs_L1rcrsnt_AlineadaT'] = b['L1rcrs_L1rcrsnt_Alineada'].str.strip().str[-1]
b['L1rsrs_L1rsrsnt_AlineadaS'] = b['L1rsrs_L1rsrsnt_Alineada'].str.extract(r'([\d]+)')
b['L1rsrs_L1rsrsnt_AlineadaT'] = b['L1rsrs_L1rsrsnt_Alineada'].str.strip().str[-1]
b = b[['rCRS', 'RSRS', 'L1rcrs_L1rcrsnt_AlineadaS', 'L1rcrs_L1rcrsnt_AlineadaT', 'L1rsrs_L1rsrsnt_AlineadaS', 'L1rsrs_L1rsrsnt_AlineadaT']]
b = b[['rCRS', 'RSRS', 'L1rcrs_L1rcrsnt_AlineadaS', 'L1rcrs_L1rcrsnt_AlineadaT', 'L1rsrs_L1rsrsnt_AlineadaS', 'L1rsrs_L1rsrsnt_AlineadaT']]
b = b.dropna(subset= ['L1rcrs_L1rcrsnt_AlineadaS', 'L1rcrs_L1rcrsnt_AlineadaT', 'L1rsrs_L1rsrsnt_AlineadaS', 'L1rsrs_L1rsrsnt_AlineadaT'], how='all')
b = b.mask(b == '')
b = b.applymap(str)

#create c
c = df[['rCRS','RSRS','L2rcrs_L2rcrsnt_Alineada','L2rsrs_L2rsrsnt_Alineada']]
c['L2rcrs_L2rcrsnt_AlineadaS'] = c['L2rcrs_L2rcrsnt_Alineada'].str.extract(r'([\d]+)')
c['L2rcrs_L2rcrsnt_AlineadaT'] = c['L2rcrs_L2rcrsnt_Alineada'].str.strip().str[-1]
c['L2rsrs_L2rsrsnt_AlineadaS'] = c['L2rsrs_L2rsrsnt_Alineada'].str.extract(r'([\d]+)')
c['L2rsrs_L2rsrsnt_AlineadaT'] = c['L2rsrs_L2rsrsnt_Alineada'].str.strip().str[-1]
c = c[['rCRS', 'RSRS', 'L2rcrs_L2rcrsnt_AlineadaS', 'L2rcrs_L2rcrsnt_AlineadaT', 'L2rsrs_L2rsrsnt_AlineadaS', 'L2rsrs_L2rsrsnt_AlineadaT']]
c = c[['rCRS', 'RSRS', 'L2rcrs_L2rcrsnt_AlineadaS', 'L2rcrs_L2rcrsnt_AlineadaT', 'L2rsrs_L2rsrsnt_AlineadaS', 'L2rsrs_L2rsrsnt_AlineadaT']]
c = c.dropna(subset= ['L2rcrs_L2rcrsnt_AlineadaS', 'L2rcrs_L2rcrsnt_AlineadaT', 'L2rsrs_L2rsrsnt_AlineadaS', 'L2rsrs_L2rsrsnt_AlineadaT'], how='all')
c = c.mask(c == '')
c = c.applymap(str)

#create d
d = df[['rCRS','RSRS','L3rcrs_L3rcrsnt_Alineada','L3rsrs_L3rsrsnt_Alineada']]
d['L3rcrs_L3rcrsnt_AlineadaS'] = d['L3rcrs_L3rcrsnt_Alineada'].str.extract(r'([\d]+)')
d['L3rcrs_L3rcrsnt_AlineadaT'] = d['L3rcrs_L3rcrsnt_Alineada'].str.strip().str[-1]
d['L3rsrs_L3rsrsnt_AlineadaS'] = d['L3rsrs_L3rsrsnt_Alineada'].str.extract(r'([\d]+)')
d['L3rsrs_L3rsrsnt_AlineadaT'] = d['L3rsrs_L3rsrsnt_Alineada'].str.strip().str[-1]
d = d[['rCRS', 'RSRS', 'L3rcrs_L3rcrsnt_AlineadaS', 'L3rcrs_L3rcrsnt_AlineadaT', 'L3rsrs_L3rsrsnt_AlineadaS', 'L3rsrs_L3rsrsnt_AlineadaT']]
d = d.dropna(subset= ['L3rcrs_L3rcrsnt_AlineadaS', 'L3rcrs_L3rcrsnt_AlineadaT', 'L3rsrs_L3rsrsnt_AlineadaS', 'L3rsrs_L3rsrsnt_AlineadaT'], how='all')
d = d.mask(d == '')
d = d.applymap(str)


#create e
e = df[['rCRS','RSRS','Mrcrs_Mrcrsnt_Alineada','Mrsrs_Mrsrsnt_Alineada']]
e['Mrcrs_Mrcrsnt_AlineadaS'] = e['Mrcrs_Mrcrsnt_Alineada'].str.extract(r'([\d]+)')
e['Mrcrs_Mrcrsnt_AlineadaT'] = e['Mrcrs_Mrcrsnt_Alineada'].str.strip().str[-1]
e['Mrsrs_Mrsrsnt_AlineadaS'] = e['Mrsrs_Mrsrsnt_Alineada'].str.extract(r'([\d]+)')
e['Mrsrs_Mrsrsnt_AlineadaT'] = e['Mrsrs_Mrsrsnt_Alineada'].str.strip().str[-1]
e = e[['rCRS', 'RSRS', 'Mrcrs_Mrcrsnt_AlineadaS', 'Mrcrs_Mrcrsnt_AlineadaT', 'Mrsrs_Mrsrsnt_AlineadaS', 'Mrsrs_Mrsrsnt_AlineadaT']]
e = e.dropna(subset= ['Mrcrs_Mrcrsnt_AlineadaS', 'Mrcrs_Mrcrsnt_AlineadaT', 'Mrsrs_Mrsrsnt_AlineadaS', 'Mrsrs_Mrsrsnt_AlineadaT'], how='all')
e = e.mask(e == '')
e = e.applymap(str)



#create f
f = df[['rCRS','RSRS','Nrcrs_Nrcrsnt_Alineada','Nrsrs_Nrsrsnt_Alineada']]
f['Nrcrs_Nrcrsnt_AlineadaS'] = f['Nrcrs_Nrcrsnt_Alineada'].str.extract(r'([\d]+)')
f['Nrcrs_Nrcrsnt_AlineadaT'] = f['Nrcrs_Nrcrsnt_Alineada'].str.strip().str[-1]
f['Nrsrs_Nrsrsnt_AlineadaS'] = f['Nrsrs_Nrsrsnt_Alineada'].str.extract(r'([\d]+)')
f['Nrsrs_Nrsrsnt_AlineadaT'] = f['Nrsrs_Nrsrsnt_Alineada'].str.strip().str[-1]
f = f[['rCRS', 'RSRS', 'Nrcrs_Nrcrsnt_AlineadaS', 'Nrcrs_Nrcrsnt_AlineadaT', 'Nrsrs_Nrsrsnt_AlineadaS', 'Nrsrs_Nrsrsnt_AlineadaT']]
f = f[['rCRS', 'RSRS', 'Nrcrs_Nrcrsnt_AlineadaS', 'Nrcrs_Nrcrsnt_AlineadaT', 'Nrsrs_Nrsrsnt_AlineadaS', 'Nrsrs_Nrsrsnt_AlineadaT']]
f = f.dropna(subset= ['Nrcrs_Nrcrsnt_AlineadaS', 'Nrcrs_Nrcrsnt_AlineadaT', 'Nrsrs_Nrsrsnt_AlineadaS', 'Nrsrs_Nrsrsnt_AlineadaT'], how='all')
f = f.mask(f == '')
f = f.applymap(str)




L0inv = []
L1inv = []
L2inv = []
L3inv = []
Minv = []
Ninv = []

L0cha = []
L1cha = []
L2cha = []
L3cha = []
Mcha = []
Ncha = []

L0del = []
L1del = []
L2del = []
L3del = []
Mdel = []
Ndel = []

L0new = []
L1new = []
L2new = []
L3new = []
Mnew = []
Nnew = []

#Define a funtion that will compare each nucleotides respecting the index of the row (pos_left), and will store them in their respective list
#funcion para A
def comparar(row, lista_inv, lista_cha, lista_del, lista_new):
    if row[2] == row[4] and row[0] == row[1]:
        lista_inv.append(row[1] + row[4] + row[5])
    elif row[2] == row[4] and (row[0] != row[1]) and (row[0] != row[5]):
        lista_cha.append(row[1] + row[4] + row[5])
    elif row[2] != row[4] and row[4] == 'nan' and (row[1] == row[3]):
        lista_del.append(row[0] + row[2] + row[3])
    elif row[4] != row[2] and row[2] == 'nan' and (row[1] != row[5]):
        lista_new.append(row[1] + row[4] + row[5])

a.apply(comparar, args= (L0inv,L0cha,L0del,L0new), axis = 1)
b.apply(comparar, args= (L1inv,L1cha,L1del,L1new), axis = 1)
c.apply(comparar, args= (L2inv,L2cha,L2del,L2new), axis = 1)
d.apply(comparar, args= (L3inv,L3cha,L3del,L3new), axis = 1)
e.apply(comparar, args= (Minv,Mcha,Mdel,Mnew), axis = 1)
f.apply(comparar, args= (Ninv,Ncha,Ndel,Nnew), axis = 1)

import sys # Exporta el Output en un archivo .txt
Output_polimorfismos = "Output_polimorfismos.txt"
sys.stdout = open(nombre_archivo, 'w')

#print INV
print('Los siguientes poliformismos se mantienen invariables sin distingo de la secuencia de referencia usada:\n', 'L0: ', L0inv,'\n', 'L1: ', L1inv, '\n', 'L2: ', L2inv,'\n', 'L3: ', L3inv,'\n', 'M: ', Minv,'\n', 'N: ', Ninv,'\n')
#print CHA
print('Los siguientes poliformismos cambian al usar RSRS como secuencia de referencia:\n', 'L0: ', L0cha,'\n', 'L1: ', L1cha, '\n', 'L2: ', L2cha,'\n', 'L3: ', L3cha,'\n', 'M: ', Mcha,'\n', 'N: ', Ncha,'\n')
#print del
print('Los siguientes poliformismos \(tipificados con respecto a rCRS\) desaparecen al usar RSRS como secuencia de referencia:\n', 'L0: ', L0del,'\n', 'L1: ', L1del, '\n', 'L2: ', L2del,'\n', 'L3: ', L3del,'\n', 'M: ', Mdel,'\n', 'N: ', Ndel,'\n')
#print new
print('Se plantean los siguientes posibles nuevos polimorfismos al usar RSRS como secuencia de referencia :\n', 'L0: ', L0new,'\n', 'L1: ', L1del, '\n', 'L2: ', L2new,'\n', 'L3: ', L3new,'\n', 'M: ', Mnew,'\n', 'N: ', Nnew,'\n')

sys.stdout.close()
sys.stdout = sys.__stdout__

print("Archivo generado exitosamente: " + Output_polimorfismos)
