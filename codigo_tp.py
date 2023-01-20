import pandas as pd
from urllib import request
import boto3


remote_url = 'https://cdn.buenosaires.gob.ar/datosabiertos/datasets/secretaria-para-la-igualdad-de-genero/violencia-de-genero/cantidad-casos-violencia-atendidos.csv'
local_file = 'ruta//donde//se//desea//guardar//el//archivo.csv'

remote_url_2 ='https://cdn.buenosaires.gob.ar/datosabiertos/datasets/direccion-general-de-estadisticas-y-censos/femicidios/vict_fem_annio__g_edad_limpio.csv'
local_file_2 = 'ruta//donde//se//desea//guardar//el//archivo.csv'

remote_url_3 = 'https://cdn.buenosaires.gob.ar/datosabiertos/datasets/direccion-general-de-estadisticas-y-censos/casos-penales-contravencionales/casos_penales_contrav_con_ind_violencia_genero.csv'
local_file_3 = 'ruta//donde//se//desea//guardar//el//archivo.csv''

request.urlretrieve(remote_url, local_file)
request.urlretrieve(remote_url_2, local_file_2)
request.urlretrieve(remote_url_3, local_file_3)

#pd.read_csv --- lee el archivo en formato csv
response1=pd.read_csv('ruta//donde//se//encuentra//el//archivo.csv', sep=';')
response2=pd.read_csv('ruta//donde//se//encuentra//el//archivo.csv', sep=';')
response3=pd.read_csv('ruta//donde//se//encuentra//el//archivo.csv', sep=',', encoding='latin-1')


df_1=pd.DataFrame(response1)
df_1.rename(columns={'Año': 'Year'}, inplace=True)


df_2=pd.DataFrame(response2)
df_2.rename(columns={'2021': 'a_2021',
                        '2020': 'a_2020',
                        '2019': 'a_2019',
                        '2018': 'a_2018',
                        '2017': 'a_2017',
                        '2016': 'a_2016',
                        '2015': 'a_2015'
}, inplace=True)


df_3= pd.DataFrame(response3)
df_3= df_3[(df_3.Comuna !='Sin dato') & (df_3.Comuna != 'Total')]

antiguo_contenido = df_3.columns
nuevo_contenido = []

#El bucle recorre las columnas de la tabla antigua y junta los espacios vacios 
# y los guarda en "nuevo contenido"

for new in antiguo_contenido:    
    nuevo_contenido.append(new.replace(' ',''))

nuevos_c = dict(zip(antiguo_contenido, nuevo_contenido))

df_3.rename(columns=nuevos_c, inplace=True)

df_3.columns

df_3.rename(columns={'Año': 'Year',
                      'Casospenales' : 'casos_penales',
                      'Casoscontravencionales' : 'casos_contrav' }, inplace=True)


df_1.to_csv('archivo1.csv', sep = ';', index=False)
df_2.to_csv('archivo2.csv',sep = ';', index=False)
df_3.to_csv('archivo3.csv', sep = ';', index=False,  encoding='latin-1')
 
 #opcional: cambiar el nombre final para su descarga csv, esto ayuda a diferenciar
 #el csv original de la url y el q fue modificado por un DATAFRAME



 # Crear la sesion con las credenciales de AWS

s3 =boto3.client('s3', aws_access_key_id = 'your_access_key_id',
 aws_secret_access_key= 'your_secret_access_key')


#Nombre del bucket y el archivo y su ubicacion

bucket_name= 'bucket_name'
file_name1 = 'ruta//donde//se//encuentra//el//archivo.csv' 
file_name2 = 'ruta//donde//se//encuentra//el//archivo.csv'
file_name3 = 'ruta//donde//se//encuentra//el//archivo.csv'

s3_file_name1 = 'ruta/de/AmazonS3/donde/se/guarda/el/archivo.csv' #nombre fianl para guardarlo
s3_file_name2 = 'ruta/de/AmazonS3/donde/se/guarda/el/archivo.csv'
s3_file_name3 = 'ruta/de/AmazonS3/donde/se/guarda/el/archivo.csv'


s3.upload_file(file_name1, bucket_name, s3_file_name1)
s3.upload_file(file_name2, bucket_name, s3_file_name2)
s3.upload_file(file_name3, bucket_name, s3_file_name3)

print('Archivo cargado a S3')
