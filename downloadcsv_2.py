# http://omawww.sat.gob.mx/cifras_sat/Paginas/datos/vinculo.html?page=ListCompleta69.html
import urllib.request
import urllib.error
import csv
from datetime import datetime
from datetime import timedelta
from os import path
import time
import locale
import re


def rfc_valido(rfctxt):
    rerfc = '[A-ZÑ&]{3,4}\d{6}[A-V1-9][A-Z1-9][0-9A]'
    validado = re.match(rerfc, rfctxt)

    if validado is None:  # Coincide con el formato general del regex?
        return False
    else:
        return True


def getfile(url, filename):
    dl = False
    gf_i = 0
    print('Getting file...')
    while not dl and gf_i < 10:
        try:
            print('Try #{}'.format(gf_i))
            urllib.request.urlretrieve(url, filename)
            dl = True
            print('Success!')
        except urllib.error.URLError as e:
            print(e.reason, " -- ", url)
            time.sleep(60)
            gf_i += 1


locale.setlocale(locale.LC_ALL, 'Spanish_Mexico')
u = 'http://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69.csv'
f = 'artic69full.csv'
getfile(u, f)
if not path.exists(f):
    raise SystemExit('No se encontró {}'.format(f))

with open(f) as a69csv:
    readCSV = csv.reader(a69csv, delimiter=',')
    # row 0 - Encabezados
    i = 0
    fecha = ''
    headers = ''
    for row in readCSV:
        if i == 0:
            headers = row
            headers[0] = 'id'
            while '' in headers:
                headers.remove('')
        elif i >= 2:
            # print(i, row[1])
            if row[0] is not None and row[0] != '' and rfc_valido(row[0]):
                rfc = row[0]
                r_s = row[1]
                t_p = row[2]
                sup = row[3]
                fmat = '%d/%m/%Y'  # 16/02/2018
                if row[4] is not None and row[4] != '':
                    fepripu = datetime.strptime(row[4], fmat)
                else:
                    fepripu = row[4]
                    print(i)
                monto = row[5]
                if row[6] is not None and row[6] != '':
                    fepu = datetime.strptime(row[6], fmat)
                else:
                    fepu = row[6]
                reg = (i, rfc, r_s, t_p, sup, fepripu, monto, fepu)
                # print(reg)
        i += 1
