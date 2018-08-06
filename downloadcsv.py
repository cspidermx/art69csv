# http://omawww.sat.gob.mx/cifras_sat/Paginas/datos/vinculo.html?page=ListCompleta69B.html
import urllib.request
import urllib.error
import csv
from datetime import datetime
import locale
import sqlite3


def dbopen(db):
    try:
        con = sqlite3.connect(db)
    except sqlite3.Error as e:
        print(e)
        return None
    return con


def dbclose(con):
    try:
        con.close
    except sqlite3.Error as e:
        print(e)
        return False
    return True


def cleandb(db):
    dbconn = dbopen(db)
    dbconn.execute("Delete from `satart69b`;")
    dbconn.execute("Delete from `info69b`;")
    dbconn.commit()
    dbclose(dbconn)


def checkdb(db):
    dif = 0
    dbconn = dbopen(db)
    r = dbconn.execute("Select * from info69b").fetchall()
    if len(r) > 0:
        fchdb = datetime.strptime(r[0][0], '%Y-%m-%d %H:%M:%S')
        csvfile = open(f)
        readfile = csv.reader(csvfile, delimiter=',')
        rwfch = readfile.__next__()
        fchfile = datetime.strptime(rwfch[0][rwfch[0].find(' al ') + 5:], '%d de %B de %Y')
        csvfile.close()
        dif = fchdb - fchfile
    dbclose(dbconn)
    return dif.total_seconds()


def getfile(url, filename):
    dl = False
    while not dl:
        try:
            urllib.request.urlretrieve(url, filename)
            dl = True
        except urllib.error.URLError as e:
            print(e.reason, " -- ", url)


def getnum(data):
    if data.find('de fecha') != -1:
        return data[:data.find('de fecha') - 1]
    else:
        return data


def dateconvert(dtafch, ln):
    done = False
    fmat = '%d de %B de %Y'
    fechadt = None
    if dtafch.find('de fecha') == -1:
        return None
    while not done:
        try:
            fechadt = datetime.strptime(dtafch[dtafch.find('de fecha')+9:], fmat)
            done = True
        except ValueError as e:
            # print(e, '|', ln, '|', dtafch, '|', dtafch[dtafch.find('de fecha') + 9:])
            if str(e).find('remains') != -1:
                dtafch = dtafch[:-1]
            else:
                if fmat == '%d %B %Y':
                    fmat = '%d de %B del %Y'
                else:
                    fmat = '%d %B %Y'
    return fechadt


locale.setlocale(locale.LC_ALL, 'Spanish_Mexico')
u = 'http://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69-B.csv'
f = 'artic69full.csv'
dbs = 'C:\\Users\\Charly\\Dropbox\\Work\\CFDIs\\art69b.sqlite'  # db = 'E:\\Dropbox\\Dropbox\\Work\\CFDIs\\CFDIs.sqlite'
getfile(u, f)
d = checkdb(dbs)
if d != 0:
    cleandb(dbs)
    dbcon = dbopen(dbs)
    with open(f) as a69csv:
        readCSV = csv.reader(a69csv, delimiter=',')
        # row 0 - Información actualizada al  02 de julio de 2018
        # row 1 - Listado completo de contribuyentes (Artículo 69-B del CFF)
        # row 2 - Encabezados
        i = 0
        fecha = ''
        headers = ''
        for row in readCSV:
            if i == 0:
                fecha = datetime.strptime(row[0][row[0].find(' al ')+5:], '%d de %B de %Y')
            elif i == 2:
                headers = row
                headers[0] = 'id'
                while '' in headers:
                    headers.remove('')
            elif i >= 3:
                nfogpr = row[4]
                nfogdv = row[9]
                nfogdf = row[11]
                nfogsf = row[14]
                nogpr = fogpr = nogdv = fogdv = nogdf = fogdf = nogsf = fogsf = None
                if nfogpr != '':
                    nogpr = getnum(nfogpr)
                    fogpr = dateconvert(nfogpr, row[0])
                if nfogdv != '':
                    nogdv = getnum(nfogdv)
                    fogdv = dateconvert(nfogdv, row[0])
                if nfogdf != '':
                    nogdf = getnum(nfogdf)
                    fogdf = dateconvert(nfogdf, row[0])
                if nfogsf != '':
                    nogsf = getnum(nfogsf)
                    fogsf = dateconvert(nfogsf, row[0])
                reg = (row[1], row[2], row[3], nogpr, fogpr, nogdv, fogdv, nogdf, fogdf, nogsf, fogsf)
                dbcon.execute("INSERT INTO satart69b VALUES(?,?,?,?,?,?,?,?,?,?,?)", reg)
            i += 1
        dbcon.execute("INSERT INTO info69b VALUES(?)", [fecha])
    dbcon.commit()
    dbclose(dbcon)
