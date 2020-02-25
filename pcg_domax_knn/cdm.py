# connection to domax mssql
import pypyodbc as podbc
import pandas as pd

kontrahent_indeksy = []
krok_1_dane_GM = []
rekomendacje = []
server = r'tcp:192.168.0.201'
database = 'customers_KNN'
username = 'KNN'
password = 'aa@knN1'


# czytaj tabelę customers_KNN.dbo.krok_1_klasteryzacja i zwróć do zmiennej
def krok_1_f(server, database, username, password):
    cnxnn = podbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxnn.cursor()

    global krok_1_dane_GM
    krok_1_dane_GM = pd.read_sql_query(
        '''SELECT NUMER_KONTRAHENTA ,CZY_KLIENT_B2B ,ZRODLO_DANYCH ,KRAJEXPORT ,typ ,STATUS 
             FROM customers_KNN.dbo.krok_1_klasteryzacja''', cnxnn)
    cnxnn.close()


# podaj do mssql do tabeli  jup_krok1_klasteryzacja listę kontrahentów, którzy zostali uznani za podobnych
def jup_krok1_klasteryzacja(server, database, username, password, kon_do_przekazania):
    cnxn = podbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cur = cnxn.cursor()
    cur.execute("truncate table customers_KNN.dbo.jup_krok1_klasteryzacja")
    crsr = cnxn.cursor()
    crsr.fast_executemany = True
    crsr.executemany(
        "INSERT INTO customers_KNN.dbo.jup_krok1_klasteryzacja (NUMER_KONTRAHENTA) VALUES (?)",
        list(kon_do_przekazania.itertuples(index=False, name=None))
    )
    cnxn.commit()
    cnxn.close()


# czytaj tabelę customers_KNN.dbo.kontrahent_indeksy i zwróć do zmiennej kontrahent_indeksy
def kon_in(server, database, username, password):
    cnxn = podbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()

    global kontrahent_indeksy
    kontrahent_indeksy = pd.read_sql_query(
        '''SELECT distinct numer_kontrahenta, indeks_czesci_uslugi FROM customers_KNN.dbo.kontrahent_indeksy 
        where numer_kontrahenta > 9
		and numer_kontrahenta in (select numer_kontrahenta from customers_KNN.dbo.jup_krok1_klasteryzacja)''', cnxn)
    cnxn.close()


# podaj do mssql do tabeli  jup_pod_kontrahenci listę kontrahentów, którzy zostali uznani za podobnych
def jup_pod_kontrahenci(server, database, username, password, num_ko_dalej):
    cnxn = podbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cur = cnxn.cursor()
    cur.execute("truncate table customers_KNN.dbo.jup_pod_kontrahenci")
    crsr = cnxn.cursor()
    crsr.fast_executemany = True
    crsr.executemany(
        "INSERT INTO customers_KNN.dbo.jup_pod_kontrahenci (NUMER_KONTRAHENTA) VALUES (?)",
        list(num_ko_dalej.itertuples(index=False, name=None))
    )
    cnxn.commit()
    cnxn.close()


# podaj do mssql do tabeli  jup_pod_kontrahenci listę kontrahentów, którzy zostali uznani za podobnych
def jup_kont_glowny(server, database, username, password, podaj_numer_kon):
    cnxn = podbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cur = cnxn.cursor()
    cur.execute("truncate table customers_KNN.dbo.jup_kont_glowny")
    crsr = cnxn.cursor()
    crsr.fast_executemany = True
    crsr.executemany(
        "INSERT INTO customers_KNN.dbo.jup_kont_glowny (NUMER_KONTRAHENTA) VALUES (?)",
        list(podaj_numer_kon.itertuples(index=False, name=None))
    )
    cnxn.commit()
    cnxn.close()


# mod podaj_numer_kon na DataFrame aby podać do bazy mssql tabeli jup_kont_glowny
def mod_podaj_numer_kon(podaj_numer_kon):
    num_kon_insert = pd.DataFrame(columns=['Numer_kontrahenta'])
    num_kon_insert.loc[-1] = podaj_numer_kon
    return num_kon_insert


# czytaj widok customers_KNN.dbo.rekomendacje_podobnych
def rekomendacje_f(server, database, username, password):
    global rekomendacje
    cnxnn = podbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxnn.cursor()

    global krok_1_dane_GM
    rekomendacje = pd.read_sql_query(
                '''SELECT MARKA_HANDLOWA
              ,typ
              ,WL_NAZWA_SKR
              ,INDEKS_CZESCI_USLUGI
              ,NAZWA
              ,ile_razy_detal
              ,rn
          FROM customers_KNN.dbo.rekomendacje_podobnych''', cnxnn)
    cnxnn.close()
