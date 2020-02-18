## connection to domax mssql
import pypyodbc as podbc
import pandas as pd

kontrahent_indeksy = []
server = r'tcp:192.168.0.201'
database = 'customers_KNN'
username = 'KNN'
password = 'aa@knN1'


## czytaj tabelę customers_KNN.dbo.kontrahent_indeksy i zwróć do zmiennej kontrahent_indeksy
def kon_in(server, database, username, password):
    cnxn = podbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()

    global kontrahent_indeksy
    kontrahent_indeksy = pd.read_sql_query(
        '''SELECT distinct numer_kontrahenta, indeks_czesci_uslugi FROM customers_KNN.dbo.kontrahent_indeksy 
        where numer_kontrahenta > 9''', cnxn)
    cnxn.close()


## podaj do mssql do tabeli  jup_pod_kontrahenci listę kontrahentów, którzy zostali uznani za podobnych
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


## podaj do mssql do tabeli  jup_pod_kontrahenci listę kontrahentów, którzy zostali uznani za podobnych
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
