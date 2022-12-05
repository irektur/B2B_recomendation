import cx_Oracle
Host_Name = 
Port_Number = '1521'
s_name = 
user = 
password=

# usunięcie danych z tabeli dla klienta dla którego wykonujeny obliczenia
def ora_krok_1_del(Host_Name,Port_Number,s_name,user,password, podaj_numer_kon):
    dsn_tns = cx_Oracle.makedsn(Host_Name, Port_Number, service_name=s_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
    c = conn.cursor()
    sql = """delete from B2B.DMX_GVY_PROPOZYCJE where NUMER_KONTRAHENTA = (:podaj_numer_kon)"""
    c.execute(sql, [podaj_numer_kon])
    conn.commit()
    c.close()
    conn.close()


# wgranie danych do b2b.DMX_GVY_PROPOZYCJE
def ora_krok_2_insert(Host_Name,Port_Number,s_name,user,password, rekomendacje):
    dsn_tns = cx_Oracle.makedsn(Host_Name, Port_Number, service_name=s_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
    c = conn.cursor()
    sql = """insert into  B2B.DMX_GVY_PROPOZYCJE (INDEKS_CZESCI, RANGA, NUMER_KONTRAHENTA) values (:1,:2,:3)"""
    df_list = rekomendacje.values.tolist()
    n = 0
    for i in rekomendacje.iterrows():
        c.execute(sql, df_list[n])
        n += 1
    conn.commit()
    c.close()
    conn.close()


