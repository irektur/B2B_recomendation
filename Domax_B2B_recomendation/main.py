from sklearn.neighbors import NearestNeighbors
import sqlalchemy
import numpy as np
import pandas as pd
import pypyodbc as podbc
from sklearn.preprocessing import MinMaxScaler
import pcg_domax_knn.cdm as cdm
import pcg_domax_knn.knn_fit as knn_f

# łączenie z mssql z bazą customers_KNN
cdm.kon_in(cdm.server, cdm.database, cdm.username, cdm.password)
dane_do_knn = cdm.kontrahent_indeksy

# numer kontrahenta dla którego wykonujemy całość
podaj_numer_kon = 20081  ##int(input())

# wywołanie podobnych kontrahentów
knn_f.knn_kon(dane_do_knn, podaj_numer_kon, 10)

# lista kontrahentów podobnych i kontrahent sprawdzany do bazy mssql
cdm.jup_pod_kontrahenci(cdm.server, cdm.database, cdm.username, cdm.password, knn_f.num_ko_dalej)
cdm.jup_kont_glowny(cdm.server, cdm.database, cdm.username, cdm.password, cdm.mod_podaj_numer_kon(podaj_numer_kon))
