from sklearn.neighbors import NearestNeighbors
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import pcg_domax_knn.cdm as cdm

indeks = []
num_ko_dalej = []


def knn_kon (dane_do_knn, podaj_numer_kon, ilu_kontrahentow):
    global indeks
    global num_ko_dalej
    # zamiana indeksów na kolumny z wartością 1 jeżeli sprzedawany i 0 jeżeli nie sprzedawany
    indeks = pd.get_dummies(dane_do_knn['indeks_czesci_uslugi'])
    dane_mod = pd.concat([dane_do_knn, indeks], axis=1)
    dane_mod = dane_mod.drop(['indeks_czesci_uslugi'], axis=1)
    dane_mod = dane_mod.groupby(['numer_kontrahenta']).sum().reset_index()
    dane_do_obliczen = dane_mod.drop(['numer_kontrahenta'], axis=1)

    # przeprowadzenie KNN
    scaler = MinMaxScaler()
    scaler.fit(dane_do_obliczen)
    normalised_data = scaler.transform(dane_do_obliczen)
    klienci_nbrs = NearestNeighbors(n_neighbors=ilu_kontrahentow)
    klienci_nbrs.fit(normalised_data)
    kontrahent = dane_mod['numer_kontrahenta']
    numer_kontrahenta_index = kontrahent.index[kontrahent == podaj_numer_kon].tolist()
    test_index = numer_kontrahenta_index
    distances, indices = klienci_nbrs.kneighbors(normalised_data[test_index])
    indeksy = dane_do_knn.iloc[indices[0]].index.tolist()

    # wyciągnięcie danych z numerami kontrahentów
    Filter_df = cdm.kontrahent_indeksy[cdm.kontrahent_indeksy.index.isin(indeksy)]
    num_ko_dalej = Filter_df['numer_kontrahenta'].astype(int)
    num_ko_dalej = pd.DataFrame(num_ko_dalej.astype(int))