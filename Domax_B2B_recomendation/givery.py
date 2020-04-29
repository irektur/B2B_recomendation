import pcg_domax_knn.cdm as cdm
import pcg_domax_knn.knn_fit as knn_f
import pcg_domax_knn.GaussianMixture as GM
import pcg_domax_knn.cora as cora
import pandas as pd

def main():
    # numer kontrahenta dla którego wykonujemy całość
    podaj_numer_kon = int(input())

    # łączenie z mssql z bazą customers_KNN
    ## dane do Gaussian Mixture
    cdm.krok_1_f(cdm.server, cdm.database, cdm.username, cdm.password)
    dane_do_GM = cdm.krok_1_dane_GM

    ## wykonanie GM
    GM.GaussianMixture_f (dane_do_GM, podaj_numer_kon)
    kon_do_przekazania = GM.kon_do_przekazania

    ###przekazanie listy kontrahentów z klastra do bazy mssql
    cdm.jup_krok1_klasteryzacja(cdm.server, cdm.database, cdm.username, cdm.password, kon_do_przekazania)

    ## dane do KNN
    cdm.kon_in(cdm.server, cdm.database, cdm.username, cdm.password)
    dane_do_knn = cdm.kontrahent_indeksy

    # wywołanie podobnych kontrahentów
    knn_f.knn_kon(dane_do_knn, podaj_numer_kon, 10)

    # lista kontrahentów podobnych i kontrahent sprawdzany wysyłane do bazy mssql
    cdm.jup_pod_kontrahenci(cdm.server, cdm.database, cdm.username, cdm.password, knn_f.num_ko_dalej)
    cdm.jup_kont_glowny(cdm.server, cdm.database, cdm.username, cdm.password, cdm.mod_podaj_numer_kon(podaj_numer_kon))

    # lista rekomendacji
    cdm.rekomendacje_f(cdm.server, cdm.database, cdm.username, cdm.password)
    cdm.rekomendacje['NUMER_KONTRAHENTA'] = podaj_numer_kon

    # usunięcie danych z b2b.DMX_GVY_PROPOZYCJE
    cora.ora_krok_1_del(cora.Host_Name,cora.Port_Number,cora.s_name,cora.user,cora.password, podaj_numer_kon)

    # insert nowych danych do b2b.DMX_GVY_PROPOZYCJE
    cora.ora_krok_2_insert(cora.Host_Name, cora.Port_Number, cora.s_name, cora.user, cora.password, cdm.rekomendacje)

if __name__ == "__main__":
    main()
