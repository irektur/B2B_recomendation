from sklearn.mixture import GaussianMixture
import pandas as pd

kon_do_przekazania = []

def GaussianMixture_f (dane_do_GM, podaj_numer_kon):
    global kon_do_przekazania
    CZY_KLIENT_B2B = pd.get_dummies(dane_do_GM['czy_klient_b2b'])
    ZRODLO_DANYCH = pd.get_dummies(dane_do_GM['zrodlo_danych'])
    KRAJEXPORT = pd.get_dummies(dane_do_GM['krajexport'])
    TYP = pd.get_dummies(dane_do_GM['typ'])
    STATUS = pd.get_dummies(dane_do_GM['status'])

    dane_mod = pd.concat([dane_do_GM, CZY_KLIENT_B2B, ZRODLO_DANYCH, TYP, STATUS, KRAJEXPORT], axis=1)
    dane_mod = dane_mod.drop(['czy_klient_b2b', 'zrodlo_danych', 'typ', 'status', 'krajexport'], axis=1)
    dane_mod = dane_mod.groupby(['numer_kontrahenta']).sum().reset_index()
   # dane_do_obliczen = dane_mod.drop(['numer_kontrahenta'], axis=1)

    gmm = GaussianMixture(n_components=10, random_state=0).fit(dane_mod)
    gmm_target = gmm.predict(dane_mod)

    kontrahent = dane_mod['numer_kontrahenta']
    numer_kontrahenta_index = kontrahent.index[kontrahent == podaj_numer_kon].tolist()
    test_index = numer_kontrahenta_index
    prediction_label = gmm.predict(dane_mod.iloc[test_index])

    df_gmm_target = pd.DataFrame(data=gmm_target[0:],  index=gmm_target[0:], columns=gmm_target[:1])
    df_gmm_target = df_gmm_target.reset_index().drop(['index'], axis=1)
    df_gmm_target.rename(columns={ df_gmm_target.columns[0]: "GRUPA" }, inplace = True)

    kon_do_przekazania = dane_mod[['numer_kontrahenta']]
    kon_do_przekazania =  pd.concat([kon_do_przekazania, df_gmm_target],  axis=1, sort=False)
    kon_do_przekazania = kon_do_przekazania.loc[kon_do_przekazania['GRUPA'] == int(prediction_label[:1])]
    kon_do_przekazania = kon_do_przekazania[['numer_kontrahenta']]

