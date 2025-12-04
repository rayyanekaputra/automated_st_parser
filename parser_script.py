import pandas as pd # type: ignore
import numpy as np
import os
### Drive: Import Total Inventory

file_drive = "./" + input("Masukkan Drive .CSV (misal: TOTAL INV 29 NOVEMBER.csv): ")

if os.path.exists(file_drive):
    print("File exists")
else:
    print("File does not exist")

file_CNW = "./" + input("Masukkan CNW .CSV (misal: DailyReport_20251130175512.csv): ")

if os.path.exists(file_CNW):
    print("File exists")
else:
    print("File does not exist")

ds = pd.read_csv(file_drive, sep=",")
ds = ds.iloc[:,:-2]
ds.drop(columns=["NO"], inplace=True)
ds.head(2)
#### Bersihkan (,)
ds = ds[~ds['ITEM NAME'].isna()]
ds['INVTR'] = ds['INVTR'].str.replace(pat=",",repl="",regex=False)
ds['SELISIH'] = ds['SELISIH'].str.replace(pat=",",repl="",regex=False)

ds[ds['SELISIH'] == "#VALUE!"]
#### Konversi jadi float
ds.astype(dtype = {
    "ITEM CODE": "object",
    "ITEM NAME": "object",
    "UOM": "object",
    "GROUP": "object",
    "GUD1": "float64",
    "GUD2": "float64",
    "GUD3": "float64",
    "GUD4": "float64",
    "GUD5": "float64",
    "GUD6": "float64",
    "GUD7": "float64",
    "DRY": "float64",
    "BUTCHER": "float64",
    "KIMA3": "float64",
    "INVTR": "float64",
    "QTY": "float64",
    "SELISIH": "float64"
})
### Drive: Check Berapa Banyak Item yang Tidak Kosong Sama Sekali
ds_gudang = ds.iloc[:,4:14]
ds_gudang
df_cek_nol = (ds_gudang==0.0).all(axis="columns")
df_cek_nol
ds_bukan_nol = ds.loc[~df_cek_nol]
ds_bukan_nol.head(10)
print(f"Item yang tidak kosong sama sekali ada: {len(ds_bukan_nol)}")
### CNW: Data CNW
ds2 = pd.read_csv(file_CNW, sep=',')
ds2.head(2)
ds2.columns
ds2_adj_negatif = ds2[ds2['AdjustmentQty'] < 0]
ds2_adj_negatif.head(2)
ds2_adj_positif = ds2[ds2['AdjustmentQty'] >= 0]
ds2_adj_positif.head(2)
ds_bukan_nol.columns
### Join: Gabungkan CNW dan Drive
gudang_cols = []
for x in ds_gudang.columns:
    gudang_cols.append(x)
print(gudang_cols)
ds_total = ds.drop(labels=gudang_cols, axis = "columns")
ds_total.head(2)
ds_merge = ds2.merge(ds_total, left_on=['ITEMCODE', 'ITEMNAME'], right_on=['ITEM CODE', 'ITEM NAME'])
ds_merge.drop(columns=['ITEM CODE', 'ITEM NAME', 'UOM','GROUP'], inplace = True) 
ds_merge.head(1)
### Move: INVTR, QTY, SELISIH
ds_sementara = ds_merge[['INVTR', 'QTY', 'SELISIH']]
ds_merge.drop(columns=['INVTR', 'QTY', 'SELISIH'], inplace = True)
### Add: New Columns StokIn, StokOut, EndingBalanceSeharusnya
ds_merge[['StokIn','StokOut','EndingBalanceSeharusnya']] = 0
ds_merge[['INVTR', 'QTY', 'SELISIH']] = ds_sementara
ds_merge.columns

print("Hitung StokIn dan StokOut..")
ds_stok_in =  ds_merge[(ds_merge['AdjustmentQty'] > 0) & (ds_merge['SAPOpnameQty'] > 0)].copy()
ds_stok_out = ds_merge[(ds_merge['AdjustmentQty'] < 0) & (ds_merge['SAPOpnameQty'] < 0)].copy()
ds_stok_in['ITEMCODE'].isin(ds_stok_out['ITEMCODE']).astype('string') #Check kalau baku
print(ds_stok_in.info())
print(ds_stok_out.info())
#Check baku timpa
ds_stok_in[ds_stok_in['ITEMCODE'].isin(ds_stok_out['ITEMCODE'])]['AdjustmentQty']
ds_stok_in['StokIn'] = ds_stok_in['PembelianQty'] +  ds_stok_in['AdjustmentQty'] +  ds_stok_in['SAPOpnameQty']
ds_stok_out['StokOut'] = ds_stok_out['PenjualanQty'] +  ds_stok_out['AdjustmentQty'] +  ds_stok_out['SAPOpnameQty']
ds_stok_out.columns
ds_stok_out.columns == ds_stok_in.columns
list_columns_merge = ds_stok_out.columns.tolist()
ds_merge.dtypes
#Alternatives karena merging nilai float bisa dibulatkan dan tidak akurat.
ds_stok_in = ds_stok_in.astype(dtype='str')
ds_stok_out = ds_stok_out.astype(dtype='str')
print("Merging..")
ds_merge = ds_merge.astype(dtype='str')
ds_merge.merge(right=ds_stok_in, how='left', left_on=list_columns_merge, right_on=list_columns_merge)
ds_merge.merge(right=ds_stok_out, how='left', left_on=list_columns_merge, right_on=list_columns_merge)
ds_merge = ds_merge.astype({
    "Company":                     "object",
    "DateFrom":                    "int64",
    "Dateto":                      "int64",
    "u_GROUP":                     "object",
    "U_SUBGROUP":                  "object",
    "ITEMCODE":                    "object",
    "ITEMNAME":                    "object",
    "OpeningBalanceQty":           "float64",
    "OpeningBalanceAmt":           "float64",
    "PembelianQty":                "float64",
    "PembelianAmt":                "float64",
    "PenjualanQty":                "float64",
    "PenjualanAmt":                "float64",
    "InventoryTransferQty":        "float64",
    "InventoryTransferAmt":        "float64",
    "AdjustmentQty":               "float64",
    "AdjustmentAmt":               "float64",
    "SAPOpnameQty":                "float64",
    "SAPOpnameAmt":                "float64",
    "EndingBalanceQty":            "float64",
    "EndingBalanceAmt":            "float64",
    "StokIn":                      "float64",
    "StokOut":                     "float64",
    "EndingBalanceSeharusnya":     "float64",
    "INVTR":                       "float64",
    "QTY":                         "float64",
    "SELISIH":                     "float64"
})
ds_merge.dtypes
print("Menghitung EndingBalance..")
ds_merge['EndingBalanceSeharusnya'] = ds_merge['OpeningBalanceQty'] + ds_merge['StokIn'] + ds_merge['StokOut']
ds_merge.sample(n=2)

print("Menghitung Check EB = Sistem..")
#### Checker EndingBalance = Sistem
ds_merge['Check EB = Sistem'] = ds_merge['EndingBalanceSeharusnya'] == ds_merge ['INVTR']
ds_merge['Check EB = Sistem'] = ds_merge['Check EB = Sistem'].astype(str)
ds_merge['Check EB = Sistem'] = ds_merge['Check EB = Sistem'].replace(to_replace="True", value="Sama")
ds_merge['Check EB = Sistem'] = ds_merge['Check EB = Sistem'].replace(to_replace="False", value="Beda")

# ds_merge.info()
ds_merge.sample(2)

print("Exporting..")
file_exported = input("Nama file: ")
if (file_exported == ""):
    file_exported = "Rekap.xlsx"
else:
    file_exported =+ ".xlsx"
ds_merge.to_excel(excel_writer=f"./{file_exported}", sheet_name="MAIN")
print(f"Exported! File {file_exported}")

