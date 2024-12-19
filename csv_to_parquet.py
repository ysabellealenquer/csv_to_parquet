import pandas as pd  
from os import walk

# Carregar o arquivo CSV  
csv_file = "/S2DTSD01/shared/BancoCSV/ETENE/Publico/OBSERVATORIO/E.D.A/BACEN/SCR/SCR_UPLOAD/2023/"

#Delimitador do pandas = "sep"
df = pd.read_csv(csv_file, sep=';', skipinitialspace=True)
print("Leitura do CSV", df.head())
 
# Converter e salvar como Parquet  
parquet_file = "/S2DTSD01/shared/BancoCSV/ETENE/Publico/OBSERVATORIO/E.D.A/BACEN\SCR/SCR_PARQUET/2023/"
#df2 = pd.read_parquet(parquet_file)
#print("Leitura do Parquet", df2.head())

# convertendo a pasta para parquet
for (dirpath, dirnames, filenames) in walk(f"{csv_file}"):
                for arquivo in filenames:
                    df = pd.read_csv(csv_file + arquivo, sep=';', skipinitialspace=True)
                    arquivo = arquivo[:-3]
                    arquivo = arquivo + 'parquet'
                    df.to_parquet(parquet_file + arquivo, engine='pyarrow', index=False)  


# arquivo convertido
#print(f'Arquivo convertido para {parquet_file}')