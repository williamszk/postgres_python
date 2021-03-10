
# return query
def query_pandas(query, nrows=None):
    rows = cursorSql.execute(query)
    c = 0 #counter
    
    # print(cursorSql.description) # print column description
    columns = [x[0] for x in cursorSql.description]
    # print(columns) # print column names
    dict_names = {}

    for x in columns:
        dict_names[x] = []
    df_temp = pd.DataFrame(dict_names)

    for i,row in enumerate(rows):
        df_temp = df_temp.append(dict(zip(df_temp.columns,list(row))), ignore_index=True)
        # print(list(row))

        c += 1
        if c%100==0:
            print(f"\rLinha processada: {str(c)} | ", end="")
        if c == nrows:
            break
    
    return df_temp





def sql2csv(query,path_salvar):
    """
    Objetivo: exportar uma tabela do sqlserver para csv
    """
    import pyodbc
    import csv
    from datetime import datetime
    import time
    import os

    # checar se a tabela já existe
    # se existir vamos deletar antes de atualizar
    if os.path.exists(path_salvar):
        print(f"O arquivo {path_salvar} já existe. Vamos deletar antes de prosseguir.")
        os.remove(path_salvar)

    time_1 = time.time()

    rows = cursorSql.execute(query)
    c = 0 #counter

    # print(cursorSql.description) # print column description
    columns = [x[0] for x in cursorSql.description]
    # print(columns) # print column names

    with open(path_salvar,'w',newline='') as csvfile:
        
        writer = csv.writer(csvfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        writer.writerow(columns) #write into csv file
        
        for i,row in enumerate(rows):
            
            writer.writerow(row) #write into csv file
            
            time_2 = time.time()
            c += 1
            if c%10==0:
                print(f"\rLinha processada: {str(c)} | Time elapsed {time_2 - time_1:10.3f} s", end="")


    print()
    print("Processo finalizado as ",datetime.now())






def redshift2pandas(query,  lim_n = 30):
    """
    'query' é um string, uma query de select do redshift
    o output é um pandas data frame
    """
    dictionary_dados = {}
    
    with con.cursor() as cur:
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        dictionary_dados = {desc[0]:[] for desc in cur.description}
        
        n_colunas = len(colnames)

        for i in range(cur.rowcount):
            output = cur.fetchone()
            for nome,valor in zip(colnames,output):
                dictionary_dados[nome].append(valor) 
            
            if i >= lim_n:
                break

            
    tabela_df = pd.DataFrame(dictionary_dados)
            
    
    return tabela_df        