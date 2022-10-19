import pandas as pd


def data_transformation():
    df = pd.read_excel("raw_files\BIGC Model.xlsx",sheet_name = 'Data extraction',
                  usecols= "B:AC", header=None)
    
    author_name, ticker_name = df.loc[2,3], df.loc[1,3]

    category = list(df[1].dropna()[:3])
    category_list = [['Revenue','COGS (ex D&A)'], ['Cash & Short-Term Investments'], ['Depreciation & Amortization', 
                        'Change in Net Working Capital']]
    categories = {k:v for k,v in zip(category, category_list)}

    dff = df.loc[df[2].isin(['Revenue', 'COGS (ex D&A)', 'Line Item', 'Cash & Short-Term Investments', 'Depreciation & Amortization'
                        ,'Change in Net Working Capital'])]

    estimate_or_actual = df.iloc[5, 4:]
    e_or_a_table = pd.DataFrame(estimate_or_actual)
    e_or_a_table_transpose = e_or_a_table.T

    working_table = pd.concat([e_or_a_table_transpose, dff], ignore_index=True)
    working_table_clean = working_table.set_index(2).T.dropna()
    working_table_final = working_table_clean.rename(columns=str).rename(columns={'nan':'Estimate/Actual'})

    final_df = pd.DataFrame()
    working_table_final_columns = list(working_table_final.columns)[2:]

    year_list = ['2018', '2019', '2020', '2021', '2022', '2023', '2024']

    for i in working_table_final_columns:
        final_df = pd.concat([final_df, pd.DataFrame({'Author Name': author_name,
                                                    'Ticker Name': ticker_name,
                                                    'Category': [k for k,v in categories.items() if i in v][0],
                                                    'Line Item': i,
                                                    'Year': year_list,
                                                    'Period': working_table_final['Line Item'],
                                                    'Estimate/Actual': working_table_final['Estimate/Actual'],
                                                    'Value': working_table_final[i]})], 
                                                    ignore_index=True)
    
    rev_yr = final_df[final_df['Line Item'] == 'Revenue']

    r = final_df.loc[(final_df['Year'].isin(['2018','2019', '2020']))]
    cash_yr = r[r['Line Item'] == 'Cash & Short-Term Investments']
    cogs_yr = r[r['Line Item'] == 'COGS (ex D&A)']

    o = final_df.loc[(final_df['Year'].isin(['2018','2019', '2020', '2021']))]
    dep_yr = o[o['Line Item'] == 'Depreciation & Amortization']

    q = final_df.loc[(final_df['Year'].isin(['2021', '2022', '2023']))]
    change_yr = q[q['Line Item'] == 'Change in Net Working Capital']

    actual_final_df = pd.concat([rev_yr,cogs_yr,cash_yr,dep_yr,change_yr], ignore_index=True)
    #convert to csv file named out.csv
    actual_final_df.to_csv('out.csv', index=False)

    return author_name

data_transformation()