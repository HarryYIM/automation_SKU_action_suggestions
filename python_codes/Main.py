import pandas as pd
from datetime import datetime, timedelta
from DBconfig import DB

# set descriptions and actions
sales_description = pd.DataFrame({'LP_sales':['Bad','Bad','Good','Good'],'CP_sales':['Bad','Good','Bad','Good'],
                                  'Sales_Description':['持续低迷','销售增长','销售下降','持续热销']})
margin_description = pd.DataFrame({'LP_margin':['Bad','Bad','Good','Good'],'CP_margin':['Bad','Good','Bad','Good'],
                                  'Margin_Description':['持续亏损','利润转正','利润转负','持续盈利']})
suggested_actions = pd.DataFrame({'Trend':['持续低迷持续亏损','持续低迷利润转正','持续低迷利润转负','持续低迷持续盈利',
                                           '销售增长持续亏损','销售增长利润转正','销售增长利润转负','销售增长持续盈利',
                                           '销售下降持续亏损','销售下降利润转正','销售下降利润转负','销售下降持续盈利',
                                           '持续热销持续亏损','持续热销利润转正','持续热销利润转负','持续热销持续盈利'],
                                  'Action':['建议下架','加大促销','考虑清仓','对标市场','对外涨价','关注库存','分析销售',
                                            '关注补货','释放库存','利润分析','关注销售','释放折扣','建议涨价','重点关注',
                                            '收缩折扣','持续补货']})

# fetch data from db and calculate the avg of sales as sales criteria
def get_df_results(p, start, end):
    # tables names
    tb_sales = '[销售总表8 (DDP成本)]'

    # fetch data from Database
    data = db.get_df(f"select [Country],[Item_Number],SUM([Quantity]) as 'Qty', AVG([Wholesale_Price]) as 'Price', sum([Quantity])*AVG([Wholesale_Price]) as 'Sales',sum([出仓成本]) as 'COGS'  from {tb_sales} where [PO_Date] between '{start}' and '{end}' group by [Country], [Item_Number]")
    data['Margin'] = data['Sales']*0.91 - data['COGS']

    # calculate criteria
    dfs = list()
    for i, df in data.groupby('Country'):
        temp = df.copy()
        temp['Sales Criteria'] = df['Sales'].mean()
        temp[f'{p}_sales'] = df.apply(lambda x: 'Good' if x['Sales'] >= df['Sales'].mean() else 'Bad', axis=1)
        temp[f'{p}_margin'] = df.apply(lambda x: 'Good' if x['Margin'] >= 0 else 'Bad', axis =1)
        dfs.append(temp)
    result = pd.concat(dfs)
    return result, result[['Country', 'Item_Number', f'{p}_sales', f'{p}_margin']]

# use the result as above, attach labels and suggestions on each sku in regards to their desciptions
def getResult(nbDays = 30):
    # dates
    today = datetime.today()
    today_formated = datetime.strftime(today,'%Y-%m-%d')

    C_end = today - timedelta(days = 1)
    C_start = today - timedelta(days = nbDays)
    L_end = C_start - timedelta(days = 1)
    L_start = L_end - timedelta(days = nbDays-1)
    C_end = datetime.strftime(C_end,'%Y-%m-%d')
    C_start = datetime.strftime(C_start,'%Y-%m-%d')
    L_end = datetime.strftime(L_end,'%Y-%m-%d')
    L_start = datetime.strftime(L_start,'%Y-%m-%d')

    print(f"Current Period ({nbDays} days): ",C_start, 'to', C_end)
    print(f"Last Period ({nbDays} days): ",L_start, 'to', L_end)

    # attach labels
    LP = get_df_results('LP', L_start, L_end)[0]
    CP = get_df_results('CP', C_start, C_end)[0]

    df = pd.merge(CP,LP[['Country','Item_Number','LP_sales','LP_margin']], on=['Country','Item_Number'], how='inner')
    df = df.merge(sales_description, on = ['LP_sales','CP_sales'], how ='left')
    df = df.merge(margin_description, on=['LP_margin','CP_margin'], how = 'left')

    df['Trend'] = df['Sales_Description'] + df['Margin_Description']
    df = df.merge(suggested_actions, on = 'Trend', how='left')

    df.drop(columns={'Trend'}).round(1).to_csv(f'..//results//4D_SKU_{nbDays}D_{today_formated}.csv', index=False, encoding='utf-8-sig')
    return

if __name__ == '__main__':
    db = DB()
    db.disconnect()
    getResult(nbDays = 30)
    print('Successfully run.')
