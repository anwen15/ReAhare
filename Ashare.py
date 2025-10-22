#-*- coding:utf-8 -*-    --------------Ashare 股票行情数据双核心版( https://github.com/mpquant/Ashare )
import datetime
import json
import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd  #


#腾讯日线
def get_price_day_tx(code, end_date='', count=10, frequency='1d'):     #日线获取  
    unit='week' if frequency in '1w' else 'month' if frequency in '1M' else 'day'     #判断日线，周线，月线
    if end_date:  end_date=end_date.strftime('%Y-%m-%d') if isinstance(end_date,datetime.date) else end_date.split(' ')[0]
    end_date='' if end_date==datetime.datetime.now().strftime('%Y-%m-%d') else end_date   #如果日期今天就变成空    
    URL=f'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},{unit},,{end_date},{count},qfq'     
    st= json.loads(requests.get(URL).content);    ms='qfq'+unit;      stk=st['data'][code]   
    buf=stk[ms] if ms in stk else stk[unit]       #指数返回不是qfqday,是day
    df=pd.DataFrame(buf,columns=['time','open','close','high','low','volume'],dtype='float')     
    df.time=pd.to_datetime(df.time);    df.set_index(['time'], inplace=True);   df.index.name=''          #处理索引 
    return df

#腾讯分钟线
def get_price_min_tx(code, end_date=None, count=10, frequency='1d'):    #分钟线获取 
    ts=int(frequency[:-1]) if frequency[:-1].isdigit() else 1           #解析K线周期数
    if end_date: end_date=end_date.strftime('%Y-%m-%d') if isinstance(end_date,datetime.date) else end_date.split(' ')[0]        
    URL=f'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={code},m{ts},,{count}' 
    st= json.loads(requests.get(URL).content);       buf=st['data'][code]['m'+str(ts)] 
    df=pd.DataFrame(buf,columns=['time','open','close','high','low','volume','n1','n2'])   
    df=df[['time','open','close','high','low','volume']]    
    df[['open','close','high','low','volume']]=df[['open','close','high','low','volume']].astype('float')
    df.time=pd.to_datetime(df.time);   df.set_index(['time'], inplace=True);   df.index.name=''          #处理索引     
    df['close'][-1]=float(st['data'][code]['qt'][code][3])                #最新基金数据是3位的
    return df


#sina新浪全周期获取函数，分钟线 5m,15m,30m,60m  日线1d=240m   周线1w=1200m  1月=7200m
def get_price_sina(code, end_date='', count=10, frequency='60m'):    #新浪全周期获取函数    
    frequency=frequency.replace('1d','240m').replace('1w','1200m').replace('1M','7200m');   mcount=count
    ts=int(frequency[:-1]) if frequency[:-1].isdigit() else 1       #解析K线周期数
    if (end_date!='') & (frequency in ['240m','1200m','7200m']): 
        end_date=pd.to_datetime(end_date) if not isinstance(end_date,datetime.date) else end_date    #转换成datetime
        unit=4 if frequency=='1200m' else 29 if frequency=='7200m' else 1    #4,29多几个数据不影响速度
        count=count+(datetime.datetime.now()-end_date).days//unit            #结束时间到今天有多少天自然日(肯定 >交易日)        
        #print(code,end_date,count)    
    URL=f'http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={code}&scale={ts}&ma=5&datalen={count}'
    try:
        content=requests.get(URL).content
        dstr= json.loads(content)
    except Exception as e:
        print(f"错误: {e}")
        # print(f'获取数据失败，请检查网络或参数',URL)
        # print('响应信息:',content)
        return e
    #df=pd.DataFrame(dstr,columns=['day','open','high','low','close','volume'],dtype='float') 
    df= pd.DataFrame(dstr,columns=['day','open','high','low','close','volume'])
    df['open'] = df['open'].astype(float); df['high'] = df['high'].astype(float);                          #转换数据类型
    df['low'] = df['low'].astype(float);   df['close'] = df['close'].astype(float);  df['volume'] = df['volume'].astype(float)    
    df.day=pd.to_datetime(df.day);    df.set_index(['day'], inplace=True);     df.index.name=''            #处理索引                 
    if (end_date!='') & (frequency in ['240m','1200m','7200m']): return df[df.index<=end_date][-mcount:]   #日线带结束时间先返回              
    return df

def get_price(code, end_date='',count=10, frequency='1d', fields=[]):        #对外暴露只有唯一函数，这样对用户才是最友好的  
    xcode= code.replace('.XSHG','').replace('.XSHE','')                      #证券代码编码兼容处理 
    xcode='sh'+xcode if ('XSHG' in code)  else  'sz'+xcode  if ('XSHE' in code)  else code     

    if  frequency in ['1d','1w','1M']:   #1d日线  1w周线  1M月线
         try:    return get_price_sina( xcode, end_date=end_date,count=count,frequency=frequency)   #主力
         except: return get_price_day_tx(xcode,end_date=end_date,count=count,frequency=frequency)   #备用                    
    
    if  frequency in ['1m','5m','15m','30m','60m']:  #分钟线 ,1m只有腾讯接口  5分钟5m   60分钟60m
         if frequency in '1m': return get_price_min_tx(xcode,end_date=end_date,count=count,frequency=frequency)
         try:    return get_price_sina(  xcode,end_date=end_date,count=count,frequency=frequency)   #主力   
         except: return get_price_min_tx(xcode,end_date=end_date,count=count,frequency=frequency)   #备用

def  save_stock_data(df,filename=None, version=1.0, folder="stock_data"):
    if df is None:
        print("数据为空，无法保存")
        return None

    # 创建文件夹（如果不存在）
    Path(folder).mkdir(parents=True, exist_ok=True)

    # 如果没有指定文件名，则根据股票代码或时间生成文件名
    if filename is None:
        filename = f"stock_data_{version}.xlsx"
    # 构建完整路径
    filepath = os.path.join(folder, filename)
    try:
        # 保存为CSV文件
        if isinstance(df, dict):
            # 创建Excel写入器
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for code, data_df in df.items():
                    if data_df is not None and not data_df.empty:
                        # 将每只股票数据保存为一个工作表
                        sheet_name = code[:31]  # Excel工作表名称限制为31个字符
                        data_df.to_excel(writer, sheet_name=code)
                        print(f"{code} 的数据已保存至工作表 {sheet_name}")

            print(f"所有股票数据已保存至 {filepath}")
            return filepath
        print(f"数据已保存至 {filepath}")
        return filepath
    except Exception as e:
        print(f"保存文件时出错: {e}")
        return None


def get_stock_data(filename="stock_data_1.0.xlsx",folder="stock_data", stock_codes=None):
    # 构建完整路径
    filepath = os.path.join(folder, filename)

    # 检查文件是否存在
    if not os.path.exists(filepath):
        print(f"文件 {filepath} 不存在")
        return None
    try:
        # 读取Excel文件
        excel_file = pd.ExcelFile(filepath)

        # 如果没有指定股票代码，则读取所有工作表
        if stock_codes is None:
            stock_codes = excel_file.sheet_names

        # 创建存储数据的字典
        stock_data = {}

        # 读取指定的工作表
        for code in stock_codes:
            if code in excel_file.sheet_names:
                # 读取数据，第一列作为索引
                df = pd.read_excel(filepath, sheet_name=code, index_col=0)
                stock_data[code] = df
                print(f"成功读取 {code} 的数据，共 {len(df)} 行")
            else:
                print(f"工作表 {code} 不存在")

        print(f"总共读取了 {len(stock_data)} 只股票的数据")
        return stock_data

    except Exception as e:
        print(f"读取文件时出错: {e}")
        return None

def get_a_stock_list():
    """
    获取A股股票列表
    """
    # 通过 sina 获取股票列表的一种方法
    stock_list = []
    stock_name= []
    # 沪市A股 (sh60xxxx, sh68xxxx)
    # 深市A股 (sz00xxxx, sz30xxxx, sz002xxxx)
    prefixes = [
        'sh60',  # 沪市主板
        'sz00',  # 深市主板/中小板
        'sz002'  # 深市中小板
    ]
    prename={
        "ST",
        "*ST"
    }
    # 或者通过网络接口获取完整列表
    try:
        i = 1
        while True:
            start_time = time.perf_counter()
            url = f"http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={i}&num=200&sort=symbol&asc=1&node=hs_a"
            response = requests.get(url)
            data = response.json()
            end_time = time.perf_counter()
            if end_time - start_time <1 :
                time.sleep(0.5)
            if len(data) == 0: #获取数据条数 自测只拿500
                break
            else:i=i+1
            stock_list = stock_list+ [item['symbol'] for item in data]
            stock_name = stock_name+ [item['name'] for item in data]
            print(f"获取到 {len(stock_list)} 条 总数据")
        stock_list = [stock for stock,name in zip(stock_list,stock_name) if stock.startswith(tuple(prefixes)) and not name.startswith(tuple(prename))]
        print(f"实际获取到 {len(stock_list)} 条 总数据")
        return stock_list
    except:
        stock_codes = []
        return stock_codes


# 获取所有股票代码
# stock_list = get_a_stock_list()
# print(f"获取到 {len(stock_list)} 只股票")

def get_all_stocks_data(stock_list, frequency='1d', count=30, max_workers=5):
    """
    批量获取所有股票走势数据

    Args:
        stock_list: 股票代码列表
        frequency: K线周期 ('1d','1w','1M','1m','5m','15m','30m','60m')
        count: 获取数据条数
        max_workers: 最大并发线程数

    Returns:
        dict: 股票代码为key，DataFrame为value的字典
    """
    stock_data = {}
    end_date = ''
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_stock = {}

        # 分批处理股票列表
        batch_size = max_workers  # 每批处理max_workers个股票

        for i in range(0, len(stock_list), batch_size):
            # 获取当前批次的股票
            batch = stock_list[i:i + batch_size]

            # 提交当前批次的任务
            for stock in batch:
                future = executor.submit(get_price, stock, end_date, count, frequency)
                future_to_stock[future] = stock

            # 如果这是满批次（不是最后一批），则睡眠
            if len(batch) == batch_size and i + batch_size < len(stock_list):
                time.sleep(5.0)
                print(f"批次 {i // batch_size + 1} 已提交，睡眠0.2秒...")

        # 收集结果
        for future in as_completed(future_to_stock):
            stock_code = future_to_stock[future]
            df=future.result()
            if df is not None and not df.empty:
                stock_data[stock_code] = df
            else:
                print(f"未获取到 {stock_code} 的数据")

            # 添加延时避免请求过于频繁
            time.sleep(0.1)

    return stock_data

def get_ZTPool():
    # 获取当前日期时间
    now = datetime.now()

    # 格式化为 yyyy-mm-dd
    date_str = now.strftime("%Y-%m-%d")
    requesturl="https://www.stockapi.com.cn/v1/base/ZTPool?date="+date_str
    print(requesturl)
    response = requests.get(requesturl)
    data = response.json()
    stock_data=data['data']
    return stock_data

'''
todo
获取的股票data解析为通用股票data
选股引擎（大规模）
每天回归往日涨停池股票，由选股引擎判断权重决定存储单元
存储单元为：每天涨停池股票共五个，今日重点关注股票，往期股票权重增加，妖股池
'''

if __name__ == '__main__':
    get_ZTPool()
    # strcode=get_a_stock_list()
    # df=get_all_stocks_data(strcode)
    # save_stock_data( df)
    # df=get_a_stock_list()
    # print('所有股票代码\n',df)
    # df=get_all_stocks_data(df)
    # save_stock_data( df)


