#-*- coding:utf-8 -*-    --------------Ashare 股票行情数据双核心版( https://github.com/mpquant/Ashare ) 
import json,requests,datetime,time;
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    dstr= json.loads(requests.get(URL).content);
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



def get_a_stock_list():
    """
    获取A股股票列表
    """
    # 通过 sina 获取股票列表的一种方法
    stock_list = []

    # 沪市A股 (sh60xxxx, sh68xxxx)
    # 深市A股 (sz00xxxx, sz30xxxx, sz002xxxx)
    prefixes = [
        'sh60',  # 沪市主板
        'sz00',  # 深市主板/中小板
        'sz002'  # 深市中小板
    ]

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
            if len(data) == 0:
                break
            else:i=i+1
            stock_list =stock_list + [item['symbol'] for item in data]
            print(f"获取到 {len(stock_list)} 条 总数据")
        stock_list = [stock for stock in stock_list if stock.startswith(tuple(prefixes))]
        print(f"实际获取到 {len(stock_list)} 条 总数据")
        return stock_list
    except:
        # 备用方案：手动构建部分代码
        stock_codes = []
        # 示例：添加部分股票代码
        for i in range(600000, 600100):  # 沪市部分股票
            stock_codes.append(f"sh{i}")
        for i in range(2000, 2100):  # 深市部分股票
            stock_codes.append(f"sz002{i}")
        return stock_codes


# 获取所有股票代码
# stock_list = get_a_stock_list()
# print(f"获取到 {len(stock_list)} 只股票")

def get_all_stocks_data(stock_list, frequency='1d', count=10, max_workers=10):
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
        # 提交所有任务
        future_to_stock = {
            executor.submit(get_price, stock,end_date,  count,frequency): stock
            for stock in stock_list[:10] # 先测试前100只股票
        }

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
if __name__ == '__main__':

    df=get_a_stock_list()
    print('所有股票代码\n',df)


