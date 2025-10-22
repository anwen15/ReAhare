from Ashare import *
from MyTT import *
def getForYaoinit(data):
     CLOSE = data.close.values
     MA5 = MA(CLOSE, 5)  # 获取5日均线序列
     MA10 = MA(CLOSE, 10)
     if not CROSS(MA5, MA10) and CROSS(CLOSE,MA5):
         return False
     #判断前30天有无涨停
     for declose,close in zip(CLOSE[1:],CLOSE):
         if close >= declose * 1.1:
             return True
     return False
def getForYaotwo(data):
    last_3_days_red = False
    # 获取收盘价和开盘价
    CLOSE = data.close.values
    OPEN = data.open.values

    # 确保有足够的数据
    if len(CLOSE) >= 4:  # 至少需要4天数据才能比较最后3天
        # 检查最后3天是否都是红盘（收盘价 > 开盘价）
        for i in range(-3, -2):  # 检查倒数第3天到倒数第1天
            if CLOSE[-1]<=OPEN[-4]:
                last_3_days_red = False
            if CLOSE[i+1] >= OPEN[i+1]:
                last_3_days_red = True
            else:
                last_3_days_red = False
                break;
            # 如果最后3天都是红盘，则保留该股票数据
    return last_3_days_red
def getForYaobonus(data):
    # 确保有足够数据
    if len(data) >= 5:
        # 取最近5天数据
        recent = data.iloc[-5:]
        HIGH = recent.high.values
        LOW = recent.low.values
        OPEN = recent.open.values
        CLOSE = recent.close.values

        # 检查每一天是否为十字星
        for i in range(len(CLOSE)):
            high, low, open_price, close_price = HIGH[i], LOW[i], OPEN[i], CLOSE[i]

            if low > 0 and (high - low) > 0:
                # 十字星判断：实体很小（≤20%波动范围）
                body_ratio = abs(close_price - open_price) / (high - low)
                if body_ratio <= 0.2:
                    if(i==len(CLOSE)): return 2 # 找到十字星
                    else: return 1
    return 0

def Yao():
    df = get_stock_data()
    df_data={}
    for code, data in df.items():
        if not getForYaoinit(data) and getForYaotwo(data):
            continue
        else:
            score=getForYaobonus(data)
            df_data[code]=score
    return df_data
if __name__ == '__main__':
    print(Yao())




