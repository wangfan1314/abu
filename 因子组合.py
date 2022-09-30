import pandas as pd

obj = pd.read_csv('因子test11组批量测试.csv', encoding='utf-8')
obj.reset_index(inplace=True, drop=True)
# obj.sort_values (by=['alpha'], ascending=1)
df = obj.drop_duplicates(inplace=False)
# .df.drop('Unnamed: 0',axis-1, inplace=True)
df1 = df.drop([1])
df2 = pd.DataFrame(df1, columns=['总收益', '最大回撤', 'alpha', '夏普比率', '因子组合', '新增因子', '因子数'])
df2 = df2.sort_values(by=['夏普比率'], ascending=0)
df2.to_csv('因子test11组批量测试-排序结果.csv', mode='a', header=['总收益', '最大回撤', 'alpha', '夏普比率', '因子组合', '新增因子', '因子数'])
df3 = pd.read_csv('因子test11组批量测试-排序结果.csv', index＿col=0)
df4 = df3.sort_values(by="总收益").drop_duplicates()
# *#df4.drop(df4.tail(1), inplace=True)
df4.reset_index(inplace=True, drop=True)
# .#
df4.drop([len(df) - 1], inplace=True)
df5 = df4.tail(8)
# df5
print(df5.iloc[-6]['因子组合'])
