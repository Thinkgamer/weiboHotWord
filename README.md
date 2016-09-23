# weiboHotWord
基于hadoop和hive的微博热词跟踪系统

#对应的blog地址为:
<a href="http://blog.csdn.net/gamer_gyt/article/details/51940211">http://blog.csdn.net/gamer_gyt/article/details/51940211</a>
<br />
<1>首先是利用微博的api得到每天的微博数据</br>
<2>编写hadoop项目对微博内容进行分词统计，设置一个阀值，当一个词的出现的数目超过这个阀值时就将其加入到热词列表里，在以后的每天就对其进行统计</br>
<3>将处理后的数据写入hive</br>
