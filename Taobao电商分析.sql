-- 创建数据库taobao并选择使用
CREATE DATABASE IF NOT EXISTS taobao;
USE taobao;

-- 统计userbehavior表中不同itemID的数量和总行数
SELECT COUNT(DISTINCT itemID) AS 不同商品数, COUNT(*) AS 总行数 FROM userbehavior;

-- 查看userbehavior表中的数据
SELECT * FROM userbehavior;

-- 创建名为temp的临时表，限制行数为500000，并添加time、date和hour列，用于存储时间信息
CREATE TABLE temp AS
SELECT *,
       timestamp(FROM_UNIXTIME(Timestamp)) AS time,
       date(FROM_UNIXTIME(Timestamp)) AS date,
       hour(FROM_UNIXTIME(Timestamp)) AS hour
FROM userbehavior
-- WHERE date(FROM_UNIXTIME(Timestamp)) BETWEEN '2017-11-25' AND '2017-12-03'
LIMIT 500000;

-- 查看temp表中的数据
SELECT * FROM temp;

-- 查看temp表的结构
DESC temp;

-- 删除temp表中日期不在'2017-11-25'和'2017-12-03'范围内的数据
DELETE FROM temp
WHERE date NOT BETWEEN '2017-11-25' AND '2017-12-03';

-- 查看temp表中的数据
SELECT * FROM temp;

-- 修改temp表中的列名和类型
ALTER TABLE temp DROP Timestamp;
ALTER TABLE temp CHANGE behaviour type TEXT;

-- 统计UV（不同用户数）、商品数和类目数
SELECT
    COUNT(DISTINCT userID) AS UV,
    COUNT(DISTINCT itemID) AS 商品数,
    COUNT(DISTINCT categoryID) AS 类目数
FROM temp;

-- 统计各类行为在总行为中的占比
SELECT
    SUM(IF(type = 'buy', 1, 0)) / COUNT(*) AS buy,
    SUM(IF(type = 'fav', 1, 0)) / COUNT(*) AS fav,
    SUM(IF(type = 'cart', 1, 0)) / COUNT(*) AS cart,
    SUM(IF(type = 'pv', 1, 0)) / COUNT(*) AS pv
FROM temp;

-- 统计每日UV、PV以及PV/UV比例
SELECT
    date,
    COUNT(DISTINCT userID) AS uv,
    COUNT(IF(type = 'pv', userID, NULL)) AS pv,
    COUNT(IF(type = 'pv', userID, NULL)) / COUNT(DISTINCT userID) AS 'pv/uv'
FROM temp
GROUP BY date;

-- 查找每个用户每天的行为日期
SELECT
    userID,
    date
FROM temp
GROUP BY userID, date;

-- 查找每个用户的购买日期及之后日期
SELECT
    a.userID,
    a.date,
    b.date AS dates_after
FROM
    (SELECT userID, date FROM temp GROUP BY userID, date) AS a
    LEFT JOIN
    (SELECT userID, date FROM temp GROUP BY userID, date) AS b
    ON a.userID = b.userID
WHERE b.date >= a.date;

-- 统计每天的用户留存情况，计算留存1到留存8的用户数
DROP VIEW IF EXISTS user_remain_view;
CREATE VIEW user_remain_view AS
SELECT
    a.date,
    COUNT(DISTINCT a.userID) AS user_count,
    COUNT(DISTINCT IF(DATEDIFF(b.date, a.date) = 1, b.userID, NULL)) AS remain_1,
    COUNT(DISTINCT IF(DATEDIFF(b.date, a.date) = 2, b.userID, NULL)) AS remain_2,
    COUNT(DISTINCT IF(DATEDIFF(b.date, a.date) = 3, b.userID, NULL)) AS remain_3,
    COUNT(DISTINCT IF(DATEDIFF(b.date, a.date) = 4, b.userID, NULL)) AS remain_4,
    COUNT(DISTINCT IF(DATEDIFF(b.date, a.date) = 5, b.userID, NULL)) AS remain_5,
    COUNT(DISTINCT IF(DATEDIFF(b.date, a.date) = 6, b.userID, NULL)) AS remain_6,
    COUNT(DISTINCT IF(DATEDIFF(b.date, a.date) = 7, b.userID, NULL)) AS remain_7,
    COUNT(DISTINCT IF(DATEDIFF(b.date, a.date) = 8, b.userID, NULL)) AS remain_8
FROM
    (SELECT userID, date FROM temp GROUP BY userID, date) AS a
    LEFT JOIN 
    (SELECT userID, date FROM temp GROUP BY userID, date) AS b
    ON a.userID = b.userID
WHERE b.date >= a.date
GROUP BY a.date;

-- 查看用户留存情况
SELECT * FROM user_remain_view;

-- 计算用户留存率
SELECT
    date,
    user_count,
    CONCAT(ROUND(remain_1 / user_count * 100, 2), '%') AS day_1,
    CONCAT(ROUND(remain_2 / user_count * 100, 2), '%') AS day_2,
    CONCAT(ROUND(remain_3 / user_count * 100, 2), '%') AS day_3,
    CONCAT(ROUND(remain_4 / user_count * 100, 2), '%') AS day_4,
    CONCAT(ROUND(remain_5 / user_count * 100, 2), '%') AS day_5,
    CONCAT(ROUND(remain_6 / user_count * 100, 2), '%') AS day_6,
    CONCAT(ROUND(remain_7 / user_count * 100, 2), '%') AS day_7,
    CONCAT(ROUND(remain_8 / user_count * 100, 2), '%') AS day_8
FROM user_remain_view;

-- R指标分析：根据每个用户最近一次购买时间，给出相应的分数

-- 建立R视图：统计每个用户的最近购买时间
DROP VIEW IF EXISTS user_recency_view;
CREATE VIEW user_recency_view AS
SELECT
    userID, 
    MAX(date) AS recent_buy_time
FROM  temp
WHERE type = 'buy'
GROUP BY userID;

-- 查看user_recency_view视图中的数据
SELECT * FROM user_recency_view;

-- 建立R评分视图：计算每个用户最近购买时间距离参照日期 '2019-12-18' 的天数，
-- 根据距离天数进行打分：<=2 5分；<=4 4分；<=6 3分；<=8 2分；其他 1分
DROP VIEW IF EXISTS r_score_view;
CREATE VIEW r_score_view AS
SELECT 
    userID,
    recent_buy_time,
    DATEDIFF('2017-12-04', recent_buy_time) AS date_distance,
    CASE
        WHEN DATEDIFF('2017-12-04', recent_buy_time) <= 2 THEN 5
        WHEN DATEDIFF('2017-12-04', recent_buy_time) <= 4 THEN 4
        WHEN DATEDIFF('2017-12-04', recent_buy_time) <= 6 THEN 3
        WHEN DATEDIFF('2017-12-04', recent_buy_time) <= 8 THEN 2
        ELSE 1 
    END AS r_score
FROM user_recency_view;

-- 查看r_score_view视图中的数据
SELECT * FROM r_score_view;

-- 统计R评分的分布情况
SELECT
    SUM(IF(r_score = 1, 1, 0)) / COUNT(*) AS r_score_1,
    SUM(IF(r_score = 3, 1, 0)) / COUNT(*) AS r_score_3,
    SUM(IF(r_score = 5, 1, 0)) / COUNT(*) AS r_score_5
FROM r_score_view;

-- F指标计算：统计每个用户的消费次数，给出相应的分数

-- 建立F视图：统计每个用户的消费次数
DROP VIEW IF EXISTS user_frequency_view;
CREATE VIEW user_frequency_view AS
SELECT
    userID,
    COUNT(userID) AS buy_frequency
FROM temp
WHERE type = 'buy'
GROUP BY userID;

-- 查看user_frequency_view视图中的数据
SELECT * FROM user_frequency_view;

-- 建立F评分视图：基于购买次数对用户进行打分
-- 按照购买次数评分：<=2 1分；<=4 3分；其他 5分
DROP VIEW IF EXISTS f_score_view;
CREATE VIEW f_score_view AS
SELECT
    userID,
    buy_frequency,
    CASE
        WHEN buy_frequency <= 2 THEN 1
        WHEN buy_frequency <= 4 THEN 3
        ELSE 5
    END AS f_score
FROM user_frequency_view;

-- 查看f_score_view视图中的数据
SELECT * FROM f_score_view;

-- 统计F评分的分布情况
SELECT
    SUM(IF(f_score = 1, 1, 0)) / COUNT(*) AS f_score_1,
    SUM(IF(f_score = 3, 1, 0)) / COUNT(*) AS f_score_3,
    SUM(IF(f_score = 5, 1, 0)) / COUNT(*) AS f_score_5
FROM f_score_view;

-- 对商品的指标进行分析：查找购买次数较多且购买转化率高的商品
SELECT
    itemID,
    SUM(IF(type = 'pv', 1, 0)) AS pv,
    SUM(IF(type = 'fav', 1, 0)) AS favorite,
    SUM(IF(type = 'cart', 1, 0)) AS cart,
    SUM(IF(type = 'buy', 1, 0)) AS buy,
    CONCAT(ROUND(COUNT(DISTINCT IF(type = 'buy', userID, NULL)) / COUNT(DISTINCT userID) * 100, 2), '%') AS buy_ratio
FROM temp
GROUP BY itemID
HAVING COUNT(userID) > 20
ORDER BY buy DESC, buy_ratio DESC;

-- 对类目的指标进行分析：查找购买次数较多且购买转化率高的类目
SELECT
    categoryID,
    SUM(IF(type = 'pv', 1, 0)) AS pv,
    SUM(IF(type = 'fav', 1, 0)) AS favorite,
    SUM(IF(type = 'cart', 1, 0)) AS cart,
    SUM(IF(type = 'buy', 1, 0)) AS buy,
    CONCAT(ROUND(COUNT(DISTINCT IF(type = 'buy', userID, NULL)) / COUNT(DISTINCT userID) * 100, 2), '%') AS buy_ratio
FROM temp
GROUP BY categoryID
HAVING COUNT(userID) > 1000
ORDER BY CAST(SUBSTRING(buy_ratio, 1, LENGTH(buy_ratio) - 1) AS DECIMAL(5, 2)) DESC, buy DESC;

-- 查找类目下商品数量超过50的类目
SELECT categoryID, COUNT(DISTINCT itemID) AS sum_items
FROM temp
GROUP BY categoryID
HAVING sum_items > 50;

-- 对类目下商品指标进行分析，并按购买转化率进行排序
SELECT
    categoryID, itemID, pv, favorite, cart, buy, buy_ratio,
    ROW_NUMBER() OVER (PARTITION BY categoryID ORDER BY CAST(SUBSTRING(buy_ratio, 1, LENGTH(buy_ratio) - 1) AS DECIMAL(5, 2)) DESC) AS item_rank
FROM (
    SELECT
        categoryID,
        itemID,
        SUM(IF(type = 'pv', 1, 0)) AS pv,
        SUM(IF(type = 'fav', 1, 0)) AS favorite,
        SUM(IF(type = 'cart', 1, 0)) AS cart,
        SUM(IF(type = 'buy', 1, 0)) AS buy,
        CONCAT(ROUND(COUNT(DISTINCT IF(type = 'buy', userID, NULL)) / COUNT(DISTINCT userID) * 100, 2), '%') AS buy_ratio
    FROM temp
    GROUP BY categoryID, itemID
    HAVING COUNT(userID) > 20
) AS temp_with_rank
ORDER BY categoryID, item_rank, CAST(SUBSTRING(buy_ratio, 1, LENGTH(buy_ratio) - 1) AS DECIMAL(5, 2)) DESC, buy DESC;

-- 用户行为路径分析
DROP VIEW IF EXISTS path_base_view;
CREATE VIEW path_base_view AS
SELECT a.* FROM 
  (SELECT
    userid,
    itemid,
    LAG(type, 4) OVER (PARTITION BY userId, itemId ORDER BY date) AS lag_4,
    LAG(type, 3) OVER (PARTITION BY userId, itemId ORDER BY date) AS lag_3,
    LAG(type, 2) OVER (PARTITION BY userId, itemId ORDER BY date) AS lag_2,
    LAG(type, 1) OVER (PARTITION BY userId, itemId ORDER BY date) AS lag_1,
    type,
    RANK() OVER (PARTITION BY userId, itemId ORDER BY date DESC) AS rank_number
  FROM temp) AS a
WHERE a.rank_number = 1 AND a.type = 'buy';

SELECT * FROM path_base_view;

-- 拼接用户行为路径，并统计各行为路径下的用户数量 
-- 注意：path中可能存在 NULL，因此我们用字符串 'emp' 来对其进行标记
SELECT 
  CONCAT(IFNULL(lag_4, 'emp'), '-', 
         IFNULL(lag_3, 'emp'), '-', 
         IFNULL(lag_2, 'emp'), '-', 
         IFNULL(lag_1, 'emp'), '-', 
         type) AS behavior_path,
  COUNT(DISTINCT userId) AS user_count
FROM path_base_view
GROUP BY
  CONCAT(IFNULL(lag_4, '空'), '-', 
         IFNULL(lag_3, '空'), '-', 
         IFNULL(lag_2, '空'), '-',
         IFNULL(lag_1, '空'), '-', 
         type)
ORDER BY user_count DESC;