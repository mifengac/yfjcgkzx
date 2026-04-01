-- 统计各学校出现的警情次数
SELECT
    学校名称,
    COUNT(*) AS 警情次数,
    年份
FROM (
    SELECT
        COALESCE(
            (regexp_match(zkdj."occuraddress" , '[^\s，,。！!？?、]{2,10}(?:中学|小学|学校|幼儿园|大学|职中|职高|技校|实验校)'))[1],
            (regexp_match(zkdj.casecontents, '[^\s，,。！!？?、]{2,10}(?:中学|小学|学校|幼儿园|大学|职中|职高|技校|实验校)'))[1],
            (regexp_match(zkdj.replies, '[^\s，,。！!？?、]{2,10}(?:中学|小学|学校|幼儿园|大学|职中|职高|技校|实验校)'))[1]
        ) AS 学校名称,
        SUBSTRING(zkdj.calltime FROM 1 FOR 4) AS 年份
    FROM "ywdata"."zq_kshddpt_dsjfx_jq" zkdj  
    WHERE
       ( zkdj."occuraddress" ~ '中学|小学|学校|幼儿园|大学|职中|职高|技校|实验校'
        OR zkdj.casecontents ~ '中学|小学|学校|幼儿园|大学|职中|职高|技校|实验校'
        OR zkdj.replies ~ '中学|小学|学校|幼儿园|大学|职中|职高|技校|实验校') AND zkdj.calltime BETWEEN '2024-01-01' AND '2025-12-31 23:59:59'
) t
WHERE 学校名称 IS NOT NULL 
GROUP BY 学校名称,年份
ORDER BY 警情次数 DESC;