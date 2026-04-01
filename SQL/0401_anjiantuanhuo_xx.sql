WITH 
-- 第一步：筛选未成年人违法记录（18岁以下，2024-2025年，有18位身份证）
未成年违法 AS (
    SELECT
        ajbh,                                                        -- 案件编号
        xyrxx_sfzh,                                                  -- 身份证号
        SUBSTRING(xyrxx_lrsj FROM 1 FOR 4) AS 年份,
        calc_age_from_idcard(xyrxx_sfzh, xyrxx_lrsj::timestamp) AS 年龄
    FROM "ywdata"."zq_zfba_xyrxx"
    WHERE
        LENGTH(xyrxx_sfzh) = 18                                      -- 必须是18位身份证
        AND SUBSTRING(xyrxx_lrsj FROM 1 FOR 4) IN ('2024', '2025')  -- 限定年份
        AND calc_age_from_idcard(xyrxx_sfzh, xyrxx_lrsj::timestamp) < 18  -- 18岁以下
),

-- 第二步：按案件编号统计人数，标记团伙类型
团伙案件 AS (
    SELECT
        ajbh,
        年份,
        COUNT(*) AS 团伙人数
    FROM 未成年违法
    GROUP BY ajbh, 年份
),

-- 第三步：关联回个人，带上团伙标记
未成年团伙人员 AS (
    SELECT
        a.ajbh,
        a.xyrxx_sfzh,
        a.年份,
        b.团伙人数,
        CASE WHEN b.团伙人数 >= 2 THEN 1 ELSE 0 END AS 属于2人团伙,
        CASE WHEN b.团伙人数 >= 3 THEN 1 ELSE 0 END AS 属于3人团伙
    FROM 未成年违法 a
    JOIN 团伙案件 b ON a.ajbh = b.ajbh AND a.年份 = b.年份
    WHERE b.团伙人数 >= 2   -- 只保留有团伙行为的记录
),

-- 第四步：关联学校信息
带学校信息 AS (
    SELECT
        p.ajbh,
        p.年份,
        p.团伙人数,
        p.属于2人团伙,
        p.属于3人团伙,
        s.xxbsm,
        COALESCE(s.xxmc, '未关联到学校') AS 学校名称
    FROM 未成年团伙人员 p
    LEFT JOIN "ywdata"."sh_gd_zxxxsxj_xx" s ON p.xyrxx_sfzh = s.sfzjh
)

-- 第五步：按学校统计，分2人团伙和3人团伙两个口径
SELECT
    年份,
    学校名称,
    xxbsm AS 学校标识码,

    -- 2人及以上团伙口径
    SUM(属于2人团伙)                                                              AS 涉案人次_2人团伙,
    ROUND(
        SUM(属于2人团伙)::numeric / NULLIF(SUM(SUM(属于2人团伙)) OVER (PARTITION BY 年份), 0) * 100, 2
    )                                                                             AS 占比_2人团伙_pct,

    -- 3人及以上团伙口径
    SUM(属于3人团伙)                                                              AS 涉案人次_3人团伙,
    ROUND(
        SUM(属于3人团伙)::numeric / NULLIF(SUM(SUM(属于3人团伙)) OVER (PARTITION BY 年份), 0) * 100, 2
    )                                                                             AS 占比_3人团伙_pct

FROM 带学校信息
WHERE 学校名称 <> '未关联到学校'   -- 可按需去掉此行，保留未关联记录
GROUP BY 年份, 学校名称, xxbsm
ORDER BY 年份, 涉案人次_2人团伙 DESC;