SELECT a.*,b.学校名称，b.年级 FROM (
				SELECT
					name AS 姓名,
					id_number AS 证件号码,
					EXTRACT(
						YEAR
					FROM
						AGE(current_date, TO_DATE(SUBSTRING(id_number FROM 7 FOR 8), 'YYYYMMDD'))
					) AS 周岁,
					游荡次数
				FROM
					(
						SELECT
							id_number,
							name,
							count(*) AS 游荡次数
						FROM
							(
								SELECT
								DISTINCT ON
					(id_number,substring(shot_time, 1, 8))
									id_number,
									name,
									substring(shot_time, 1, 8) AS 日期
								FROM
									
											t_spy_ryrlgj_xx a
WHERE
shot_time >= 20250101000000
AND shot_time <= 20260331000000
AND substring(shot_time, 9, 4)>= 0000
AND substring(shot_time, 9, 4)<= 0500		
							)
GROUP BY
							id_number ,
							name

HAVING
							count(*) >= 10
					)
WHERE
					EXTRACT(
						YEAR
FROM
						AGE(current_date, TO_DATE(SUBSTRING(id_number FROM 7 FOR 8), 'YYYYMMDD'))
)<18
AND EXTRACT(
						YEAR
FROM
						AGE(current_date, TO_DATE(SUBSTRING(id_number FROM 7 FOR 8), 'YYYYMMDD'))
)>6
			) a
INNER JOIN (
	SELECT
			sfzjh,
			xxmc AS 学校名称,
			njmc AS 年级
	FROM
			sh_gd_zxxxsxj_xx
	WHERE
		substring(sszgjyxzdm, 1, 4)= '4453'
			AND jdzt = '在校'
) b ON
	a.证件号码 = b.sfzjh
WHERE
NOT EXISTS(
	SELECT
		1
	FROM
		sh_yf_zzxj_xx k
	WHERE
		b.sfzjh = k."sfzjh"
)