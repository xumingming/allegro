SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
	AND p_brand <> 'Brand#45'
	AND p_type NOT LIKE 'SMALL ANODIZED%'
	AND p_size IN (47, 15, 37, 30, 46, 16, 18, 6)
	AND ps_suppkey NOT IN (
		SELECT s_suppkey
		FROM supplier
		WHERE s_comment LIKE '%Customer%Complaints%'
	)
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size