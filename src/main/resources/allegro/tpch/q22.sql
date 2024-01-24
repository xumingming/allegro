SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal
FROM (
	SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal
	FROM customer
	WHERE substring(c_phone FROM 1 FOR 2) IN (
			'24', 
			'32', 
			'17', 
			'18', 
			'12', 
			'14', 
			'22'
		)
		AND c_acctbal > 5000.097675
		AND NOT EXISTS (
			SELECT *
			FROM orders
			WHERE o_custkey = c_custkey
		)
) custsale
GROUP BY cntrycode
ORDER BY cntrycode