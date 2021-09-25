SELECT TT.hometown
FROM Trainer AS TT
GROUP BY TT.hometown
HAVING COUNT(*) = (
	SELECT MAX(NEW.cnt)
    FROM (
      SELECT COUNT(*) AS 'cnt'
      FROM Trainer AS T
      GROUP BY T.hometown
      ) AS NEW
    )
;