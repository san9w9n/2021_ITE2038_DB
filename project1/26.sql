SELECT SLEV.name, SLEV.totlev
FROM (
	SELECT T.id AS 'id', T.name AS 'name', SUM(CP.level) 'totlev'
    	FROM Trainer AS T, CatchedPokemon AS CP
    	WHERE T.id = CP.owner_id
    	GROUP BY T.id
  	) AS SLEV
WHERE SLEV.totlev = ( 
  			SELECT MAX(LEV.totlev)
  			FROM (
              SELECT SUM(CP.level) 'totlev'
              FROM Trainer AS T, CatchedPokemon AS CP
              WHERE T.id = CP.owner_id
              GROUP BY T.id
              ) AS LEV
  )
;