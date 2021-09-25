SELECT T.name, PP.name, COUNT(*)
FROM Trainer AS T, CatchedPokemon AS CCP, Pokemon AS PP
WHERE T.id = CCP.owner_id AND CCP.pid = PP.id 
	AND T.id IN (
      SELECT PT.id
      FROM (
        SELECT DISTINCT owner_Id AS 'id' , P.type AS 'type'
        FROM CatchedPokemon AS CP, Pokemon AS P
        WHERE CP.pid = P.id
        ) AS PT
      GROUP BY PT.id
      HAVING COUNT(*)=1
  )
GROUP BY T.id, T.name, PP.id
ORDER BY T.name
;