SELECT TT.name, CCP.nickname
FROM Trainer AS TT, CatchedPokemon AS CCP, (
  SELECT T.id AS 'ID', MAX(CP.level) AS 'MAXLevel'
  FROM Trainer AS T, CatchedPokemon AS CP
  WHERE T.id = CP.owner_id
  GROUP BY T.id
  HAVING COUNT(*) >= 4
  ) AS NEW
WHERE TT.id = NEW.ID AND CCP.owner_id = TT.id AND CCP.LEVEL = NEW.MAXLevel
ORDER BY CCP.nickname
;