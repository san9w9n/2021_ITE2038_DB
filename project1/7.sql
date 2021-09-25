SELECT CCP.nickname
FROM Trainer AS TT, CatchedPokemon AS CCP, (
  SELECT T.hometown AS 'Home', MAX(CP.level) AS 'MAXLevel'
  FROM Trainer AS T, CatchedPokemon AS CP
  WHERE T.id = CP.owner_id
  GROUP BY T.hometown
  ) AS NEW
WHERE TT.hometown = NEW.HOME AND CCP.level = NEW.MAXLevel AND TT.id = CCP.owner_id
ORDER BY CCP.nickname
;