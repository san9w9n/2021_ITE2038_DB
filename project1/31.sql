SELECT SANG.name
FROM (
  SELECT P.id AS 'id', P.name AS 'name'
  FROM Trainer AS T, CatchedPokemon AS CP, Pokemon AS P
  WHERE T.id = CP.owner_id AND CP.pid = P.id
      AND T.hometown = 'Sangnok City'
  ) AS SANG, (
  SELECT PP.id AS 'id', PP.name AS 'name'
  FROM Trainer AS TT, CatchedPokemon AS CCP, Pokemon AS PP
  WHERE TT.id = CCP.owner_id AND CCP.pid = PP.id
      AND TT.hometown = 'Blue City'
  ) AS BLUE
WHERE SANG.id = BLUE.id   
ORDER BY SANG.name
;