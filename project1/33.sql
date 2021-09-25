SELECT T.name
FROM Trainer AS T
WHERE T.id IN (
  SELECT CP.owner_id
  FROM CatchedPokemon AS CP, Pokemon AS P
  WHERE CP.pid = P.id AND P.type = 'Psychic'
  )
ORDER BY T.name
;