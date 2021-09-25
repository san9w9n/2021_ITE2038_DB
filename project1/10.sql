SELECT P.name
FROM Pokemon AS P
WHERE P.id NOT IN (
  SELECT CP.pid
  FROM CatchedPokemon AS CP
  WHERE CP.pid
  )
ORDER BY P.name
;