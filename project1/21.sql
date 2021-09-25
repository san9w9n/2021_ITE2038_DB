SELECT T.name
FROM Trainer AS T, CatchedPokemon AS CP, Pokemon AS P
WHERE CP.pid = P.id AND T.id = CP.owner_id
GROUP BY T.id, P.id
HAVING COUNT(*) >= 2
ORDER BY T.name
;