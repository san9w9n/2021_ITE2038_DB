SELECT T.name
FROM Trainer AS T, CatchedPokemon AS CP
WHERE T.id = CP.owner_id
GROUP BY T.name
HAVING COUNT(*) >= 3
ORDER BY COUNT(*)
;