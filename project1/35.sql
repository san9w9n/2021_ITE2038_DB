SELECT T.name, COUNT(*)
FROM Trainer AS T, CatchedPokemon AS CP
WHERE T.id = CP.owner_id
GROUP BY T.id
ORDER BY T.name
;