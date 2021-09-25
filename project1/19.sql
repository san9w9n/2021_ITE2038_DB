SELECT SUM(CP.level)
FROM Trainer AS T, CatchedPokemon AS CP
WHERE T.name = 'Matis' AND T.id = CP.owner_id
;