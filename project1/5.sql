SELECT AVG(CP.level)
FROM Trainer T, CatchedPokemon CP
WHERE T.name = 'Red' AND T.id = CP.owner_id
;