SELECT CP.nickname
FROM Trainer AS T, Gym AS G, CatchedPokemon AS CP, Pokemon AS P
WHERE T.id = G.leader_id AND T.id = CP.owner_id AND G.city = 'Sangnok City'
	AND CP.pid = P.id AND P.type = 'Water'
ORDER BY CP.nickname
;