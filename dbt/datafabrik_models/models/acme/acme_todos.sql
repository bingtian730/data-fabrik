SELECT
    id,
    user_id,
    title,
    completed,
    NOW() AS processed_at
FROM (
    VALUES
        (1, 1, 'delectus aut autem',         false),
        (2, 1, 'quis ut nam facilis',         true),
        (3, 1, 'fugiat veniam minus',         false),
        (4, 1, 'et porro tempora',            true),
        (5, 1, 'laboriosam mollitia',         false)
) AS t(id, user_id, title, completed)
