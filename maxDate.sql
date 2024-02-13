-- Brings the date of the most recent tweet
SELECT MAX(CREATED_AT)::DATE
FROM PROJECTS.BRONZE.TWEETS;