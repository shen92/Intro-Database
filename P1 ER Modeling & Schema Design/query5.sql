SELECT COUNT(Rating)
FROM (SELECT UserID,Rating
	FROM Item, User
	WHERE Item.SellerID = User.UserID
	GROUP BY UserID) AS A
WHERE A.Rating > 1000