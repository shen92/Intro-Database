drop table if exists Item;
create table Item (ItemID INTEGER PRIMARY KEY, Name TEXT, Currently FLOAT, Buy_Price FLOAT, First_Bid FLOAT, Number_of_Bids INTEGER, Started TEXT, Ends TEXT, SellerID TEXT, Description TEXT,FOREIGN KEY(SellerID) REFERENCES User(UserID));

drop table if exists User;
create table User(Location TEXT, Country TEXT, UserID TEXT PRIMARY KEY, Rating INTEGER);

drop table if exists Bid;
create table Bid(ItemID INTEGER, UserID TEXT, Time TEXT, Amount FLOAT, FOREIGN KEY(ItemID) REFERENCES Item(ItemID), FOREIGN KEY(UserID) REFERENCES User(UserID));

drop table if exists Category;
create table Category(Name TEXT, PRIMARY KEY(Name));
	
drop table if exists BelongTo;
create table BelongTo(ItemID INTEGER, Name TEXT, FOREIGN KEY(ItemID) REFERENCES Item(ItemID), FOREIGN KEY(Name) REFERENCES Category(Name))
