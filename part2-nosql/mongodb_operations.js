// mongodb_operations.js
// Run with: mongosh --file mongodb_operations.js
// Assumes a running local MongoDB instance and a DB named 'fleximart_nosql'

const dbname = 'fleximart_nosql';
const coll = 'products';

// Switch to DB
db = db.getSiblingDB(dbname);

// Operation 1: Load Data (assuming you placed JSON array in products_catalog.json)
print('Operation 1: load data - use mongoimport or insertMany below if running interactively');
// If using mongoimport from shell: 
// mongoimport --db fleximart_nosql --collection products --file products_catalog.json --jsonArray

// Alternatively, if file content is pasted here, do insertMany
// const data = [ ... ]; db.products.insertMany(data);

// Operation 2: Basic Query
// Find all products in "Electronics" category with price less than 50000, return name, price, stock
print('Operation 2: Basic Query');
db[coll].find(
  { category: "Electronics", price: { $lt: 50000 } },
  { name: 1, price: 1, stock: 1, _id: 0 }
).pretty();

// Operation 3: Review Analysis - products with average rating >= 4.0
print('Operation 3: Review Analysis');
db[coll].aggregate([
  { $unwind: "$reviews" },
  {
    $group: {
      _id: "$product_id",
      name: { $first: "$name" },
      avgRating: { $avg: "$reviews.rating" }
    }
  },
  { $match: { avgRating: { $gte: 4.0 } } },
  { $project: { product_id: "$_id", name: 1, avgRating: 1, _id: 0 } }
]);

// Operation 4: Update Operation - add a review to product ELEC001
print('Operation 4: Add review to ELEC001');
db[coll].updateOne(
  { product_id: "ELEC001" },
  { $push: { reviews: { user_id: "U999", username: "NewUser", rating: 4, comment: "Good value", date: new Date() } } }
);

// Operation 5: Complex Aggregation - average price by category, product_count
print('Operation 5: Avg price by category');
db[coll].aggregate([
  { $group: {
      _id: "$category",
      avg_price: { $avg: "$price" },
      product_count: { $sum: 1 }
  }},
  { $project: { category: "$_id", avg_price: 1, product_count: 1, _id: 0 } },
  { $sort: { avg_price: -1 } }
]).pretty();
