# Experiment 08 – MongoDB CRUD Operations

## Aim

To perform document-oriented storage operations in MongoDB: insert, query, update, and delete.

## 1. Prerequisites

Required software:
- MongoDB Community Server
- MongoDB Shell (mongosh)
- Ubuntu / WSL terminal

Check the installations:

```bash
mongod --version
mongosh --version
```

Start the MongoDB service:

```bash
sudo systemctl start mongod
```

Check the service status:

```bash
sudo systemctl status mongod
```

Enable MongoDB to start automatically:

```bash
sudo systemctl enable mongod
```

## 2. Connect to MongoDB

Start the MongoDB Shell:

```bash
mongosh
```

Check the MongoDB server connection:

```javascript
db.runCommand({ ping: 1 })
```

## 3. Create a Database

Create and select the campus store database:

```javascript
use campus_store
```

Create a collection named `products`:

```javascript
db.createCollection("products")
```

Verify the collection:

```javascript
show collections
```

## 4. Insert Documents

Insert three product documents into the collection:

```javascript
db.products.insertMany([
    {
        name: "Data Science Book",
        category: "Books",
        author: "Sharma",
        price: 650,
        stock: 20
    },
    {
        name: "Wireless Mouse",
        category: "Electronics",
        brand: "Logitech",
        price: 900,
        stock: 15
    },
    {
        name: "Notebook",
        category: "Stationery",
        color: "Blue",
        price: 80,
        stock: 100
    }
])
```

Verify the inserted documents:

```javascript
db.products.find()
```

Display documents in formatted form:

```javascript
db.products.find().pretty()
```

## 5. Read Documents

### 5.1 Display All Products

```javascript
db.products.find()
```

### 5.2 Find Products with Price Below 700

```javascript
db.products.find({
    price: { $lt: 700 }
})
```

### 5.3 Find Products in the Books Category

```javascript
db.products.find({
    category: "Books"
})
```

### 5.4 Display Only Product Name and Price

```javascript
db.products.find(
    {},
    {
        _id: 0,
        name: 1,
        price: 1
    }
)
```

## 6. Update Documents

Increase the price of all books by 5%:

```javascript
db.products.updateMany(
    { category: "Books" },
    [
        {
            $set: {
                price: { $multiply: ["$price", 1.05] }
            }
        }
    ]
)
```

Verify the updated documents:

```javascript
db.products.find({
    category: "Books"
})
```

## 7. Delete Documents

Delete products whose stock is zero:

```javascript
db.products.deleteMany({
    stock: 0
})
```

Verify the remaining documents:

```javascript
db.products.find()
```

## 8. Practice Exercises

### Exercise 1 – Add Two Products with Different Attributes

Insert two products belonging to different categories:

```javascript
db.products.insertMany([
    {
        name: "Machine Learning Book",
        category: "Books",
        author: "Andrew",
        price: 750,
        stock: 12
    },
    {
        name: "Wireless Keyboard",
        category: "Electronics",
        brand: "Logitech",
        price: 1500,
        stock: 25
    }
])
```

Verify:

```javascript
db.products.find()
```

### Exercise 2 – Find Products with Stock Below 20

```javascript
db.products.find({
    stock: { $lt: 20 }
})
```

### Exercise 3 – Increase the Price of All Books by 5%

```javascript
db.products.updateMany(
    { category: "Books" },
    [
        {
            $set: {
                price: { $multiply: ["$price", 1.05] }
            }
        }
    ]
)
```

Verify:

```javascript
db.products.find({
    category: "Books"
})
```

### Exercise 4 – Delete Products with Zero Stock

First, insert a product with zero stock:

```javascript
db.products.insertOne({
    name: "Old Notebook",
    category: "Stationery",
    color: "Red",
    price: 50,
    stock: 0
})
```

Delete products with zero stock:

```javascript
db.products.deleteMany({
    stock: 0
})
```

Verify:

```javascript
db.products.find({
    stock: 0
})
```

### Exercise 5 – Demonstrate Flexible Schema

Display all documents:

```javascript
db.products.find().pretty()
```

The documents have common fields such as `name`, `category`, `price`, and `stock`, but category-specific fields differ:

- Books: `author`
- Electronics: `brand`
- Stationery: `color`

## 9. Additional CRUD Operations

### Insert One Document

```javascript
db.products.insertOne({
    name: "Pen",
    category: "Stationery",
    color: "Black",
    price: 10,
    stock: 50
})
```

### Find One Document

```javascript
db.products.findOne({
    name: "Pen"
})
```

### Update One Document

```javascript
db.products.updateOne(
    { name: "Pen" },
    { $set: { price: 12 } }
)
```

### Delete One Document

```javascript
db.products.deleteOne({
    name: "Pen"
})
```

## 10. Verify Database Operations

Display all databases:

```javascript
show dbs
```

Select the database:

```javascript
use campus_store
```

Display collections:

```javascript
show collections
```

Count the documents:

```javascript
db.products.countDocuments()
```

Display all products:

```javascript
db.products.find().pretty()
```

## 11. Exit MongoDB

```javascript
exit
```

Stop the MongoDB service if required:

```bash
sudo systemctl stop mongod
```

---

## Result

CRUD operations were performed successfully in MongoDB by inserting, querying, updating, and deleting product documents. Flexible document schemas were demonstrated using category-specific product attributes.

---

**End of Experiment 08**