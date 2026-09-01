package sqlite

// PageQuery is the statement a picture of the tree pages with, exposed so that a test can put
// it to EXPLAIN QUERY PLAN.
//
// The plan is what is under test rather than the rows: this query returned the right answer
// both before and after the change that made it usable, and the difference between the two is
// entirely in what SQLite decided to do to produce it.
const PageQuery = pageQuery
