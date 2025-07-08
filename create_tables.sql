CREATE TABLE clients(
  client_id INTEGER PRIMARY KEY,
  entity_type TEXT,
  entity_year_established INTEGER
);
CREATE TABLE payments(
  transaction_id INTEGER PRIMARY KEY,
  contract_id INTEGER,
  client_id INTEGER,
  transaction_date INTEGER,
  payment_amt INTEGER,
  payment_code TEXT,
  FOREIGN KEY(client_id) REFERENCES clients(client_id)
);
