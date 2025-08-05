# memberjojo

`memberjojo` is a Python library for managing [Membermojo](http://membermojo.co.uk/)
data from CSV imports.\
It provides member database interactions, and transaction querying.\
This is done in a local SQLite database, and does not alter anything on Membermojo.\
It provides tools to load, and query membership and transaction data efficiently
without having to use SQLite directly.\
When importing CSV files existing entries are skipped, so you can just import the
latest download and the local database is updated with new entries.\
All the transaction data is imported into the database,
but currently only a limited amount of member data is imported.

---

## Installation

Install via `pip`:

```bash
pip install memberjojo
```

Or clone the repo and install locally with `flit`:

```bash
git clone https://github.com/a16bitsysop/memberjojo.git
cd memberjojo
flit install --symlink
```

## Usage

Example loading members and using Member objects:

```python
from pathlib import Path
from membermojo import Member

# database is created if it does not exist
member_database_path = Path(Path(__file__).parent, "database", "my-members.db")
member_csv_path = Path("download", "members.csv")

members = Member(member_database_path)
members.import_csv(member_csv_path)

for member in members:
    print(member.first_name, member.last_name, member.member_num)

# Get full name for a given member number
found_name = members.get_name(1)
if found_name:
    print(f"Member with id of 1 is {found_name}")
else:
    print("Member 1 does not exist")
```

## Documentation

Full documentation is available at  
👉 [https://a16bitsysop.github.io/memberjojo/](https://a16bitsysop.github.io/memberjojo/)

---

## Running Tests

Run tests:

```bash
pytest
```

## Contributing

Contributions are welcome! Please:

1. Fork the repo
2. Create your feature branch `git checkout -b my-feature`
3. Edit the source code to add and test your changes
4. Commit your changes `git commit -m 'Add some feature'`
5. Push to your branch `git push origin my-feature`
6. Open a Pull Request

Please follow the existing code style and write tests for new features.

---

## License

This project is licensed under the MIT [MIT License](https://github.com/a16bitsysop/memberjojo/blob/main/LICENSE).

---

## Contact

Created and maintained by Duncan Bellamy.
Feel free to open issues or reach out on GitHub.

---
