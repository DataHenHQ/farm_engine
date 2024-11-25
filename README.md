**Disclaimer**: This is a WIP README file, we will add usage examples and dev docs later.

# Farm Engine database
Farm engine is a storage focused database made in `Rust` that aims to use close to no RAM and to optimize through disk usage for efficiency while providing a custom index engine interface so anyone could create their own index engines for it at any given time.

This database is currently in an early alpha state but it has a few cool features ready to use and heavily tested in a production environment:
- Custom CSV read only index with yes/no/skip flags that allows to use a CSV file as a database.
- Table implementation.

There are a few experimental features within some branches:
- Binary tree index WIP implementation located at `msh-2` branch.
- CSV file as input with custom type transformation and de-serialization located at `FARM-73` branch.

## History
We created Farm Engine as part of a bigger project intended to use no RAM and first deployed as part of a smaller POC project to test it’s viability in both efficiency and flexibility in really limited resource environments with a huge success.
