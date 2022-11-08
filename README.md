```bash
go install github.com/xooooooox/hey-helper-postgresql@latest
hey-helper-postgresql -o ~/tables.go -p model -s 'public' -d 'postgres://username:password@127.0.0.1:5432/database_name?sslmode=disable'
hey-helper-postgresql -o ~/tables.go -p model -d 'postgres://username:password@127.0.0.1:5432/database_name?sslmode=disable'
```
