<h1 align="center">Dynamoutil</h1>

Collection of useful commands for DynamoDB.

## Installation

Using go get:

```sh
$ go get -u github.com/daangn/dynamoutil
```

## Copy a dynamodb table from remote to local

### Write a config file.

.dynamoutil.yaml
```yaml
copy:
  - service: "default"
    ##  Origin tables to copy.
    origin:
      region: "ap-northeast-2"
      table: "remote-aws-table"
    ## Target table to store.
    target:
      region: "ap-northeast-2"
      endpoint: "http://localhost:8000"
      table: "local-aws-table"
      ## Must match keys of target dynamodb.
      # accessKeyID: "123"
      # secretAccessKey: "123"
```

### Run "copy" command.

```sh
$ dynamoutil -c .dynamoutil.yaml copy
Config file:.dynamoutil.yaml

Origin region: ap-northeast-2  table: remote-aws-table  endpoint: 
Target region: ap-northeast-2  table: local-aws-table  endpoint: http://localhost:8000

Are you sure about copying all items from remote-aws-table? [Y/n]
```

## Dump a dynamodb table from remote

### Write a config file.

```yaml
dump:
  - service: "default"
    db:
      region: "ap-northeast-2"
      # endpoint: "http://localhost:8000"
      table: "remote-dynamodb-table-name"
    output: json
    # Default name is dynamodb's table name
    filename: "remote-dynamodb-table-name"
```

### Run "dump" command.

```sh
$ dynamoutil -c .dynamoutil.yaml dump

Config file:.dynamoutil.yaml

service: default  region: ap-northeast-2  table: remote-aws-table  endpoint:   output: json 

Are you sure about dumping all items from rocket-chat-alpha-message? [Y/n] Y

    Writes 1828 items. 380.71 items/s
```

## Author

* Github:
  - [@novemberde](https://github.com/novemberde)
  - [@mingrammer](https://github.com/mingrammer)
  - [@erickim713](https://github.com/erickim713)

## 🤝 Contributing

Contributions, issues and feature requests are welcome!<br />Feel free to check [issues page](/daangn/dynamoutil/issues).

*This repository only allows Pull Request to apply on master branch.*

## License

[Apache License](./LICENSE)