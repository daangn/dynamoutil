# Dynamoutil

Collection of useful commands for DynamoDB.

## Installation

Using go get:

```sh
$ go get -u github.com/daangn/dynamoutil
```

## Copy dynamodb from the remote table to the local table

Write a config file.

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

Run "copy" command.

```sh
$ dynamoutil -c .dynamoutil.yaml copy
```

## License

[Apache License](./LICENSE)