# NBA JSON-document validator

## Docker

### Building

#### Validator

```SHELL
docker build -t naturalis/nba-json-validator:<tag> .
```

#### GUI

```SHELL
docker build -t naturalis/nba-json-validator-gui:<tag> -f Dockerfile.gui .
```

### Running

#### Validator

#### GUI

```SHELL
docker run --rm -p 80:80 naturalis/nba-json-validator-gui:1
```
