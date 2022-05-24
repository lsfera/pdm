```
docker network create demo && \
docker-compose \
  -f mongodb.repl.docker-compose.yml \
  up 
```
Once sidecar finishes setting up the replicaset you can either
* Debug dotnet projects (tested with vs2022)
* Launch the compose related project via command line

```
docker-compose \
  -f docker-compose.yml \
  up --build
```

And - finally - the clean up command:

```
docker-compose \
  -f mongodb.repl.docker-compose.yml \
  -f docker-compose.yml \
  down --remove-orphans \
  --rmi local && \
  docker volume prune --force && \
  docker network prune --force

```