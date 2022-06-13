## Developing Locally

### Starup docker container

```
docker run --rm -p 6379:6379 redis/redis-stack-server:latest
```


### Install python requirements

```
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

### Start the flask app

```
python3 app.py
```

### Navigate to the home page

1) [Webapp](http://localhost:5000)

2) Data will automatically load  if it is not already present

